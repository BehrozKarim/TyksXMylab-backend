"""Persistent append-only PostgreSQL event history, ready for a future CDC source."""

import json
import re
from contextlib import contextmanager
from typing import Iterable

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from .models import IsolateEvent
from .store import EventConflict


class PostgresEventStore:
    backend = "postgresql"

    def __init__(self, database_url: str, schema: str = "aei"):
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,62}", schema):
            raise ValueError("Invalid PostgreSQL schema name")
        self.database_url = database_url
        self.schema = schema
        with self.connection() as db:
            db.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"aei-init:{schema}",))
            db.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
            db.execute("""CREATE TABLE IF NOT EXISTS isolate_events (
                sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
                event_id TEXT PRIMARY KEY,
                isolate_id TEXT NOT NULL,
                revision INTEGER NOT NULL CHECK (revision >= 1),
                patient_id TEXT NOT NULL,
                sample_date DATE NOT NULL,
                received_at TIMESTAMPTZ NOT NULL,
                payload JSONB NOT NULL,
                UNIQUE(isolate_id, revision)
            )""")
            db.execute("CREATE INDEX IF NOT EXISTS event_patient_idx ON isolate_events (patient_id)")
            db.execute("""CREATE OR REPLACE FUNCTION reject_event_mutation()
                RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN
                    RAISE EXCEPTION 'Event history is append-only' USING ERRCODE = '23514';
                END;
                $$""")
            db.execute("""CREATE OR REPLACE TRIGGER forbid_event_mutation
                BEFORE UPDATE OR DELETE OR TRUNCATE ON isolate_events
                FOR EACH STATEMENT EXECUTE FUNCTION reject_event_mutation()""")

    @contextmanager
    def connection(self):
        with psycopg.connect(self.database_url, connect_timeout=5,
                             options=f"-c search_path={self.schema},pg_catalog") as db:
            yield db

    def append_many(self, events: Iterable[IsolateEvent]) -> tuple[int, int]:
        # Validate duplicate identities inside the incoming batch before using COPY.
        submitted = 0
        unique = {}
        revisions = {}
        for event in events:
            submitted += 1
            payload = json.loads(event.canonical_json())
            previous = unique.get(event.event_id)
            key = (event.isolate_id, event.revision)
            if ((previous is not None and previous[1] != payload)
                    or (key in revisions and revisions[key] != event.event_id)):
                raise EventConflict("Event ID or isolate revision already exists with different data")
            unique[event.event_id] = (event, payload)
            revisions[key] = event.event_id
        if not submitted:
            return 0, 0

        with self.connection() as db:
            db.execute("""CREATE TEMPORARY TABLE incoming_events (
                event_id TEXT, isolate_id TEXT, revision INTEGER, patient_id TEXT,
                sample_date DATE, received_at TIMESTAMPTZ, payload JSONB
            ) ON COMMIT DROP""")
            with db.cursor().copy("COPY incoming_events FROM STDIN") as copy:
                for event, payload in unique.values():
                    copy.write_row((event.event_id, event.isolate_id, event.revision,
                                    event.patient_id, event.sample_date, event.received_at, Jsonb(payload)))
            # Conflict checking happens after insertion in the same transaction, which
            # also handles another writer inserting the same ID concurrently.
            inserted = db.execute("""INSERT INTO isolate_events
                (event_id, isolate_id, revision, patient_id, sample_date, received_at, payload)
                SELECT event_id, isolate_id, revision, patient_id, sample_date, received_at, payload
                FROM incoming_events ORDER BY event_id
                ON CONFLICT DO NOTHING""").rowcount
            conflict = db.execute("""SELECT 1 FROM incoming_events incoming
                LEFT JOIN isolate_events stored USING (event_id)
                WHERE stored.event_id IS NULL OR stored.payload IS DISTINCT FROM incoming.payload
                LIMIT 1""").fetchone()
            if conflict:
                raise EventConflict("Event ID or isolate revision already exists with different data")
        return inserted, submitted - inserted

    def events(self, isolate_id: str | None = None) -> list[IsolateEvent]:
        with self.connection() as db:
            if isolate_id is None:
                rows = db.execute("SELECT payload FROM isolate_events ORDER BY sequence").fetchall()
            else:
                rows = db.execute("SELECT payload FROM isolate_events WHERE isolate_id = %s ORDER BY revision",
                                  (isolate_id,)).fetchall()
        return [IsolateEvent.model_validate(row[0]) for row in rows]

    def count(self) -> int:
        with self.connection() as db:
            return db.execute("SELECT COUNT(*) FROM isolate_events").fetchone()[0]
