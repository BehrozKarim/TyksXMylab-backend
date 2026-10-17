"""Storage contract and legacy SQLite adapter for migration and isolated unit tests."""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Protocol

from .models import IsolateEvent


class EventConflict(ValueError):
    pass


class EventStore(Protocol):
    backend: str
    def append_many(self, events: Iterable[IsolateEvent]) -> tuple[int, int]: ...
    def events(self, isolate_id: str | None = None) -> list[IsolateEvent]: ...
    def count(self) -> int: ...


class SQLiteEventStore:
    backend = "sqlite"
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connection() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("""CREATE TABLE IF NOT EXISTS isolate_events (
                event_id TEXT PRIMARY KEY,
                isolate_id TEXT NOT NULL,
                revision INTEGER NOT NULL CHECK (revision >= 1),
                payload TEXT NOT NULL,
                UNIQUE(isolate_id, revision)
            )""")
            db.execute("""CREATE TRIGGER IF NOT EXISTS forbid_event_update
                BEFORE UPDATE ON isolate_events BEGIN
                SELECT RAISE(ABORT, 'Event history is append-only'); END""")
            db.execute("""CREATE TRIGGER IF NOT EXISTS forbid_event_delete
                BEFORE DELETE ON isolate_events BEGIN
                SELECT RAISE(ABORT, 'Event history is append-only'); END""")

    @contextmanager
    def connection(self):
        db = sqlite3.connect(self.path, timeout=30)
        try:
            with db:
                yield db
        finally:
            db.close()

    def append_many(self, events: Iterable[IsolateEvent]) -> tuple[int, int]:
        inserted = duplicates = 0
        # One transaction: a conflicting record rolls back the whole submitted batch.
        with self.connection() as db:
            for event in events:
                payload = event.canonical_json()
                try:
                    db.execute("INSERT INTO isolate_events VALUES (?, ?, ?, ?)",
                               (event.event_id, event.isolate_id, event.revision, payload))
                    inserted += 1
                except sqlite3.IntegrityError as error:
                    existing = db.execute("SELECT payload FROM isolate_events WHERE event_id = ?",
                                          (event.event_id,)).fetchone()
                    if existing and existing[0] == payload:
                        duplicates += 1
                    else:
                        raise EventConflict("Event ID or isolate revision already exists with different data") from error
        return inserted, duplicates

    def events(self, isolate_id: str | None = None) -> list[IsolateEvent]:
        with self.connection() as db:
            if isolate_id is None:
                rows = db.execute("SELECT payload FROM isolate_events ORDER BY rowid").fetchall()
            else:
                rows = db.execute("SELECT payload FROM isolate_events WHERE isolate_id = ? ORDER BY revision",
                                  (isolate_id,)).fetchall()
        return [IsolateEvent.model_validate_json(row[0]) for row in rows]

    def count(self) -> int:
        with self.connection() as db:
            return db.execute("SELECT COUNT(*) FROM isolate_events").fetchone()[0]
