"""Integration tests use disposable schemas in the explicitly configured test database."""

import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import uuid4

import psycopg
from fastapi.testclient import TestClient
from psycopg import sql

from aei.api import create_app
from aei.migration import read_sqlite_history
from aei.postgres import PostgresEventStore
from aei.store import EventConflict, SQLiteEventStore
from test_pipeline import event


@unittest.skipUnless(os.environ.get("AEI_TEST_DATABASE_URL"), "Set AEI_TEST_DATABASE_URL for PostgreSQL tests")
class PostgresTests(unittest.TestCase):
    def setUp(self):
        self.url = os.environ["AEI_TEST_DATABASE_URL"]
        self.schema = "aei_test_" + uuid4().hex
        self.store = PostgresEventStore(self.url, self.schema)

    def tearDown(self):
        if not self.schema.startswith("aei_test_"):
            raise RuntimeError("Refusing cleanup outside the disposable test schema")
        with psycopg.connect(self.url) as db:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))

    def test_bulk_ingestion_preserves_revisions_and_deduplicates_retries(self):
        events = [event(str(i)) for i in range(50)]
        self.assertEqual(self.store.append_many(events + [events[0]]), (50, 1))
        self.assertEqual(self.store.append_many(events), (0, 50))
        self.store.append_many([event("0", revision=2, result="R")])
        self.assertEqual([item.revision for item in self.store.events("0")], [1, 2])

    def test_changed_event_and_revision_conflicts_roll_back_entire_batch(self):
        self.store.append_many([event()])
        with self.assertRaises(EventConflict):
            self.store.append_many([event("new"), event(result="R")])
        self.assertEqual(self.store.count(), 1)
        changed_id = event().model_copy(update={"event_id": "another-id"})
        with self.assertRaises(EventConflict):
            self.store.append_many([event("new"), changed_id])
        self.assertEqual(self.store.count(), 1)

    def test_conflicting_input_batch_is_rejected(self):
        with self.assertRaises(EventConflict):
            self.store.append_many([event(), event(result="R")])
        self.assertEqual(self.store.count(), 0)

    def test_event_history_rejects_update_delete_and_truncate(self):
        self.store.append_many([event()])
        for statement in ("UPDATE isolate_events SET revision=2", "DELETE FROM isolate_events",
                          "TRUNCATE isolate_events"):
            with self.assertRaises(psycopg.errors.CheckViolation):
                with self.store.connection() as db:
                    db.execute(statement)
        self.assertEqual(self.store.count(), 1)

    def test_concurrent_identical_deliveries_are_counted_once(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.store.append_many([event()]), range(2)))
        self.assertEqual(sum(result[0] for result in results), 1)
        self.assertEqual(sum(result[1] for result in results), 1)
        self.assertEqual(self.store.count(), 1)

    def test_sqlite_migration_is_read_only_and_retry_safe(self):
        with tempfile.TemporaryDirectory() as folder:
            source = Path(folder) / "legacy.sqlite3"
            legacy = SQLiteEventStore(source)
            legacy.append_many([event(), event(revision=2, result="R")])
            before = source.read_bytes()
            events = read_sqlite_history(source)
            self.assertEqual(self.store.append_many(events), (2, 0))
            self.assertEqual(self.store.append_many(events), (0, 2))
            self.assertEqual(source.read_bytes(), before)
            self.assertEqual([item.canonical_json() for item in self.store.events()],
                             [item.canonical_json() for item in events])

    def test_postgres_api_reports_both_periods(self):
        with TestClient(create_app(store=self.store)) as client:
            events = [event(str(i), result="S" if i < 24 else "R").model_dump(mode="json") for i in range(30)]
            self.assertEqual(client.post("/events/batch", json=events).json()["inserted"], 30)
            self.assertEqual(client.get("/health").json()["storage"], "postgresql")
            result = client.get("/statistics/comparison", params={"as_of": "2026-09-27"})
            self.assertEqual(result.status_code, 200, result.text)
            for period in ("rolling_28_days", "rolling_12_months"):
                self.assertEqual(result.json()[period]["rows"][0]["resistance_percent"], 20)
            self.assertEqual(client.get("/isolates/0/history").status_code, 200)
