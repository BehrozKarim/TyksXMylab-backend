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
from aei.dataset import load_bundle
from aei.dataset_store import SOURCE_TABLES
from aei.migration import read_sqlite_history
from aei.postgres import PostgresEventStore
from aei.store import EventConflict, SQLiteEventStore
from test_pipeline import event
from test_import import fixture_tables, write_fixture


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
                          "TRUNCATE isolate_events CASCADE"):
            with self.assertRaises(psycopg.errors.CheckViolation):
                with self.store.connection() as db:
                    db.execute(statement)
        # The new enrichment FK also blocks an unqualified TRUNCATE before triggers run.
        with self.assertRaises(psycopg.errors.FeatureNotSupported):
            with self.store.connection() as db:
                db.execute("TRUNCATE isolate_events")
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

    def fixture_bundle(self, tables=None):
        with tempfile.TemporaryDirectory() as folder:
            return load_bundle(write_fixture(folder, tables))

    def table_counts(self):
        with self.store.connection() as db:
            return {table: db.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(
                sql.Identifier(table))).fetchone()[0] for table in SOURCE_TABLES}

    def test_complete_import_is_retry_safe_and_keeps_cultures_and_metadata(self):
        bundle = self.fixture_bundle()
        first = self.store.import_dataset(bundle)
        second = self.store.import_dataset(bundle)
        self.assertEqual((first["inserted"], first["duplicates"]), (2, 0))
        self.assertEqual((second["inserted"], second["duplicates"]), (0, 2))
        self.assertTrue(all(item["inserted"] == 0 for item in second["tables"].values()))
        self.assertEqual(self.table_counts(), dict(dataset_manifests=1, patients=1, wards=2,
                                                   antibiotics=2, culture_events=4, isolate_enrichments=2))
        with self.store.connection() as db:
            self.assertEqual(db.execute("SELECT payload->>'result' FROM culture_events WHERE culture_id='c2'").fetchone()[0], "negative")
            self.assertEqual(db.execute("SELECT payload->>'result' FROM culture_events WHERE culture_id='c3'").fetchone()[0], "mixed_growth")
            self.assertEqual(db.execute("SELECT payload->>'sex' FROM isolate_enrichments WHERE event_id='synthetic:i1:1'").fetchone()[0], "F")
            self.assertTrue(db.execute("SELECT (payload->>'is_screening')::boolean FROM antibiotics WHERE antibiotic_code='FOX'").fetchone()[0])
        self.assertEqual([e.canonical_json() for e in self.store.events()],
                         [e.canonical_json() for e in bundle.events])

    def test_enrichment_attaches_to_existing_exact_revision_without_rewriting_events(self):
        bundle = self.fixture_bundle()
        self.store.append_many(bundle.events)
        original = bundle.events[0]
        correction = original.model_copy(update={"event_id": "correction:i1:2", "revision": 2})
        self.store.append_many([correction])
        before = [e.canonical_json() for e in self.store.events()]
        result = self.store.import_dataset(bundle)
        self.assertEqual(result["inserted"], 0)
        self.assertEqual([e.canonical_json() for e in self.store.events()], before)
        with self.store.connection() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM isolate_enrichments WHERE event_id=%s",
                                        (correction.event_id,)).fetchone()[0], 0)

    def test_conflicting_existing_event_rolls_back_every_source_table(self):
        bundle = self.fixture_bundle()
        original = bundle.events[0]
        changed = original.model_copy(update={"tests": (original.tests[0].model_copy(update={"result": "R"}),)})
        self.store.append_many([changed])
        with self.assertRaises(EventConflict):
            self.store.import_dataset(bundle)
        self.assertEqual(self.store.count(), 1)
        self.assertEqual(self.store.events()[0].canonical_json(), changed.canonical_json())
        self.assertTrue(all(value == 0 for value in self.table_counts().values()))

    def test_failure_during_enrichment_rolls_back_events_cultures_and_manifest(self):
        with self.store.connection() as db:
            db.execute("""CREATE FUNCTION reject_test_enrichment() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'Injected late failure' USING ERRCODE = '23514'; END; $$""")
            db.execute("""CREATE TRIGGER fail_import BEFORE INSERT ON isolate_enrichments
                FOR EACH STATEMENT EXECUTE FUNCTION reject_test_enrichment()""")
        with self.assertRaises(psycopg.errors.CheckViolation):
            self.store.import_dataset(self.fixture_bundle())
        self.assertEqual(self.store.count(), 0)
        self.assertTrue(all(value == 0 for value in self.table_counts().values()))

    def test_different_source_manifest_requires_separate_schema(self):
        self.store.import_dataset(self.fixture_bundle())
        before = self.table_counts()
        tables = fixture_tables()
        tables["patients.csv"][0]["home_area"] = "Changed"
        with self.assertRaisesRegex(EventConflict, "separate --schema"):
            self.store.import_dataset(self.fixture_bundle(tables))
        self.assertEqual(self.table_counts(), before)
        with self.store.connection() as db:
            self.assertEqual(db.execute("SELECT payload->>'home_area' FROM patients").fetchone()[0], "Centre")

    def test_partial_import_can_extend_without_republishing_existing_records(self):
        from datetime import date
        bundle = self.fixture_bundle()
        initial = self.store.import_dataset(bundle, through=date(2026, 9, 26))
        self.assertEqual(initial["inserted"], 1)
        self.assertEqual(self.table_counts()["culture_events"], 3)
        complete = self.store.import_dataset(bundle)
        self.assertEqual((complete["inserted"], complete["duplicates"]), (1, 1))
        self.assertEqual(complete["tables"]["culture_events"], {"inserted": 1, "duplicates": 3})
        self.assertEqual(self.table_counts()["isolate_enrichments"], 2)

    def test_concurrent_source_imports_publish_only_one_copy(self):
        bundle = self.fixture_bundle()
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: self.store.import_dataset(bundle), range(2)))
        self.assertEqual(sum(result["inserted"] for result in results), 2)
        self.assertEqual(sum(result["duplicates"] for result in results), 2)
        self.assertEqual(self.table_counts()["culture_events"], 4)

    def test_source_history_rejects_update_delete_and_truncate(self):
        self.store.import_dataset(self.fixture_bundle())
        for table in SOURCE_TABLES:
            for template in ("UPDATE {} SET payload='{}'::jsonb", "DELETE FROM {}", "TRUNCATE {} CASCADE"):
                with self.subTest(table=table, statement=template), self.assertRaises(psycopg.errors.CheckViolation):
                    with self.store.connection() as db:
                        statement = (sql.SQL("UPDATE {} SET payload=%s").format(sql.Identifier(table))
                                     if template.startswith("UPDATE") else sql.SQL(template).format(sql.Identifier(table)))
                        db.execute(statement, (psycopg.types.json.Jsonb({}),) if template.startswith("UPDATE") else None)
