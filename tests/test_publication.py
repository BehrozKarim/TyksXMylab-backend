"""Replay, concurrency and frozen-report contracts against disposable PostgreSQL schemas."""

import csv
import io
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from unittest.mock import patch
from uuid import uuid4

import psycopg
from fastapi.testclient import TestClient
from psycopg import sql

from aei.api import create_app
from aei.dataset import load_bundle
from aei.publication import PublicationService, export_csv
from aei.publication_models import EvidenceRequest, ExpectedState, ReplayReset, ReplayStart, ServiceError
from aei.postgres import PostgresEventStore
from test_import import fixture_tables, write_fixture
from test_pipeline import event


PARAMS = dict(organism="Escherichia coli", specimen="urine", antibiotic_code="CIP")


@unittest.skipUnless(os.environ.get("AEI_TEST_DATABASE_URL"), "Set AEI_TEST_DATABASE_URL for PostgreSQL tests")
class PublicationTests(unittest.TestCase):
    def setUp(self):
        self.url = os.environ["AEI_TEST_DATABASE_URL"]
        self.schema = "aei_test_" + uuid4().hex
        self.store = PostgresEventStore(self.url, self.schema)
        self.service = PublicationService(self.store)

    def tearDown(self):
        if not self.schema.startswith("aei_test_"):
            raise RuntimeError("Refusing cleanup outside disposable test schema")
        with psycopg.connect(self.url) as db:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))

    def import_source(self, tables=None, through=None):
        with tempfile.TemporaryDirectory() as folder:
            bundle = load_bundle(write_fixture(folder, tables))
        self.store.import_dataset(bundle, through=through)
        return bundle

    def reset(self, seed="2026-09-25", through="2026-09-27"):
        return self.service.reset(ReplayReset(seed_through=seed, replay_through=through))

    def report(self, **kwargs):
        return self.service.evidence(EvidenceRequest(**{**PARAMS, **kwargs}))

    @staticmethod
    def expected(state):
        return ExpectedState(expected_run_id=state.run_id, expected_revision=state.state_revision)

    def due_now(self):
        with self.store.connection() as db:
            db.execute("UPDATE replay_runs SET next_tick_at=CURRENT_TIMESTAMP - INTERVAL '1 second'")

    def test_initial_run_and_cache_survive_service_restart_and_raw_ingestion(self):
        self.import_source()
        first = self.report()
        second = self.report(as_of="2026-09-27", dedup_scope="selected_cohort")
        self.assertEqual(first, second)
        self.assertEqual(first.context.published_cultures, 4)
        self.assertEqual(first.context.published_isolates, 2)
        self.store.append_many([event("external")])
        restarted = PublicationService(PostgresEventStore(self.url, self.schema))
        self.assertEqual(restarted.evidence(EvidenceRequest(**PARAMS)), first)
        self.assertEqual(restarted.state().status, "completed")
        self.assertEqual(self.store.count(), 3)

    def test_reset_and_steps_publish_complete_bundles_without_future_leakage(self):
        self.import_source()
        before = [item.canonical_json() for item in self.store.events()]
        old = self.report(ward_code="MED")
        state = self.reset()
        self.assertNotEqual(old.context.run_id, state.run_id)
        report = self.report(ward_code="MED")
        self.assertEqual(report.windows.rolling_28_days.status, "empty_cohort")
        self.assertEqual((state.published_cultures, state.published_isolates), (1, 1))
        self.assertTrue(report.windows.rolling_28_days.coverage.source_fully_imported)
        # September 26 has only negative and mixed cultures, with no isolates.
        state = self.service.step(self.expected(state))
        self.assertEqual((state.published_cultures, state.published_isolates), (3, 1))
        self.assertEqual(state.latest_visible_sample_date, date(2026, 9, 26))
        after_negative = self.report(ward_code="MED")
        self.assertNotEqual(report.analysis_id, after_negative.analysis_id)
        self.assertEqual(after_negative.windows.rolling_28_days.n_eligible, 0)
        state = self.service.step(self.expected(state))
        self.assertEqual((state.status, state.published_cultures, state.published_isolates), ("completed", 4, 2))
        with self.store.connection() as db:
            revision = db.execute("""SELECT published_revision FROM run_isolates
                WHERE run_id=%s AND event_id='synthetic:i2:1'""", (state.run_id,)).fetchone()[0]
            self.assertEqual(revision, state.state_revision)
        self.assertEqual(self.report(ward_code="MED", sex="F").windows.rolling_28_days.n_r, 1)
        with self.assertRaises(ServiceError):
            self.service.step(self.expected(state))
        self.assertEqual(self.service.analysis(old.analysis_id), old)
        self.assertEqual([item.canonical_json() for item in self.store.events()], before)

    def test_old_run_token_is_rejected_even_when_revision_matches(self):
        self.import_source()
        old = self.reset()
        new = self.reset()
        self.assertEqual(old.state_revision, new.state_revision)
        with self.assertRaises(ServiceError) as caught:
            self.report(**self.expected(old).model_dump())
        self.assertEqual(caught.exception.code, "stale_state")
        self.assertEqual(caught.exception.details["state"]["run_id"], new.run_id)
        with self.store.connection() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM saved_analyses").fetchone()[0], 0)

    def test_future_date_and_invalid_reset_leave_current_state_unchanged(self):
        self.import_source()
        state = self.reset()
        with self.assertRaises(ServiceError) as caught:
            self.report(as_of="2026-09-26")
        self.assertEqual(caught.exception.code, "future_as_of")
        with self.assertRaises(ServiceError):
            self.reset(through="2026-09-30")
        self.assertEqual(self.service.state(), state)

    def test_partial_import_cannot_replay_unstaged_future_or_change_saved_results(self):
        bundle = self.import_source(through=date(2026, 9, 26))
        saved = self.report()
        self.assertFalse(saved.windows.rolling_28_days.coverage.source_fully_imported)
        with self.assertRaises(ServiceError) as caught:
            self.reset()
        self.assertEqual(caught.exception.code, "incomplete_source")
        self.store.import_dataset(bundle)
        self.assertEqual(self.report(), saved)
        self.assertEqual(self.service.state().published_cultures, 3)
        self.reset(seed="2026-09-27")
        self.assertTrue(self.report().windows.rolling_28_days.coverage.source_fully_imported)

    def test_empty_day_expires_first_isolate_without_new_arrivals(self):
        tables = fixture_tables()
        for record, day in ((tables["cultures.csv"][0], "2026-09-01"),
                            (tables["cultures.csv"][3], "2026-09-30")):
            stamp = date.fromisoformat(day)
            record.update(sample_date=day, iso_year=str(stamp.isocalendar().year), iso_week=str(stamp.isocalendar().week))
        self.import_source(tables)
        state = self.reset(seed="2026-09-28", through="2026-09-30")
        first = self.report()
        self.assertEqual(first.windows.rolling_28_days.n_s, 1)
        advanced = self.service.step(self.expected(state))
        self.assertEqual(advanced.published_cultures, state.published_cultures)
        self.assertEqual(advanced.latest_visible_sample_date, state.latest_visible_sample_date)
        self.assertEqual(advanced.state_revision, state.state_revision + 1)
        self.assertEqual(self.report().windows.rolling_28_days.status, "empty_cohort")
        self.assertEqual(self.report().windows.rolling_12_months.n_s, 1)

    def test_failed_publication_rolls_back_cultures_isolates_and_clock(self):
        self.import_source()
        state = self.reset()
        with self.store.connection() as db:
            db.execute("""CREATE FUNCTION fail_publication() RETURNS trigger LANGUAGE plpgsql AS $$
                BEGIN RAISE EXCEPTION 'Injected failure' USING ERRCODE='23514'; END; $$""")
            db.execute("""CREATE TRIGGER fail_step BEFORE INSERT ON run_isolates
                FOR EACH STATEMENT EXECUTE FUNCTION fail_publication()""")
        with self.assertRaises(psycopg.errors.CheckViolation):
            self.service.step(self.expected(state))
        self.assertEqual(self.service.state(), state)
        with self.store.connection() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM run_cultures WHERE run_id=%s", (state.run_id,)).fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM run_isolates WHERE run_id=%s", (state.run_id,)).fetchone()[0], 1)

    def test_concurrent_steps_with_same_expectations_advance_once(self):
        self.import_source()
        state = self.reset()
        barrier = threading.Barrier(2)

        def advance(_):
            barrier.wait(timeout=5)
            try:
                return self.service.step(self.expected(state))
            except ServiceError as error:
                return error

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(advance, range(2)))
        self.assertEqual(sum(isinstance(item, ServiceError) for item in results), 1)
        self.assertEqual(self.service.state().demo_date, date(2026, 9, 26))

    def test_repeated_start_and_multiple_workers_share_one_due_time(self):
        self.import_source()
        self.reset()
        first = self.service.start(ReplayStart(interval_seconds=60))
        self.assertEqual(self.service.start(ReplayStart()), first)
        with self.assertRaises(ServiceError):
            self.service.step(ExpectedState())
        self.due_now()
        other = PublicationService(self.store)
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda worker: worker.worker_tick(), [self.service, other]))
        state = self.service.state()
        self.assertEqual(state.demo_date, date(2026, 9, 26))
        self.assertEqual(state.state_revision, first.state_revision + 1)
        paused = self.service.pause(ExpectedState())
        self.assertEqual(paused.status, "paused")
        self.due_now()
        self.service.worker_tick()
        self.assertEqual(self.service.state(), paused)

    def test_worker_failure_preserves_publication_and_records_error_state(self):
        self.import_source()
        self.reset()
        started = self.service.start(ReplayStart())
        self.due_now()
        with patch.object(self.service, "_advance", side_effect=RuntimeError("injected")):
            with self.assertLogs("aei.publication", level="ERROR"):
                self.service.worker_tick()
        state = self.service.state()
        self.assertEqual(state.status, "error")
        self.assertEqual(state.demo_date, started.demo_date)
        self.assertEqual(state.published_cultures, started.published_cultures)
        self.assertTrue(state.last_error)

    def test_report_freezes_before_concurrent_reset_can_publish(self):
        self.import_source()
        initial = self.reset()
        entered, release, resetting = threading.Event(), threading.Event(), threading.Event()
        from aei.evidence import calculate_evidence

        def delayed(*args):
            entered.set()
            if not release.wait(timeout=5):
                raise TimeoutError("Test did not release analysis")
            return calculate_evidence(*args)

        def reset():
            resetting.set()
            return self.reset(seed="2026-09-27")

        with ThreadPoolExecutor(max_workers=2) as pool:
            with patch("aei.publication.calculate_evidence", delayed):
                report_future = pool.submit(self.report, ward_code="MED")
                self.assertTrue(entered.wait(timeout=5))
                reset_future = pool.submit(reset)
                self.assertTrue(resetting.wait(timeout=5))
                release.set()
                saved, new = report_future.result(timeout=5), reset_future.result(timeout=5)
        self.assertEqual(saved.context.run_id, initial.run_id)
        self.assertEqual(saved.context.state_revision, initial.state_revision)
        self.assertEqual(saved.windows.rolling_28_days.n_tested, 0)
        self.assertNotEqual(new.run_id, initial.run_id)
        self.assertEqual(self.service.analysis(saved.analysis_id), saved)
        self.assertEqual(self.report(ward_code="MED").windows.rolling_28_days.n_r, 1)

    def test_pause_acknowledges_inflight_step_before_stopping(self):
        self.import_source()
        self.reset(seed="2026-09-24")
        self.service.start(ReplayStart(interval_seconds=60))
        self.due_now()
        entered, release = threading.Event(), threading.Event()
        publish = self.service._publish

        def delayed(*args):
            result = publish(*args)
            entered.set()
            if not release.wait(timeout=5):
                raise TimeoutError("Test did not release publication")
            return result

        with ThreadPoolExecutor(max_workers=2) as pool:
            with patch.object(self.service, "_publish", delayed):
                ticking = pool.submit(self.service.worker_tick)
                self.assertTrue(entered.wait(timeout=5))
                pausing = pool.submit(self.service.pause, ExpectedState())
                release.set()
                ticking.result(timeout=5)
                state = pausing.result(timeout=5)
        self.assertEqual(state.status, "paused")
        self.assertEqual(state.demo_date, date(2026, 9, 25))
        self.due_now()
        self.service.worker_tick()
        self.assertEqual(self.service.state(), state)

    def test_run_membership_and_saved_analyses_are_append_only(self):
        self.import_source()
        self.report()
        for table in ("run_cultures", "run_isolates", "saved_analyses"):
            for statement in (f"UPDATE {table} SET run_id=run_id", f"DELETE FROM {table}", f"TRUNCATE {table} CASCADE"):
                with self.subTest(statement=statement), self.assertRaises(psycopg.errors.CheckViolation):
                    with self.store.connection() as db:
                        db.execute(statement)

    def test_api_metadata_errors_exports_and_legacy_compatibility(self):
        self.import_source()
        with TestClient(create_app(store=self.store)) as client:
            meta = client.get("/api/v1/meta").json()
            self.assertEqual(meta["periods"], ["rolling_28_days", "rolling_12_months"])
            self.assertEqual([ward["ward_code"] for ward in meta["wards"]], ["ED", "MED"])
            self.assertEqual(meta["organisms"], ["Escherichia coli"])
            self.assertTrue(meta["antibiotics"][1]["is_screening"])
            saved = client.get("/api/v1/evidence", params=PARAMS).json()
            export = f'/api/v1/analyses/{saved["analysis_id"]}/export'
            self.assertEqual(client.get(export).json(), saved)
            self.assertEqual(set(saved["context"]["source_files"]), set(fixture_tables()))
            rows = list(csv.DictReader(io.StringIO(client.get(export, params={"format": "csv"}).text)))
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["context.run_id"], saved["context"]["run_id"])
            self.assertEqual(rows[0]["susceptibility_pct"], "")
            for url, params, status in [
                ("/api/v1/evidence", {}, 422),
                ("/api/v1/evidence", {**PARAMS, "age_min": -1}, 422),
                ("/api/v1/evidence", {**PARAMS, "ward_code": "unknown"}, 422),
                ("/api/v1/evidence", {**PARAMS, "expected_revision": 1}, 422),
                ("/api/v1/analyses/missing", {}, 404),
                ("/api/v1/missing", {}, 404),
                (export, {"format": "pdf"}, 422),
            ]:
                result = client.get(url, params=params)
                self.assertEqual(result.status_code, status, result.text)
                self.assertEqual(set(result.json()), {"error", "request_id"})
                self.assertEqual(result.json()["request_id"], result.headers["x-request-id"])
            invalid = client.post("/api/v1/replay/start", json={"interval_seconds": 0})
            self.assertEqual(invalid.status_code, 422)
            self.assertIn("error", invalid.json())
            self.assertIn("detail", client.get("/statistics").json())
            self.assertEqual(client.get("/statistics", params={"as_of": "2026-09-27"}).status_code, 200)
            preflight = client.options("/api/v1/evidence", headers={"Origin": "http://localhost:3000",
                "Access-Control-Request-Method": "GET"})
            self.assertEqual(preflight.headers["access-control-allow-origin"], "http://localhost:3000")

    def test_background_replay_completes_and_saved_json_survives_http_reset(self):
        self.import_source()
        with TestClient(create_app(store=self.store)) as client:
            client.post("/api/v1/replay/reset", json={"seed_through": "2026-09-25"}).raise_for_status()
            frozen = client.get("/api/v1/evidence", params=PARAMS).json()
            client.post("/api/v1/replay/start", json={"interval_seconds": 0.05}).raise_for_status()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                state = client.get("/api/v1/state").json()
                if state["status"] == "completed":
                    break
                time.sleep(0.05)
            self.assertEqual(state["status"], "completed")
            self.assertEqual(state["published_cultures"], 4)
            client.post("/api/v1/replay/reset", json={"seed_through": "2026-09-24"}).raise_for_status()
            self.assertEqual(client.get(f'/api/v1/analyses/{frozen["analysis_id"]}').json(), frozen)
            response = client.get("/api/v1/evidence", params={**PARAMS,
                "expected_run_id": frozen["context"]["run_id"], "expected_revision": frozen["context"]["state_revision"]})
            self.assertEqual(response.status_code, 409)

    def test_unexpected_api_errors_have_safe_envelope_and_request_id(self):
        self.import_source()
        with TestClient(create_app(store=self.store), raise_server_exceptions=False) as client:
            with patch.object(PublicationService, "meta", side_effect=RuntimeError("private database details")):
                with self.assertLogs("aei.api_v1", level="ERROR"):
                    response = client.get("/api/v1/meta")
            self.assertEqual(response.status_code, 500)
            self.assertEqual(response.json()["error"]["code"], "internal_error")
            self.assertNotIn("private database details", response.text)
            self.assertEqual(response.json()["request_id"], response.headers["x-request-id"])

    def test_cli_replay_uses_same_bundles_clock_and_frozen_evidence(self):
        with tempfile.TemporaryDirectory() as folder:
            source = write_fixture(folder)
            command = [sys.executable, "-m", "aei", "--database-url", self.url, "--schema", self.schema]
            replay = subprocess.run(command + ["replay", "--dataset-dir", str(source),
                "--from-date", "2026-09-26", "--through", "2026-09-27", "--delay", "0"],
                capture_output=True, text=True, encoding="utf-8", timeout=30)
            self.assertEqual(replay.returncode, 0, replay.stderr)
            lines = [json.loads(line) for line in replay.stdout.splitlines()]
            self.assertEqual((lines[0]["new_cultures"], lines[0]["new_isolates"]), (2, 0))
            self.assertEqual(lines[-1]["state"]["status"], "completed")
            evidence = subprocess.run(command + ["evidence", "--organism", PARAMS["organism"],
                "--specimen", "urine", "--antibiotic", "CIP"], capture_output=True, text=True,
                encoding="utf-8", timeout=30)
            self.assertEqual(evidence.returncode, 0, evidence.stderr)
            document = json.loads(evidence.stdout)
            self.assertEqual(document["context"]["run_id"], lines[-1]["state"]["run_id"])
            self.assertEqual(self.service.analysis(document["analysis_id"]).model_dump(mode="json"), document)

    def test_csv_escapes_formula_labels_and_keeps_counts_and_provenance(self):
        tables = fixture_tables()
        for row in tables["isolates.csv"] + tables["susceptibility.csv"]:
            row["organism"] = "=custom-label"
        self.import_source(tables)
        report = self.report(organism="=custom-label")
        rows = list(csv.DictReader(io.StringIO(export_csv(report))))
        self.assertEqual(rows[0]["context.filters.organism"], "'=custom-label")
        self.assertEqual(rows[0]["n_tested"], "1")
        self.assertEqual(rows[0]["context.source_manifest_id"], report.context.source_manifest_id)

    def test_equivalent_known_at_offsets_share_one_frozen_report(self):
        self.import_source()
        first = self.report(known_at="2026-09-27T12:00:00Z")
        second = self.report(known_at="2026-09-27T15:00:00+03:00")
        self.assertEqual(first, second)
