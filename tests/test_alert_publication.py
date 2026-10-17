import csv
import io
import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

import psycopg
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.types.json import Jsonb

from aei.alert_models import AlertConfiguration, AlertListRequest, AlertMonitor, SavedAlertAnalysis
from aei.alert_scenario import write_alert_scenario
from aei.alerts import advance_alert
from aei.api import create_app
from aei.dataset import load_bundle
from aei.postgres import PostgresEventStore
from aei.publication import PublicationService, export_csv
from aei.publication_models import ExpectedState, ReplayReset, ReplayStart, ServiceError
from test_pipeline import event


MONITOR = AlertMonitor(ward_code="ED")
CONFIG = AlertConfiguration(monitors=(MONITOR,))
ALL_STATES = ("new", "ongoing", "cleared", "insufficient_evidence")


@unittest.skipUnless(os.environ.get("AEI_TEST_DATABASE_URL"), "Set AEI_TEST_DATABASE_URL for PostgreSQL tests")
class AlertPublicationTests(unittest.TestCase):
    def setUp(self):
        self.url = os.environ["AEI_TEST_DATABASE_URL"]
        self.schema = "aei_test_" + uuid4().hex
        self.store = PostgresEventStore(self.url, self.schema)
        self.service = PublicationService(self.store, alert_configuration=CONFIG)
        with tempfile.TemporaryDirectory() as folder:
            self.bundle = load_bundle(write_alert_scenario(Path(folder) / "scenario"))
        self.store.import_dataset(self.bundle)
        self.state = self.service.reset(ReplayReset(seed_through="2026-09-26", replay_through="2026-10-01"))

    def tearDown(self):
        if not self.schema.startswith("aei_test_"):
            raise RuntimeError("Refusing cleanup outside disposable test schema")
        with psycopg.connect(self.url) as db:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))

    def listing(self, **changes):
        return self.service.alerts(AlertListRequest(**changes))

    def step(self):
        self.state = self.service.step(ExpectedState(expected_run_id=self.state.run_id,
                                                     expected_revision=self.state.state_revision))
        return self.listing(states=ALL_STATES)

    def counts(self):
        with self.store.connection() as db:
            return {table: db.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table))).fetchone()[0]
                    for table in ("isolate_events", "culture_events", "run_cultures", "run_isolates", "saved_analyses",
                                  "alert_batches", "alert_evaluations", "alerts", "alert_transitions")}

    def test_authored_scenario_lifecycle_preserves_one_identity_and_original_detection(self):
        initial = self.listing()
        self.assertEqual(initial.alerts, ())
        self.assertEqual(initial.monitors[0].evaluation.status, "insufficient_evidence")
        detected = self.step()
        alert = detected.alerts[0]
        original = detected.monitors[0]
        self.assertEqual((alert.status, original.evaluation.resistance_change_pp), ("new", 10))
        self.assertEqual((original.evaluation.baseline.n_tested, original.evaluation.recent.n_tested), (30, 30))
        self.assertEqual(alert.first_trigger_revision, 2)
        low = self.step().alerts[0]
        self.assertEqual((low.status, low.last_sufficient_state), ("insufficient_evidence", "new"))
        self.assertEqual(low.last_trigger_revision, 2)
        cleared = self.step().alerts[0]
        self.assertEqual((cleared.status, cleared.last_sufficient_state), ("cleared", "cleared"))
        self.assertEqual(self.listing().alerts, ())
        recurrence = self.step().alerts[0]
        self.assertEqual(recurrence.status, "ongoing")
        self.assertEqual(recurrence.alert_id, alert.alert_id)
        final = self.step().alerts[0]
        self.assertEqual((final.first_trigger_revision, final.last_trigger_revision), (2, 6))
        investigation = self.service.investigation(alert.alert_id, ExpectedState())
        self.assertEqual(investigation.original_detection, original)
        self.assertEqual(investigation.current_analysis.context.state_revision, self.state.state_revision)
        self.assertEqual(investigation.current_analysis.context.monitoring_as_of, date(2026, 10, 1))
        self.assertEqual([row.status for row in investigation.transitions],
                         ["new", "insufficient_evidence", "cleared", "ongoing"])
        with self.store.connection() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM alerts WHERE run_id=%s", (self.state.run_id,)).fetchone()[0], 1)
        self.assertEqual(self.store.count(), len(self.bundle.events))

    def test_reads_retries_and_start_pause_do_not_duplicate_or_advance_alert_lifecycle(self):
        listing = self.step()
        before = self.counts()
        for _ in range(3):
            self.assertEqual(self.listing(), listing)
            self.service.investigation(listing.alerts[0].alert_id, ExpectedState())
            with self.service.locked() as db:
                self.service.alert_engine.evaluate(db, self.state)
        self.assertEqual(self.counts(), before)
        self.service.start(ReplayStart(interval_seconds=60))
        self.state = self.service.pause(ExpectedState())
        current = self.listing()
        self.assertEqual(current.alerts[0].status, "new")
        self.assertEqual(current.monitoring_revision, listing.monitoring_revision)
        self.assertEqual(current.state_revision, self.state.state_revision)
        investigation = self.service.investigation(listing.alerts[0].alert_id, ExpectedState())
        self.assertEqual(investigation.current_analysis.context.state_revision, self.state.state_revision)
        self.assertEqual(len(investigation.transitions), 1)

    def test_low_counts_after_clearance_retain_last_sufficient_state_and_recurrence_identity(self):
        self.step()
        insufficient = self.step().monitors[0]
        cleared = self.step().alerts[0]
        later = insufficient.model_copy(update={"context": insufficient.context.model_copy(update={"state_revision": 5})})
        tracked = advance_alert(cleared, later)
        self.assertEqual((tracked.status, tracked.last_sufficient_state), ("insufficient_evidence", "cleared"))
        self.assertEqual(tracked.alert_id, cleared.alert_id)
        self.assertEqual(tracked.last_trigger_revision, cleared.last_trigger_revision)

    def test_upgrade_adopts_existing_run_at_present_revision_without_inventing_past_alerts(self):
        legacy = self.state.model_copy(update={"run_id": str(uuid4()), "state_revision": 17})
        with self.service.locked() as db:
            db.execute("INSERT INTO replay_runs (run_id, manifest_id, payload) VALUES (%s, %s, %s)",
                       (legacy.run_id, legacy.source_manifest_id, Jsonb(legacy.model_dump(mode="json"))))
            for table in ("run_cultures", "run_isolates"):
                db.execute(sql.SQL("INSERT INTO {} SELECT %s, event_id, published_revision FROM {} WHERE run_id=%s")
                           .format(sql.Identifier(table), sql.Identifier(table)), (legacy.run_id, self.state.run_id))
            db.execute("UPDATE active_run SET run_id=%s", (legacy.run_id,))
        adopted = self.listing()
        self.assertEqual((adopted.run_id, adopted.monitoring_revision, adopted.alerts), (legacy.run_id, 17, ()))
        self.state = legacy
        created = self.step().alerts[0]
        self.assertEqual(created.first_trigger_revision, 18)
        with self.store.connection() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM alert_batches WHERE run_id=%s", (legacy.run_id,)).fetchone()[0], 2)

    def test_configuration_is_frozen_per_run_and_restart_cannot_change_it(self):
        original = self.step()
        changed = AlertConfiguration(threshold_pp=50, monitors=(MONITOR,))
        restarted = PublicationService(PostgresEventStore(self.url, self.schema), alert_configuration=changed)
        self.assertEqual(restarted.alerts(AlertListRequest()), original)
        restarted.reset(ReplayReset(seed_through="2026-09-27", replay_through="2026-10-01"))
        new = restarted.alerts(AlertListRequest())
        self.assertEqual(new.configuration.threshold_pp, 50)
        self.assertNotEqual(new.configuration_hash, original.configuration_hash)
        self.assertEqual(new.monitors[0].evaluation.status, "below_threshold")
        self.assertEqual(new.alerts, ())

    def test_archived_investigation_never_mixes_active_run_and_frozen_exports_survive(self):
        original = self.step()
        alert = original.alerts[0]
        saved = self.service.analysis(alert.original_analysis_id)
        original_json, original_csv = saved.model_dump_json(), export_csv(saved)
        self.service.reset(ReplayReset(seed_through="2026-09-26", replay_through="2026-10-01"))
        investigation = self.service.investigation(alert.alert_id, ExpectedState())
        self.assertEqual(investigation.current_analysis_status, "archived_run")
        self.assertIsNone(investigation.current_analysis)
        self.assertEqual(investigation.original_detection, saved)
        self.assertEqual(self.service.analysis(saved.analysis_id).model_dump_json(), original_json)
        self.assertEqual(export_csv(self.service.analysis(saved.analysis_id)), original_csv)
        self.assertEqual(self.listing().alerts, ())

    def test_failure_after_alert_writes_rolls_back_records_clock_and_all_monitor_history(self):
        before_state, before_counts = self.service.state(), self.counts()
        evaluate = self.service.alert_engine.evaluate

        def fail(db, state):
            evaluate(db, state)
            raise RuntimeError("Injected after monitor persistence")

        with patch.object(self.service.alert_engine, "evaluate", side_effect=fail):
            with self.assertRaisesRegex(RuntimeError, "Injected"):
                self.step()
        self.assertEqual(self.service.state(), before_state)
        self.assertEqual(self.counts(), before_counts)
        self.assertEqual(self.step().alerts[0].status, "new")

    def test_worker_failure_rolls_back_publication_and_preserves_last_monitor_date(self):
        running = self.service.start(ReplayStart(interval_seconds=60))
        before = self.counts()
        with self.store.connection() as db:
            db.execute("UPDATE replay_runs SET next_tick_at=CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE run_id=%s",
                       (running.run_id,))
        with patch.object(self.service.alert_engine, "evaluate", side_effect=RuntimeError("Injected detector failure")):
            with self.assertLogs("aei.publication", level="ERROR"):
                self.service.worker_tick()
        state = self.service.state()
        self.assertEqual((state.status, state.demo_date), ("error", running.demo_date))
        self.assertEqual(self.counts(), before)
        self.assertEqual(self.listing().monitoring_as_of, running.demo_date)

    def test_concurrent_same_revision_steps_create_only_one_alert(self):
        barrier = threading.Barrier(2)
        request = ExpectedState(expected_run_id=self.state.run_id, expected_revision=self.state.state_revision)

        def advance(_):
            barrier.wait(timeout=5)
            try:
                return self.service.step(request)
            except ServiceError as error:
                return error

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(advance, range(2)))
        self.assertEqual(sum(isinstance(item, ServiceError) for item in results), 1)
        listing = self.listing()
        self.assertEqual(len(listing.alerts), 1)
        self.assertEqual(listing.monitoring_as_of, date(2026, 9, 27))
        self.assertEqual(len(self.service.investigation(listing.alerts[0].alert_id, ExpectedState()).transitions), 1)

    def test_reader_waits_for_whole_publication_including_alerts(self):
        entered, release = threading.Event(), threading.Event()
        evaluate = self.service.alert_engine.evaluate

        def hold(db, state):
            evaluate(db, state)
            entered.set()
            if not release.wait(timeout=10):
                raise RuntimeError("Test release timed out")

        with ThreadPoolExecutor(max_workers=2) as pool, patch.object(self.service.alert_engine, "evaluate", side_effect=hold):
            writer = pool.submit(self.service.step, ExpectedState())
            try:
                self.assertTrue(entered.wait(timeout=10))
                reader = pool.submit(self.listing)
                self.assertFalse(reader.done())
            finally:
                release.set()
            state, report = writer.result(timeout=10), reader.result(timeout=10)
        self.assertEqual(report.state_revision, state.state_revision)
        self.assertEqual(report.monitoring_as_of, state.demo_date)
        self.assertEqual(report.monitors[0].evaluation.recent.n_tested, 30)
        self.assertEqual(report.alerts[0].status, "new")

    def test_unpublished_events_and_dashboard_evidence_do_not_create_monitors(self):
        before = self.listing()
        self.store.append_many([event(f"outside-{i}", result="R") for i in range(40)])
        from aei.publication_models import EvidenceRequest
        self.service.evidence(EvidenceRequest(organism="Escherichia coli", specimen="urine", antibiotic_code="CIP", sex="F"))
        self.assertEqual(self.listing(), before)
        self.assertEqual(len(before.monitors), 1)

    def test_api_errors_filters_investigation_and_json_csv_suppression(self):
        detected = self.step()
        alert_id = detected.alerts[0].alert_id
        with TestClient(create_app(store=self.store)) as client:
            response = client.get("/api/v1/alerts")
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["alerts"][0]["status"], "new")
            detail = client.get(f"/api/v1/investigations/{alert_id}")
            self.assertEqual(detail.status_code, 200)
            self.assertEqual(detail.json()["original_detection"]["evaluation"]["resistance_change_pp"], 10)
            for params in ({"as_of": "2026-09-26"}, {"ward_code": "ED"}, {"states": "invalid"},
                           {"expected_revision": 2}):
                bad = client.get("/api/v1/alerts", params=params)
                self.assertEqual(bad.status_code, 422, bad.text)
                self.assertEqual(bad.json()["error"]["code"], "invalid_request")
            stale = client.get("/api/v1/alerts", params={"expected_run_id": "old", "expected_revision": 2})
            self.assertEqual((stale.status_code, stale.json()["error"]["code"]), (409, "stale_state"))
            unknown = client.get("/api/v1/investigations/unknown")
            self.assertEqual((unknown.status_code, unknown.json()["error"]["code"]), (404, "investigation_not_found"))
            low = self.step()
            report = low.monitors[0]
            exported = client.get(f"/api/v1/analyses/{report.analysis_id}/export?format=json")
            self.assertEqual(exported.status_code, 200)
            self.assertEqual(SavedAlertAnalysis.model_validate(exported.json()), report)
            self.assertIsNone(exported.json()["evaluation"]["recent"]["resistance_pct"])
            csv_export = client.get(f"/api/v1/analyses/{report.analysis_id}/export?format=csv")
            rows = list(csv.DictReader(io.StringIO(csv_export.text)))
            recent = next(row for row in rows if row["window"] == "recent")
            self.assertEqual((recent["n_tested"], recent["resistance_pct"], recent["evaluation.resistance_change_pp"]),
                             ("29", "", ""))
            self.assertEqual(recent["context.configuration_hash"], CONFIG.configuration_hash)
            self.step()
            self.assertEqual(client.get("/api/v1/alerts").json()["alerts"], [])
            historical = client.get("/api/v1/alerts", params=[("states", "cleared"), ("states", "ongoing")])
            self.assertEqual(historical.status_code, 200, historical.text)
            self.assertEqual(historical.json()["alerts"][0]["status"], "cleared")
            self.assertIn("/api/v1/investigations/{investigation_id}", client.get("/openapi.json").json()["paths"])

    def test_configuration_evaluations_and_transitions_are_append_only(self):
        self.step()
        for table in ("alert_configurations", "alert_batches", "alert_evaluations", "alert_transitions"):
            column = "monitoring_as_of" if table == "alert_batches" else "analysis_id" if table == "alert_evaluations" else "payload"
            for statement in (sql.SQL("UPDATE {} SET {}={}").format(sql.Identifier(table), sql.Identifier(column), sql.Identifier(column)),
                              sql.SQL("DELETE FROM {}").format(sql.Identifier(table)), sql.SQL("TRUNCATE {} CASCADE").format(sql.Identifier(table))):
                with self.subTest(table=table, statement=str(statement)):
                    with self.assertRaises(psycopg.errors.CheckViolation):
                        with self.store.connection() as db:
                            db.execute(statement)

    def test_disabled_monitors_and_unavailable_monitors_are_explicit_without_alerts(self):
        for config in (AlertConfiguration(monitors=()), AlertConfiguration(monitors=(AlertMonitor(antibiotic_code="MISSING"),))):
            service = PublicationService(self.store, alert_configuration=config)
            service.reset(ReplayReset(seed_through="2026-09-27", replay_through="2026-10-01"))
            listing = service.alerts(AlertListRequest())
            self.assertEqual(listing.alerts, ())
            self.assertEqual(len(listing.monitors), len(config.monitors))
            if config.monitors:
                self.assertEqual(listing.monitors[0].evaluation.status, "unavailable")


class AuthoredScenarioTests(unittest.TestCase):
    def test_scenario_is_valid_labeled_deterministic_and_never_overwrites_files(self):
        with tempfile.TemporaryDirectory() as folder:
            path = write_alert_scenario(Path(folder) / "scenario")
            before = {item.name: item.read_bytes() for item in path.iterdir()}
            bundle = load_bundle(path)
            self.assertTrue(bundle.report.valid)
            self.assertTrue(all(item.patient_id.startswith("authored-alert-demo-") for item in bundle.patients))
            self.assertEqual(bundle.report.manifest.manifest_id,
                             load_bundle(write_alert_scenario(Path(folder) / "another")).report.manifest.manifest_id)
            with self.assertRaises(FileExistsError):
                write_alert_scenario(path)
            self.assertEqual({item.name: item.read_bytes() for item in path.iterdir()}, before)
