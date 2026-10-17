import json
import os
import socket
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import date
from unittest.mock import patch
from uuid import uuid4

import psycopg
import httpx
import uvicorn
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.types.json import Jsonb

from aei.alert_models import AlertListRequest
from aei.analysis_models import HistoryRequest, PatientSearchRequest
from aei.api import create_app
from aei.feed_models import CultureBundle, FeedBatch, FeedStart
from aei.feed_sender import culture_bundles, send_batch
from aei.postgres import PostgresEventStore
from aei.publication import PublicationService, export_csv
from aei.publication_models import EvidenceRequest, ExpectedState, ReplayReset, ReplayStart, ServiceError
from test_feed import changed_bundle, fixture_dataset


@contextmanager
def live_server(store):
    """Exercise real HTTP streaming; TestClient buffers an endless SSE response."""
    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(create_app(store=store), log_level="error", ws="none",
                                         timeout_graceful_shutdown=5))
    thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
    thread.start()
    try:
        deadline = time.monotonic() + 10
        while not server.started:
            if not thread.is_alive() or time.monotonic() > deadline:
                raise RuntimeError("HTTP test server failed to start")
            time.sleep(0.01)
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        thread.join(timeout=30)
        listener.close()
        if thread.is_alive():
            raise RuntimeError("HTTP test server failed to stop")


def next_state(lines):
    frame = {}
    for line in lines:
        if not line:
            if frame.get("event") == "state":
                return int(frame["id"]), json.loads(frame["data"])
            frame = {}
        elif ":" in line:
            name, value = line.split(":", 1)
            frame[name] = value.lstrip()
    raise AssertionError("SSE connection ended before a state notification")


@unittest.skipUnless(os.environ.get("AEI_TEST_DATABASE_URL"), "Set AEI_TEST_DATABASE_URL for PostgreSQL tests")
class LiveFeedTests(unittest.TestCase):
    def setUp(self):
        self.url = os.environ["AEI_TEST_DATABASE_URL"]
        self.schema = "aei_test_" + uuid4().hex
        self.store = PostgresEventStore(self.url, self.schema)
        self.addCleanup(self.cleanup)
        self.dataset = fixture_dataset()
        self.store.import_dataset(self.dataset)
        self.bundles = tuple(culture_bundles(self.dataset))
        self.service = PublicationService(self.store)
        self.state = self.service.feed.start(FeedStart(source_id="test-sender", source_label="Authored feed test",
            clock_mode="simulated", as_of="2026-09-25", seed_through="2026-09-25"))

    def cleanup(self):
        if not self.schema.startswith("aei_test_"):
            raise RuntimeError("Refusing cleanup outside disposable test schema")
        with psycopg.connect(self.url) as db:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))

    def batch(self, bundles=None, **changes):
        values = dict(run_id=self.state.run_id, source_id="test-sender", delivery_id="delivery-1",
                      as_of="2026-09-27", bundles=(changed_bundle(self.bundles[-1]),) if bundles is None else bundles)
        values.update(changes)
        return FeedBatch(**values)

    def publish(self, batch=None):
        receipt = self.service.feed.submit(batch or self.batch())
        self.service.feed.worker_tick()
        return self.service.feed.receipt(receipt.receipt_id)

    def counts(self):
        with self.store.connection() as db:
            return {table: db.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table))).fetchone()[0]
                    for table in ("culture_events", "isolate_events", "isolate_enrichments", "run_cultures", "run_isolates",
                                  "saved_analyses", "publication_notifications", "alert_batches")}

    def evidence(self):
        return self.service.evidence(EvidenceRequest(organism="Escherichia coli", specimen="urine", antibiotic_code="CIP", ward_code="MED"))

    def test_pending_delivery_is_hidden_and_complete_publication_updates_all_views(self):
        cursor, _ = self.service.updates.begin()
        receipt = self.service.feed.submit(self.batch())
        self.assertEqual(receipt.status, "pending")
        self.assertEqual(self.service.state(), self.state)
        self.assertEqual(self.service.updates.read_after(cursor), [])
        self.assertEqual(self.service.patients(PatientSearchRequest(search="live-patient")).patients, ())
        self.service.feed.worker_tick()
        receipt = self.service.feed.receipt(receipt.receipt_id)
        state = self.service.state()
        self.assertEqual((receipt.status, receipt.published_revision, state.state_revision), ("published", 2, 2))
        self.assertEqual((state.mode, state.status, state.demo_date), ("feed", "receiving", date(2026, 9, 27)))
        self.assertEqual((state.published_cultures, state.published_isolates), (2, 2))
        report = self.evidence()
        self.assertEqual(report.windows.rolling_28_days.n_r, 1)
        self.assertEqual(report.context.feed_source_label, "Authored feed test")
        self.assertEqual(report.context.feed_data_digest, state.feed_data_digest)
        self.assertTrue(report.windows.rolling_28_days.coverage.includes_feed_data)
        self.assertEqual(report.windows.rolling_28_days.coverage.source_cultures_imported, 4)
        self.assertTrue(report.windows.rolling_28_days.coverage.source_fully_imported)
        self.assertEqual(len(self.service.patients(PatientSearchRequest(search="live-patient")).patients), 1)
        self.publish(self.batch((), delivery_id="clock", as_of="2026-09-28"))
        history = self.service.patient_history("live-patient", HistoryRequest())
        self.assertEqual(history.cultures[0].isolates[0].event.tests[0].result, "R")

    def test_identical_delivery_and_new_id_redelivery_are_idempotent(self):
        original = self.publish()
        state, counts = self.service.state(), self.counts()
        self.assertEqual(self.service.feed.submit(self.batch()), original)
        duplicate = self.publish(self.batch(delivery_id="new-delivery-id"))
        self.assertEqual((duplicate.status, duplicate.published_revision), ("published", original.published_revision))
        self.assertEqual(self.service.state(), state)
        self.assertEqual(self.counts(), counts)
        changed = self.batch((changed_bundle(self.bundles[-1], result="S"),))
        with self.assertRaises(ServiceError) as caught:
            self.service.feed.submit(changed)
        self.assertEqual(caught.exception.code, "delivery_conflict")

    def test_concurrent_acceptance_and_workers_publish_once(self):
        batch = self.batch()
        with ThreadPoolExecutor(max_workers=3) as pool:
            receipts = list(pool.map(lambda _: self.service.feed.submit(batch), range(3)))
            self.assertEqual(len({item.receipt_id for item in receipts}), 1)
            list(pool.map(lambda _: self.service.feed.worker_tick(), range(3)))
        self.assertEqual(self.service.state().state_revision, 2)
        self.assertEqual(self.service.feed.receipt(receipts[0].receipt_id).status, "published")

    def test_invalid_targets_future_dates_and_replay_controls_are_rejected(self):
        for changes, code in (({"run_id": "old"}, "inactive_feed_run"), ({"source_id": "other"}, "feed_not_receiving"),
                              ({"as_of": "2026-09-26"}, "future_feed_sample")):
            with self.subTest(changes=changes), self.assertRaises(ServiceError) as caught:
                self.service.feed.submit(self.batch(**changes))
            self.assertEqual(caught.exception.code, code)
        for action, request in ((self.service.start, ReplayStart()), (self.service.pause, ExpectedState()),
                                 (self.service.step, ExpectedState())):
            with self.assertRaises(ServiceError) as caught:
                action(request)
            self.assertEqual(caught.exception.code, "feed_run_active")
        self.assertEqual(self.service.state(), self.state)

    def test_already_staged_samples_publish_only_the_delivered_bundles(self):
        counts = self.counts()
        receipt = self.publish(self.batch((self.bundles[1],)))  # Negative culture on September 26.
        self.assertEqual(receipt.status, "published")
        self.assertEqual(self.service.state().published_cultures, 2)
        self.assertEqual(self.service.state().published_isolates, 1)
        self.assertEqual(self.evidence().windows.rolling_28_days.n_eligible, 0)  # c4 is still hidden.
        self.assertEqual(self.counts()["culture_events"], counts["culture_events"])
        self.assertEqual(self.counts()["isolate_events"], counts["isolate_events"])

    def test_full_culture_correction_changes_results_and_preserves_old_exports(self):
        self.assertEqual(self.publish().status, "published")
        original = self.evidence()
        frozen = export_csv(original)
        corrected = changed_bundle(self.bundles[-1], revision=2, result="S")
        receipt = self.publish(self.batch((corrected,), delivery_id="correction"))
        self.assertEqual(receipt.status, "published")
        current = self.evidence().windows.rolling_28_days
        self.assertEqual((current.n_tested, current.n_s, current.n_r), (1, 1, 0))
        self.assertEqual(export_csv(self.service.analysis(original.analysis_id)), frozen)
        self.assertEqual(self.service.analysis(original.analysis_id).windows.rolling_28_days.n_r, 1)
        with self.store.connection() as db:
            self.assertEqual(db.execute("SELECT COUNT(*) FROM isolate_events WHERE isolate_id='live-isolate-0'").fetchone()[0], 2)

    def test_corrections_require_contiguous_revisions_and_same_isolate_set(self):
        for revision, expected in ((2, "missing_culture_revision"),):
            receipt = self.publish(self.batch((changed_bundle(self.bundles[-1], revision=revision),)))
            self.assertEqual((receipt.status, receipt.error_code), ("failed", expected))
        self.assertEqual(self.publish(self.batch(delivery_id="first")).status, "published")
        receipt = self.publish(self.batch((changed_bundle(self.bundles[-1], revision=3),), delivery_id="gap"))
        self.assertEqual(receipt.error_code, "invalid_culture_revision")
        value = changed_bundle(self.bundles[-1], revision=2).model_dump(mode="json", by_alias=True)
        value["isolates"] = []
        value["culture"].update(result="negative", n_isolates=0)
        receipt = self.publish(self.batch((CultureBundle.model_validate(value),), delivery_id="retract"))
        self.assertEqual(receipt.error_code, "unsupported_isolate_set_change")
        self.assertEqual(self.evidence().windows.rolling_28_days.n_r, 1)

    def test_late_collection_and_late_correction_do_not_move_clock_backwards(self):
        self.publish(self.batch((), as_of="2026-10-01", delivery_id="clock"))
        late = changed_bundle(self.bundles[-1], day="2026-09-24")
        self.assertEqual(self.publish(self.batch((late,))).status, "published")
        self.assertEqual(self.service.state().demo_date, date(2026, 10, 1))
        corrected = changed_bundle(self.bundles[-1], day="2026-09-24", revision=2, result="S")
        self.assertEqual(self.publish(self.batch((corrected,), delivery_id="correction")).status, "published")
        self.assertEqual(self.evidence().windows.rolling_28_days.n_s, 1)

    def test_failed_alert_rolls_back_data_state_and_notifications_then_retry_succeeds(self):
        receipt = self.service.feed.submit(self.batch())
        counts = self.counts()
        evaluate = self.service.alert_engine.evaluate

        def fail_after_writes(db, state):
            evaluate(db, state)
            raise RuntimeError("Injected failure after alerts")

        with patch.object(self.service.alert_engine, "evaluate", side_effect=fail_after_writes):
            with self.assertLogs("aei.feed", level="ERROR"):
                self.service.feed.worker_tick()
        failed = self.service.feed.receipt(receipt.receipt_id)
        self.assertEqual((failed.status, failed.error_code), ("failed", "publication_failed"))
        self.assertEqual(self.service.state(), self.state)
        self.assertEqual(self.counts(), counts)
        self.assertEqual(self.service.feed.retry(receipt.receipt_id).status, "pending")
        self.service.feed.worker_tick()
        self.assertEqual(self.service.feed.receipt(receipt.receipt_id).status, "published")

    def test_conflicting_batch_is_atomic_and_does_not_hide_error_behind_acceptance(self):
        conflicting = changed_bundle(self.bundles[-1], identity="conflict", patient="0001")
        value = conflicting.model_dump(mode="json", by_alias=True)
        value["patient"]["home_area"] = "Different immutable demographic"
        value["isolates"][0]["enrichment"]["home_area"] = value["patient"]["home_area"]
        conflicting = CultureBundle.model_validate(value)
        counts = self.counts()
        receipt = self.publish(self.batch((changed_bundle(self.bundles[-1]), conflicting)))
        self.assertEqual((receipt.status, receipt.error_code), ("failed", "source_conflict"))
        self.assertEqual(self.counts(), counts)

    def test_queue_survives_restart_and_old_run_queue_is_cancelled_after_reset(self):
        receipt = self.service.feed.submit(self.batch())
        restarted = PublicationService(PostgresEventStore(self.url, self.schema))
        restarted.feed.worker_tick()
        self.assertEqual(restarted.feed.receipt(receipt.receipt_id).status, "published")
        pending = self.service.feed.submit(self.batch((), delivery_id="queued-clock", as_of="2026-09-28"))
        reset = self.service.reset(ReplayReset(seed_through="2026-09-25", replay_through="2026-09-27"))
        self.service.feed.worker_tick()
        self.assertEqual(self.service.feed.receipt(pending.receipt_id).status, "cancelled")
        self.assertEqual(self.service.state(), reset)
        self.assertEqual(self.service.feed.submit(self.batch()), restarted.feed.receipt(receipt.receipt_id))
        with self.assertRaises(ServiceError):
            self.service.feed.retry(pending.receipt_id)

    def test_replay_reset_excludes_feed_only_records_and_source_import_remains_retryable(self):
        self.publish()
        original_report = self.evidence()
        self.store.import_dataset(self.dataset)
        state = self.service.reset(ReplayReset(seed_through="2026-09-27", replay_through="2026-09-27"))
        self.assertEqual((state.mode, state.published_cultures, state.published_isolates), ("replay", 4, 2))
        self.assertEqual(self.service.patients(PatientSearchRequest(search="live-patient")).patients, ())
        self.assertEqual(self.service.analysis(original_report.analysis_id), original_report)

    def test_state_and_alert_reads_stay_responsive_while_next_batch_is_processing(self):
        pending = self.service.feed.submit(self.batch())
        before_alerts = self.service.alerts(AlertListRequest())
        cursor, _ = self.service.updates.begin()
        entered, release = threading.Event(), threading.Event()
        evaluate = self.service.alert_engine.evaluate

        def hold(db, state):
            evaluate(db, state)
            entered.set()
            if not release.wait(10):
                raise RuntimeError("Test release timed out")

        with ThreadPoolExecutor(max_workers=2) as pool, patch.object(self.service.alert_engine, "evaluate", side_effect=hold):
            work = pool.submit(self.service.feed.worker_tick)
            try:
                self.assertTrue(entered.wait(5))
                self.assertEqual(pool.submit(self.service.state).result(timeout=2), self.state)
                self.assertEqual(pool.submit(self.service.alerts, AlertListRequest()).result(timeout=2), before_alerts)
                self.assertEqual(self.service.updates.read_after(cursor), [])
                self.assertEqual(self.service.feed.receipt(pending.receipt_id).status, "pending")
                accepted = pool.submit(self.service.feed.submit, self.batch((), delivery_id="next", as_of="2026-09-28"))
                self.assertEqual(accepted.result(timeout=2).status, "pending")
            finally:
                release.set()
            work.result(timeout=5)
        rows = self.service.updates.read_after(cursor)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][1]["state_revision"], 2)

    def test_wall_clock_advances_on_quiet_day_and_simulated_clock_does_not(self):
        self.service.feed.worker_tick()
        self.assertEqual(self.service.state(), self.state)
        with patch("aei.feed.utc_today", return_value=date(2026, 9, 27)):
            state = self.service.feed.start(FeedStart(source_id="wall", source_label="Wall-clock test", seed_through="2026-09-25"))
            with self.assertRaises(ServiceError):
                self.service.feed.submit(self.batch((), run_id=state.run_id, source_id="wall", as_of="2026-09-28"))
        with patch("aei.feed.utc_today", return_value=date(2026, 9, 28)):
            self.service.feed.worker_tick()
        new = self.service.state()
        self.assertEqual(new.demo_date, date(2026, 9, 28))
        self.assertEqual(new.published_cultures, state.published_cultures)
        self.assertEqual(new.state_revision, state.state_revision + 1)
        self.assertEqual(self.service.alerts(AlertListRequest()).monitoring_as_of, new.demo_date)

    def test_sse_log_resumes_across_publications_and_reset_without_patient_payloads(self):
        cursor, initial = self.service.updates.begin()
        self.assertEqual(initial[1]["run_id"], self.state.run_id)
        self.publish()
        self.publish(self.batch((), delivery_id="clock", as_of="2026-09-28"))
        rows = self.service.updates.read_after(cursor)
        self.assertEqual([row[1]["state_revision"] for row in rows], [2, 3])
        resumed, first = self.service.updates.begin(rows[0][0])
        self.assertIsNone(first)
        self.assertEqual(self.service.updates.read_after(resumed), rows[1:])
        frame = self.service.updates.encode(rows[0])
        self.assertIn("event: state\n", frame)
        self.assertNotIn("live-patient", frame)
        self.service.reset(ReplayReset(seed_through="2026-09-25", replay_through="2026-09-27"))
        self.assertNotEqual(self.service.updates.read_after(rows[-1][0])[-1][1]["run_id"], self.state.run_id)
        for bad in (-1, 2**63):
            with self.assertRaises(ServiceError):
                self.service.updates.begin(bad)

    def test_wall_clock_publication_after_midnight_uses_current_day(self):
        with patch("aei.feed.utc_today", return_value=date(2026, 9, 27)):
            state = self.service.feed.start(FeedStart(source_id="wall", source_label="Wall-clock test", seed_through="2026-09-25"))
            receipt = self.service.feed.submit(self.batch(run_id=state.run_id, source_id="wall"))
        with patch("aei.feed.utc_today", return_value=date(2026, 9, 28)):
            self.service.feed.worker_tick()
        self.assertEqual(self.service.state().demo_date, date(2026, 9, 28))
        self.assertEqual(self.service.feed.receipt(receipt.receipt_id).accepted_as_of, date(2026, 9, 27))

    def test_feed_inbox_and_notification_log_are_append_only(self):
        self.publish()
        for table in ("feed_inbox", "publication_notifications"):
            for query in ("UPDATE {} SET payload=payload", "DELETE FROM {}", "TRUNCATE {} CASCADE"):
                with self.assertRaises(psycopg.errors.CheckViolation):
                    with self.store.connection() as db:
                        db.execute(sql.SQL(query).format(sql.Identifier(table)))

    def test_http_contract_auth_validation_and_receipt_errors(self):
        with patch.dict(os.environ, {"AEI_FEED_TOKEN": "test-token-only"}), TestClient(create_app(store=self.store)) as client:
            payload = self.batch().model_dump(mode="json", by_alias=True)
            self.assertEqual(client.post("/api/v1/feed/batches", json=payload).status_code, 401)
            headers = {"Authorization": "Bearer test-token-only"}
            accepted = client.post("/api/v1/feed/batches", json=payload, headers=headers)
            self.assertEqual(accepted.status_code, 202, accepted.text)
            self.assertEqual(client.get(accepted.json()["receipt_url"], headers=headers).status_code, 200)
            missing = client.get("/api/v1/feed/receipts/missing", headers=headers)
            self.assertEqual((missing.status_code, missing.json()["error"]["code"]), (404, "receipt_not_found"))
            bad = client.get("/api/v1/updates", headers={"Last-Event-ID": "not-an-id"})
            self.assertEqual((bad.status_code, bad.json()["error"]["code"]), (422, "invalid_event_cursor"))
            payload["bundles"][0]["isolates"] = []
            self.assertEqual(client.post("/api/v1/feed/batches", json=payload, headers=headers).status_code, 422)
            self.assertIn("/api/v1/updates", client.get("/openapi.json").json()["paths"])

    def test_live_http_sender_and_sse_reconnect_deliver_committed_state(self):
        with patch.dict(os.environ, {"AEI_FEED_TOKEN": ""}), live_server(self.store) as base_url:
            with httpx.Client(base_url=base_url, timeout=15) as client:
                with client.stream("GET", "/api/v1/updates") as response:
                    self.assertEqual(response.status_code, 200)
                    self.assertTrue(response.headers["content-type"].startswith("text/event-stream"))
                    lines = response.iter_lines()
                    initial_id, initial = next_state(lines)
                    self.assertEqual(initial["run_id"], self.state.run_id)
                    receipt = send_batch(client, self.batch(), wait_seconds=10)
                    cursor, published = next_state(lines)
                    self.assertGreater(cursor, initial_id)
                    self.assertEqual(published["state_revision"], receipt["published_revision"])
                    self.assertEqual(published["published_cultures"], 2)
                    alerts = client.get("/api/v1/alerts").json()
                    self.assertEqual(alerts["monitoring_revision"], published["state_revision"])
                # Arrival while disconnected must be recovered by the browser's cursor.
                receipt = send_batch(client, self.batch((), delivery_id="disconnected", as_of="2026-09-28"))
                with client.stream("GET", "/api/v1/updates?after=0", headers={"Last-Event-ID": str(cursor)}) as response:
                    resumed_id, resumed = next_state(response.iter_lines())
                    self.assertGreater(resumed_id, cursor)
                    self.assertEqual(resumed["state_revision"], receipt["published_revision"])
                    self.assertEqual(resumed["demo_date"], "2026-09-28")

    def test_old_saved_json_exports_do_not_gain_new_provenance_defaults(self):
        saved = self.evidence()
        document = saved.model_dump(mode="json")
        old_id = str(uuid4())
        document["analysis_id"] = old_id
        for field in ("mode", "clock_mode", "feed_source_id", "feed_source_label", "feed_data_digest"):
            document["context"].pop(field)
        for window in document["windows"].values():
            window["coverage"].pop("coverage_scope")
            window["coverage"].pop("includes_feed_data")
        with self.store.connection() as db:
            db.execute("""INSERT INTO saved_analyses VALUES (%s, %s, %s, 'evidence-v1', 'evidence', 'legacy-schema-fixture', %s)""",
                       (old_id, saved.context.run_id, saved.context.state_revision, Jsonb(document)))
        with TestClient(create_app(store=self.store)) as client:
            self.assertEqual(client.get(f"/api/v1/analyses/{old_id}/export").json(), document)
            self.assertEqual(client.get(f"/api/v1/analyses/{old_id}").json(), document)
