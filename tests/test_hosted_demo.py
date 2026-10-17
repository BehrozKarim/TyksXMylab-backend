"""Public hosted controls, durable progress and concurrent-click idempotency."""

import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch
from uuid import uuid4

import psycopg
from fastapi.testclient import TestClient
from psycopg import sql

from aei.api import create_app
from aei.hosted_demo import HostedDemo, main
from aei.postgres import PostgresEventStore
from aei.publication import PublicationService
from aei.publication_models import EvidenceRequest
from aei.summary_provider import SummarySettings
from test_feed import fixture_dataset


class HostedSetupTests(unittest.TestCase):
    def test_setup_rejects_implicit_database_and_normal_schema_before_reading_data(self):
        from contextlib import redirect_stderr
        import io
        for environment in ({}, {"AEI_DATABASE_URL": "explicit", "AEI_DATABASE_SCHEMA": "aei"}):
            with self.subTest(environment=environment), patch.dict(os.environ, environment, clear=True), \
                    patch("sys.argv", ["hosted_demo", "--source", "never-read.zip"]), \
                    patch("aei.dataset.load_bundle") as load, redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as error:
                    main()
                self.assertEqual(error.exception.code, 2)
                load.assert_not_called()


@unittest.skipUnless(os.environ.get("AEI_TEST_DATABASE_URL"), "Set AEI_TEST_DATABASE_URL for PostgreSQL tests")
class HostedDemoTests(unittest.TestCase):
    def setUp(self):
        self.url = os.environ["AEI_TEST_DATABASE_URL"]
        self.schema = "aei_test_" + uuid4().hex
        self.store = PostgresEventStore(self.url, self.schema)
        self.addCleanup(self.cleanup)
        self.dataset = fixture_dataset()
        self.store.import_dataset(self.dataset)
        self.publication = PublicationService(self.store)
        self.demo = HostedDemo(self.publication)
        self.state = self.demo.prepare(self.dataset, days=2, batch_size=1)

    def cleanup(self):
        if not self.schema.startswith("aei_test_"):
            raise RuntimeError("Refusing cleanup outside disposable test schema")
        with psycopg.connect(self.url) as db:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))

    def next_id(self):
        return self.demo.status()["next_batch"]["delivery_id"]

    def test_restart_resumes_pending_and_published_batches_without_reset(self):
        delivery = self.next_id()
        accepted, status = self.demo.next(delivery)
        self.assertTrue(accepted)
        self.assertEqual(status["phase"], "processing")
        restarted = HostedDemo(PublicationService(PostgresEventStore(self.url, self.schema)))
        self.assertEqual(restarted.status(), status)
        self.assertEqual(restarted.prepare(self.dataset).run_id, self.state.run_id)
        restarted.publication.feed.worker_tick()
        published = restarted.status()
        self.assertEqual(published["published_batches"], 1)
        self.assertGreater(published["last_publication"]["revision"], self.state.state_revision)
        self.assertFalse(restarted.next(delivery)[0])
        self.assertNotEqual(published["next_batch"]["delivery_id"], delivery)

    def test_concurrent_clicks_and_stale_tabs_accept_only_one_delivery(self):
        delivery = self.next_id()
        with ThreadPoolExecutor(max_workers=2) as workers:
            results = list(workers.map(self.demo.next, [delivery, delivery]))
        self.assertEqual(sum(accepted for accepted, _ in results), 1)
        with self.store.connection() as db:
            self.assertEqual(db.execute("SELECT count(*) FROM feed_inbox").fetchone()[0], 1)
        self.assertFalse(self.demo.next("unknown-batch")[0])

    def test_completion_matches_original_source_and_preserves_dates(self):
        count = 0
        while self.demo.status()["phase"] == "ready":
            self.assertTrue(self.demo.next(self.next_id())[0])
            self.publication.feed.worker_tick()
            count += 1
        status = self.demo.status()
        self.assertEqual(status["phase"], "complete")
        self.assertEqual(status["published_batches"], count)
        self.assertIsNone(status["next_batch"])
        state = self.publication.state()
        self.assertEqual(state.published_cultures, len(self.dataset.cultures))
        self.assertEqual(state.demo_date, self.dataset.report.manifest.source_end)
        self.assertEqual(self.store.count(), len(self.dataset.events))

    def test_failed_delivery_blocks_later_batches_and_retry_continues(self):
        self.demo.next(self.next_id())
        with self.store.connection() as db:
            receipt = db.execute("SELECT receipt_id FROM feed_jobs").fetchone()[0]
            db.execute("UPDATE feed_jobs SET status='failed', error_code='temporary_failure'")
        self.assertEqual(self.demo.status()["phase"], "error")
        self.assertFalse(self.demo.next(self.next_id())[0])
        self.publication.feed.retry(receipt)
        self.publication.feed.worker_tick()
        self.assertEqual(self.demo.status()["published_batches"], 1)

    def test_prepare_is_atomic_and_explicit_restart_preserves_history(self):
        previous = self.publication.state()
        with patch("aei.feed_sender.batches", side_effect=RuntimeError("plan failed")):
            with self.assertRaisesRegex(RuntimeError, "plan failed"):
                self.demo.prepare(self.dataset, restart=True)
        self.assertEqual(self.publication.state(), previous)
        restarted = self.demo.prepare(self.dataset, restart=True)
        self.assertNotEqual(restarted.run_id, previous.run_id)
        with self.store.connection() as db:
            self.assertIsNotNone(db.execute("SELECT 1 FROM hosted_demo_batches WHERE run_id=%s",
                                          (previous.run_id,)).fetchone())

    def test_public_api_ai_and_demo_require_no_password_but_feed_keeps_token(self):
        with patch.dict(os.environ, {"AEI_HOSTED_DEMO": "1", "AEI_FEED_TOKEN": "test-feed-token"}):
            client = TestClient(create_app(store=self.store, summary_settings=SummarySettings(provider="none")))
            self.addCleanup(client.close)
            self.assertEqual(client.get("/api/v1/state").status_code, 200)
            status = client.get("/api/v1/demo/status")
            self.assertEqual(status.status_code, 200)
            report = self.publication.evidence(EvidenceRequest(organism="Escherichia coli",
                specimen="urine", antibiotic_code="CIP", ward_code="MED"))
            summary = client.post("/api/v1/ai/summaries", json={"analysis_id": report.analysis_id, "language": "en"})
            self.assertEqual(summary.status_code, 200, summary.text)
            self.assertEqual(summary.json()["output_mode"], "template")
            self.assertEqual(client.get("/api/v1/feed/receipts/unknown").status_code, 401)
            body = {"delivery_id": status.json()["next_batch"]["delivery_id"]}
            self.assertEqual(client.post("/api/v1/demo/next", json=body).status_code, 202)
            self.assertEqual(client.post("/api/v1/demo/next", json=body).status_code, 409)

    def test_hosted_routes_are_absent_in_normal_local_startup(self):
        with patch.dict(os.environ, {"AEI_HOSTED_DEMO": "0"}):
            client = TestClient(create_app(store=self.store, summary_settings=SummarySettings(provider="none")))
            self.addCleanup(client.close)
            self.assertEqual(client.get("/api/v1/demo/status").status_code, 404)


if __name__ == "__main__":
    unittest.main()
