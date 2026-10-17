"""Launcher isolation, durable resume, real HTTP delivery and process ownership."""

from contextlib import redirect_stdout
import io
import json
import os
from pathlib import Path
import socket
import signal
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event
import unittest
from unittest.mock import patch
from uuid import uuid4

import httpx
import psycopg
from psycopg import sql

from aei.demo import (DemoError, DemoSession, Processes, deliver, demo_plan, free_port,
                      configure_demo_alerts, install_interrupt_handlers, resolve_source, validate_frontend_dependencies)
from aei.alert_models import AlertConfiguration
from aei.feed_models import FeedStart
from aei.demo_control import DemoControls
from aei.publication import PublicationService
from aei.publication_models import EvidenceRequest
from test_feed import fixture_dataset
from test_live_feed import live_server


class DemoUnitTests(unittest.TestCase):
    def test_demo_uses_three_point_threshold_without_changing_ordinary_defaults(self):
        with patch.dict(os.environ, {}, clear=True):
            demo = configure_demo_alerts()
            self.assertEqual(demo.threshold_pp, 3)
            self.assertEqual(demo.minimum_tested, 30)
            self.assertEqual(demo.monitors, AlertConfiguration().monitors)
        self.assertEqual(AlertConfiguration().threshold_pp, 10)

    def test_explicit_alert_configuration_is_respected_and_invalid_path_fails(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / 'alerts.json'
            path.write_text('{"threshold_pp": 7}', encoding='utf-8')
            with patch.dict(os.environ, {'AEI_ALERT_CONFIG': str(path)}):
                self.assertEqual(configure_demo_alerts().threshold_pp, 7)
            with patch.dict(os.environ, {'AEI_ALERT_CONFIG': str(path.parent / 'missing.json')}):
                with self.assertRaises(FileNotFoundError):
                    configure_demo_alerts()

    def test_missing_new_font_dependency_fails_before_dashboard_startup(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            (root / 'package.json').write_text(json.dumps({
                'dependencies': {'react': '19', '@fontsource/barlow': '5'},
                'devDependencies': {'vite': '7'},
            }), encoding='utf-8')
            for package in ('react', 'vite'):
                installed = root / 'node_modules' / package
                installed.mkdir(parents=True)
                (installed / 'package.json').write_text('{}', encoding='utf-8')
            with self.assertRaisesRegex(DemoError, r'@fontsource/barlow.*npm.cmd ci'):
                validate_frontend_dependencies(root)
            font = root / 'node_modules/@fontsource/barlow'
            font.mkdir(parents=True)
            (font / 'package.json').write_text('{}', encoding='utf-8')
            validate_frontend_dependencies(root)

    def test_controls_require_proxy_credential_and_dashboard_origin(self):
        with DemoControls('demo-run', ['http://localhost:5173']) as control, httpx.Client(
                base_url=control.url, trust_env=False) as client:
            self.assertEqual(client.get('/status').status_code, 403)
            client.headers['Authorization'] = f'Bearer {control.token}'
            self.assertEqual(client.get('/status').json()['phase'], 'starting')
            self.assertEqual(client.post('/next', json={'delivery_id': 'one'}).status_code, 403)
            client.headers['Origin'] = 'http://unrelated.example'
            self.assertEqual(client.post('/next', json={'delivery_id': 'one'}).status_code, 403)
            client.headers['Origin'] = 'http://localhost:5173'
            self.assertEqual(client.post('/next', json=[]).status_code, 400)
            self.assertEqual(client.post('/next', json={'delivery_id': 'one'}).status_code, 409)
            control.update(phase='ready', next_batch={'delivery_id': 'one', 'cultures': 2, 'as_of': '2026-09-25'})
            self.assertEqual(client.post('/next', json={'delivery_id': 'one'}).status_code, 202)
            self.assertEqual(client.get('/status').json()['phase'], 'processing')
            self.assertEqual(client.post('/next', json={'delivery_id': 'one'}).status_code, 409)
            control.update(phase='ready', next_batch={'delivery_id': 'two', 'cultures': 2, 'as_of': '2026-09-26'})
            self.assertEqual(client.post('/next', json={'delivery_id': 'one'}).status_code, 409)
            self.assertEqual(client.get('/status').json()['phase'], 'ready')

    def test_explicit_missing_source_does_not_fall_back_to_present_data(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {}, clear=True):
            root = Path(folder)
            (root / 'synthetic_microbiology_dataset').mkdir()
            self.assertEqual(resolve_source(root=root), (root / 'synthetic_microbiology_dataset').resolve())
            with self.assertRaisesRegex(DemoError, 'Dataset not found'):
                resolve_source(root / 'missing.zip', root=root)

    def test_local_archive_fallback_and_explicit_environment(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {}, clear=True):
            root = Path(folder)
            (root / '.data').mkdir()
            archive = root / '.data/synthetic_microbiology_dataset.zip'
            archive.touch()
            self.assertEqual(resolve_source(root=root), archive.resolve())
            with patch.dict(os.environ, {'AEI_DATASET_PATH': str(root / 'absent')}):
                with self.assertRaises(DemoError):
                    resolve_source(root=root)

    def test_seed_precedes_arrivals_and_short_datasets_are_bounded(self):
        dataset = fixture_dataset()
        plan = demo_plan(dataset, 3, 20)
        self.assertEqual((plan['seed_through'], plan['through']), ('2026-09-24', '2026-09-27'))
        self.assertEqual(demo_plan(dataset, 366, 20)['seed_through'], '2026-09-24')

    def test_normal_schemas_are_rejected(self):
        for schema in ('aei', 'public', 'aei_demo_', 'aei_demo_bad-name', 'aei_demo_' + 'x' * 55):
            with self.subTest(schema=schema), self.assertRaises(DemoError):
                DemoSession('unused', schema)

    def test_port_conflicts_fail_without_stopping_the_listener(self):
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            listener.listen()
            port = listener.getsockname()[1]
            with self.assertRaisesRegex(DemoError, 'already in use'):
                free_port(port)
            self.assertEqual(listener.getsockname()[1], port)

    def test_inherited_ignored_interrupt_is_replaced_with_cleanup_exception(self):
        signals = [getattr(signal, name) for name in ('SIGINT', 'SIGTERM', 'SIGBREAK') if hasattr(signal, name)]
        old = {number: signal.getsignal(number) for number in signals}
        try:
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            install_interrupt_handlers()
            for number in signals:
                with self.subTest(number=number), self.assertRaises(KeyboardInterrupt):
                    signal.getsignal(number)(number, None)
        finally:
            for number, handler in old.items():
                signal.signal(number, handler)

    def test_supervisor_reports_child_failure_and_stops_only_its_other_child(self):
        with tempfile.TemporaryDirectory() as folder:
            supervisor = Processes(Path(folder))
            try:
                supervisor.start('running', [sys.executable, '-c', 'import time; time.sleep(90)'], folder, os.environ.copy())
                supervisor.start('failed', [sys.executable, '-c', 'raise SystemExit(7)'], folder, os.environ.copy())
                supervisor.children[1][1].wait(timeout=10)
                with self.assertRaisesRegex(DemoError, 'failed exited with code 7'):
                    supervisor.check()
            finally:
                supervisor.close()
            self.assertIsNotNone(supervisor.children[0][1].poll())


@unittest.skipUnless(os.environ.get('AEI_TEST_DATABASE_URL'), 'Set AEI_TEST_DATABASE_URL for PostgreSQL tests')
class DemoIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.url = os.environ['AEI_TEST_DATABASE_URL']
        self.schema = 'aei_demo_test_' + uuid4().hex
        self.dataset = fixture_dataset()
        self.plan = demo_plan(self.dataset, 3, 1)
        self.addCleanup(self.cleanup)

    def cleanup(self):
        if not self.schema.startswith('aei_demo_test_'):
            raise RuntimeError('Unexpected cleanup schema')
        with psycopg.connect(self.url) as db:
            db.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(self.schema)))

    def test_unowned_schema_is_not_initialized_or_changed(self):
        with psycopg.connect(self.url) as db:
            db.execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(self.schema)))
            db.execute(sql.SQL('CREATE TABLE {}.important (value INT)').format(sql.Identifier(self.schema)))
        with self.assertRaisesRegex(DemoError, 'not owned'):
            with DemoSession(self.url, self.schema):
                self.fail('Unowned schema was accepted')
        with psycopg.connect(self.url) as db:
            tables = db.execute('SELECT table_name FROM information_schema.tables WHERE table_schema=%s', (self.schema,)).fetchall()
        self.assertEqual(tables, [('important',)])

    def test_one_launcher_per_schema_and_lock_released_on_exit(self):
        with DemoSession(self.url, self.schema):
            with self.assertRaisesRegex(DemoError, 'Another demo launcher'):
                with DemoSession(self.url, self.schema):
                    self.fail('Concurrent ownership was accepted')
        with DemoSession(self.url, self.schema) as resumed:
            self.assertEqual(resumed.read(), {})

    def test_changed_plan_needs_restart_and_changed_dataset_needs_separate_schema(self):
        with DemoSession(self.url, self.schema) as session, redirect_stdout(io.StringIO()):
            session.prepare(self.dataset, self.plan)
            with self.assertRaisesRegex(DemoError, 'batch size changed'):
                session.prepare(self.dataset, {**self.plan, 'batch_size': 20})
            with self.assertRaisesRegex(DemoError, 'different dataset'):
                session.prepare(self.dataset, {**self.plan, 'manifest_id': 'different'}, restart=True)
            session.prepare(self.dataset, {**self.plan, 'batch_size': 20}, restart=True)
            self.assertTrue(session.read()['needs_start'])

    def test_resume_restart_and_http_publication_keep_sources_and_saved_reports(self):
        with DemoSession(self.url, self.schema) as session, redirect_stdout(io.StringIO()):
            store = session.prepare(self.dataset, self.plan)
            service = PublicationService(store)
            expected = service.state()
            with live_server(store) as origin, httpx.Client(base_url=origin, timeout=10) as client:
                state = session.ensure_run(client, expected.run_id)
                before = service.evidence(EvidenceRequest(organism='Escherichia coli', specimen='urine', antibiotic_code='CIP'))
                self.assertEqual(before.context.published_cultures, 0)
                # Interrupt pacing after one confirmed delivery, then resume the exact run.
                def interrupt_after_publication(seconds):
                    if seconds:
                        raise KeyboardInterrupt
                with self.assertRaises(KeyboardInterrupt):
                    deliver(client, session, self.dataset, state, delay=1, wait=interrupt_after_publication)
                self.assertEqual(service.state().published_cultures, 1)
                restarted = session.ensure_run(client, state['run_id'])
                self.assertEqual(restarted['run_id'], state['run_id'])
                sent, skipped = deliver(client, session, self.dataset, restarted, delay=0, wait=lambda _: None)
                self.assertEqual((sent, skipped), (3, 1))
                final = service.state()
                self.assertEqual(final.published_cultures, 4)
                self.assertEqual(service.analysis(before.analysis_id), before)
                self.assertEqual(deliver(client, session, self.dataset, restarted, delay=0, wait=lambda _: None), (0, 4))
                self.assertEqual(service.state(), final)
                with store.connection() as db:
                    self.assertEqual(db.execute('SELECT count(*) FROM feed_inbox').fetchone()[0], 4)
                    self.assertEqual(db.execute('SELECT count(*) FROM culture_events').fetchone()[0], 4)
                    self.assertEqual(db.execute('SELECT count(*) FROM isolate_events').fetchone()[0], 2)
                session.prepare(self.dataset, self.plan, restart=True)
                fresh = session.ensure_run(client, final.run_id)
                self.assertNotEqual(fresh['run_id'], final.run_id)
                self.assertEqual(fresh['published_cultures'], 0)
                self.assertEqual(service.analysis(before.analysis_id), before)

    def test_other_api_or_replaced_run_is_rejected_before_delivering(self):
        with DemoSession(self.url, self.schema) as session, redirect_stdout(io.StringIO()):
            store = session.prepare(self.dataset, self.plan)
            service = PublicationService(store)
            expected = service.state()
            with live_server(store) as origin, httpx.Client(base_url=origin, timeout=10) as client:
                with self.assertRaisesRegex(DemoError, 'not serving the expected'):
                    session.ensure_run(client, 'unrelated-server')
                self.assertEqual(service.state(), expected)
                session.ensure_run(client, expected.run_id)
                replacement = service.feed.start(FeedStart(source_id='other-sender', source_label='Other',
                    clock_mode='simulated', as_of='2026-09-25', seed_through='2026-09-25'))
                with self.assertRaisesRegex(DemoError, 'replaced or stopped'):
                    session.ensure_run(client, replacement.run_id)

    def test_manual_delivery_waits_for_each_click_and_publishes_via_http(self):
        with DemoSession(self.url, self.schema) as session, redirect_stdout(io.StringIO()):
            store = session.prepare(self.dataset, self.plan)
            service = PublicationService(store)
            with live_server(store) as origin, httpx.Client(base_url=origin, timeout=10) as feed:
                state = session.ensure_run(feed, service.state().run_id)
                stop = Event()
                def wait(seconds):
                    if stop.wait(seconds):
                        raise KeyboardInterrupt
                with DemoControls(state['run_id'], ['http://localhost:5173']) as control, ThreadPoolExecutor(1) as pool:
                    task = pool.submit(deliver, feed, session, self.dataset, state, wait=wait, control=control)
                    try:
                        with httpx.Client(base_url=control.url, trust_env=False, headers={
                            'Authorization': f'Bearer {control.token}', 'Origin': 'http://localhost:5173'}) as buttons:
                            for index in range(4):
                                deadline = time.monotonic() + 15
                                while True:
                                    status = buttons.get('/status').json()
                                    if status['phase'] == 'ready' and status['published_batches'] == index:
                                        break
                                    if time.monotonic() >= deadline:
                                        self.fail(f'Timed out waiting for next manual batch: {status}')
                                    time.sleep(0.05)
                                time.sleep(0.25)
                                self.assertEqual(service.state().published_cultures, index)
                                payload = {'delivery_id': status['next_batch']['delivery_id']}
                                self.assertEqual(buttons.post('/next', json=payload).status_code, 202)
                                self.assertEqual(buttons.post('/next', json=payload).status_code, 409)
                            self.assertEqual(task.result(timeout=15), (4, 0))
                            self.assertEqual(buttons.get('/status').json()['phase'], 'complete')
                            self.assertEqual(buttons.post('/next', json=payload).status_code, 409)
                            self.assertEqual(service.state().published_cultures, 4)
                            # Relaunching a completed source exposes completion without any click.
                            self.assertEqual(deliver(feed, session, self.dataset, state, wait=wait, control=control), (0, 4))
                    finally:
                        stop.set()


if __name__ == '__main__':
    unittest.main()
