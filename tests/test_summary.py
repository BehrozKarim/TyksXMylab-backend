"""Facts, suppression, provider contract, durable jobs and publication isolation."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
from uuid import uuid4

import httpx
import psycopg
from fastapi.testclient import TestClient
from psycopg import sql

from aei.api import create_app
from aei.analysis_models import HistoryRequest
from aei.evidence import calculate_evidence
from aei.postgres import PostgresEventStore
from aei.publication import PublicationService
from aei.publication_models import EvidenceRequest, ExpectedState, PublishedContext, ReplayReset, SavedEvidence, ServiceError
from aei.summary_facts import complete_summary, extract_facts
from aei.summary_models import SavedSummary, SummaryRequest
from aei.summary_prompt import PROMPT
from aei.summary_provider import OpenAISummaryProvider, ProviderUnavailable, SummarySettings
from aei.summary_service import SummaryService
from test_evidence import request, snapshot
from test_feed import fixture_dataset
from test_pipeline import event


def report_for(events=None, **filters):
    source = snapshot(events if events is not None else [event(str(i), result='S' if i < 23 else 'R') for i in range(30)])
    result = calculate_evidence(source, request(**filters))
    return SavedEvidence(analysis_id='saved-report', windows=result.windows,
        context=PublishedContext(**result.context.model_dump(), run_id='test-run', state_revision=1,
            demo_date=date(2026, 9, 27), published_cultures=len(source.events), published_isolates=len(source.events),
            latest_visible_sample_date=date(2026, 9, 27), source_kind='directory', source_files=source.manifest.files,
            importer_version='test'))


def choices(facts):
    return {'sentences': [{'fact_id': f.fact_id, 'variant': 0} for f in facts if f.section == 'paragraph']}


def saved():
    return SavedSummary(summary_id='summary', analysis_id='saved-report', run_id='test-run', state_revision=1,
        method_version='evidence-v1', as_of='2026-09-27', status='queued', created_at=datetime.now(timezone.utc),
        provider='openai', model='test-model')


class SummaryFactsTests(unittest.TestCase):
    def test_prompt_preserves_user_structure_and_reporting_rules(self):
        for section in ('Scenario:', 'Task:', 'Reporting rules:', 'Requirements:', 'Guardrails:', 'Output:'):
            self.assertIn(section, PROMPT)
        self.assertIn('Exactly 30 qualifies', PROMPT)
        self.assertIn('not zero', PROMPT)
        self.assertNotIn('sensor', PROMPT)

    def test_thirty_reports_percentages_and_intervals_without_patient_records(self):
        report = report_for()
        facts = extract_facts(report)
        result = complete_summary(saved(), facts, choices(facts))
        self.assertEqual(result.output_mode, 'ai')
        self.assertIn('76.7%', result.paragraph)
        self.assertIn('30 tested isolates', result.paragraph)
        self.assertIn('2025-09-28 to 2026-09-27, inclusive', result.paragraph)
        self.assertTrue(80 <= len(result.paragraph.split()) <= 120)
        self.assertIn('95% Wilson', ' '.join(f.text for f in result.supporting_facts))
        text = json.dumps([f.model_dump(mode='json') for f in facts])
        for forbidden in ('patient_id', 'culture_id', 'event_id', 'n_s', 'n_r', 'source_files'):
            self.assertNotIn(f'"{forbidden}"', text)
        self.assertNotIn('synthetic', result.paragraph.lower())

    def test_twenty_nine_omits_percentages_intervals_and_sr_breakdowns(self):
        report = report_for([event(str(i)) for i in range(29)])
        # Even a corrupt persisted percentage must not leak when status suppresses it.
        windows = report.windows.model_copy(update={'rolling_28_days': report.windows.rolling_28_days.model_copy(
            update={'susceptibility_pct': 99.9})})
        facts = extract_facts(report.model_copy(update={'windows': windows}))
        result = complete_summary(saved(), facts, choices(facts))
        self.assertIn('insufficient data: 29 tested isolates', result.paragraph)
        self.assertNotIn('%', result.paragraph)
        self.assertNotIn('99.9', json.dumps([f.model_dump(mode='json') for f in facts]))
        self.assertFalse(any(f.fact_id.endswith('.interval') for f in facts))

    def test_empty_untested_and_not_applicable_are_distinct(self):
        cases = [(report_for([]), 'no eligible isolates'),
                 (report_for([event(tests=[])]), 'none of 1 eligible isolates'),
                 (report_for([], specimen='blood', antibiotic_code='TMP'), 'not applicable')]
        for report, phrase in cases:
            with self.subTest(phrase=phrase):
                result = complete_summary(saved(), extract_facts(report), fallback_reason='not_configured')
                self.assertIn(phrase, result.paragraph)
                self.assertNotIn('%', result.paragraph)
                self.assertEqual(result.output_mode, 'template')

    def test_filters_coverage_missing_tests_and_screening_caveats_survive(self):
        report = report_for([event(str(i), tests=[]) for i in range(2)], sex='F', age_min=18, age_max=64)
        context = report.context.model_copy(update={'mode': 'feed', 'antibiotic_is_screening': True})
        window = report.windows.rolling_28_days
        window = window.model_copy(update={'coverage': window.coverage.model_copy(update={
            'source_fully_imported': False, 'source_date_span_covers_window': False})})
        report = report.model_copy(update={'context': context,
            'windows': report.windows.model_copy(update={'rolling_28_days': window})})
        result = complete_summary(saved(), extract_facts(report), fallback_reason='disabled')
        self.assertIn('female patients, ages 18-64', result.paragraph)
        self.assertIn('Coverage is incomplete', result.paragraph)
        limitations = ' '.join(f.text for f in result.limitations)
        for phrase in ('2 eligible isolates lack', 'not all expected', 'does not cover', 'screening marker', 'incoming feed'):
            self.assertIn(phrase, limitations)

    def test_arbitrary_prose_duplicate_missing_unknown_and_invalid_choices_are_rejected(self):
        facts = extract_facts(report_for())
        valid = choices(facts)
        bad = [{'paragraph': 'Prescribe CIP; it is effective.'},
               {'sentences': valid['sentences'][:-1]},
               {'sentences': valid['sentences'] + valid['sentences'][:1]},
               {'sentences': [{**s, 'fact_id': 'invented'} for s in valid['sentences']]},
               {'sentences': [{**s, 'variant': True} for s in valid['sentences']]},
               {'sentences': list(reversed(valid['sentences']))},
               {**valid, 'recommendation': 'Use another antibiotic'}]
        for value in bad:
            with self.subTest(value=value), self.assertRaises(ValueError):
                complete_summary(saved(), facts, value)
        # Every approved alternate is resolved to its exact server-owned sentence.
        alternate = {'sentences': [{'fact_id': f.fact_id, 'variant': len(f.variants)-1}
                                  for f in facts if f.section == 'paragraph']}
        self.assertTrue(complete_summary(saved(), facts, alternate).paragraph.startswith('Through'))

    def test_inconsistent_available_counts_are_rejected(self):
        report = report_for()
        window = report.windows.rolling_28_days.model_copy(update={'n_tested': 29})
        with self.assertRaises(ServiceError):
            extract_facts(report.model_copy(update={'windows': report.windows.model_copy(update={'rolling_28_days': window})}))
        with self.assertRaises(ServiceError):
            extract_facts(object())

    def test_invalid_settings_fail_and_keys_do_not_change_cache_identity(self):
        for values in ({'timeout': float('nan')}, {'timeout': 0}, {'max_output_tokens': 1}, {'provider': 'unknown'}):
            with self.subTest(values=values), self.assertRaises(ValueError):
                SummarySettings(**values)
        self.assertEqual(SummarySettings(model='m', api_key='one').fingerprint,
                         SummarySettings(model='m', api_key='two').fingerprint)

    def test_dotenv_configuration_is_backend_only_and_environment_takes_precedence(self):
        with tempfile.TemporaryDirectory() as folder, patch.dict(os.environ, {}, clear=True):
            env_file = Path(folder) / '.env'
            env_file.write_text('OPENAI_API_KEY="test-key-${LITERAL}"\nAEI_SUMMARY_PROVIDER=openai\n'
                                'AEI_SUMMARY_MODEL=gpt-4o-mini\n', encoding='utf-8-sig')
            with patch('aei.summary_provider.SUMMARY_ENV_FILE', env_file):
                settings = SummarySettings.from_env()
                self.assertEqual(settings.model, 'gpt-4o-mini')
                self.assertEqual(settings.api_key, 'test-key-${LITERAL}')
                self.assertIsNone(settings.unavailable_reason)
                self.assertNotIn('OPENAI_API_KEY', os.environ)
                with patch.dict(os.environ, {'AEI_SUMMARY_MODEL': 'process-model', 'OPENAI_API_KEY': ''}):
                    overridden = SummarySettings.from_env()
                    self.assertEqual(overridden.model, 'process-model')
                    self.assertIsNone(overridden.api_key)
                    self.assertEqual(overridden.unavailable_reason, 'not_configured')
            with patch('aei.summary_provider.SUMMARY_ENV_FILE', Path(folder) / 'missing.env'):
                self.assertEqual(SummarySettings.from_env().unavailable_reason, 'not_configured')


class ProviderTests(unittest.IsolatedAsyncioTestCase):
    async def test_responses_contract_and_completed_structured_output(self):
        facts = extract_facts(report_for())
        async def handler(request):
            body = json.loads(request.content)
            self.assertEqual(str(request.url), 'https://api.openai.com/v1/responses')
            self.assertEqual(body['model'], 'chosen-model')
            self.assertFalse(body['store'])
            self.assertTrue(body['text']['format']['strict'])
            self.assertEqual(body['instructions'], PROMPT)
            self.assertNotIn('patient_id', body['input'])
            self.assertEqual(request.headers['Authorization'], 'Bearer test-key')
            return httpx.Response(200, json={'status': 'completed', 'output': [
                {'type': 'reasoning', 'summary': []}, {'type': 'message', 'content': [
                    {'type': 'output_text', 'text': json.dumps(choices(facts))}]}]})
        settings = SummarySettings(model='chosen-model', api_key='test-key')
        result = await OpenAISummaryProvider(settings, transport=httpx.MockTransport(handler)).generate(facts)
        self.assertEqual(result, choices(facts))
        self.assertNotIn('test-key', repr(settings))

    async def test_provider_refusal_incomplete_invalid_and_failures(self):
        facts = extract_facts(report_for())
        settings = SummarySettings(model='test', api_key='test-key')
        for status, payload, error in [(429, {}, ProviderUnavailable), (401, {}, ProviderUnavailable),
                (200, {'status': 'incomplete'}, ValueError),
                (200, {'status': 'completed', 'output': [{'type': 'message', 'content': [{'type': 'refusal'}]}]}, ValueError),
                (200, {'status': 'completed', 'output': []}, ValueError)]:
            with self.subTest(status=status, payload=payload):
                provider = OpenAISummaryProvider(settings, transport=httpx.MockTransport(lambda _: httpx.Response(status, json=payload)))
                with self.assertRaises(error):
                    await provider.generate(facts)


class FakeProvider:
    def __init__(self, response='valid', wait=False):
        self.response, self.wait = response, wait
        self.calls = 0
        self.entered, self.release = threading.Event(), threading.Event()

    async def generate(self, facts):
        self.calls += 1
        self.entered.set()
        while self.wait and not self.release.is_set():
            await asyncio.sleep(0.01)
        if isinstance(self.response, Exception):
            raise self.response
        return choices(facts) if self.response == 'valid' else self.response


@unittest.skipUnless(os.environ.get('AEI_TEST_DATABASE_URL'), 'Set AEI_TEST_DATABASE_URL for PostgreSQL tests')
class SummaryIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.url = os.environ['AEI_TEST_DATABASE_URL']
        self.schema = 'aei_test_summary_' + uuid4().hex
        self.addCleanup(self.cleanup)
        self.store = PostgresEventStore(self.url, self.schema)
        self.store.import_dataset(fixture_dataset())
        self.publication = PublicationService(self.store)
        self.publication.reset(ReplayReset(seed_through='2026-09-25', replay_through='2026-09-27'))
        self.report = self.publication.evidence(EvidenceRequest(organism='Escherichia coli', specimen='urine', antibiotic_code='CIP'))
        self.settings = SummarySettings(model='test-model', api_key='test-key', timeout=1)
        self.provider = FakeProvider()
        self.service = SummaryService(self.publication, self.settings, self.provider)
        self.request = SummaryRequest(analysis_id=self.report.analysis_id)

    def cleanup(self):
        if not self.schema.startswith('aei_test_summary_'):
            raise RuntimeError('Unexpected cleanup schema')
        with psycopg.connect(self.url) as db:
            db.execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(self.schema)))

    def tick(self):
        asyncio.run(self.service.worker_tick())

    def test_concurrent_requests_reuse_job_and_completed_snapshot_after_publication(self):
        with ThreadPoolExecutor(4) as pool:
            results = list(pool.map(lambda _: self.service.request(self.request), range(8)))
        self.assertEqual(len({r.summary_id for r in results}), 1)
        self.tick()
        result = self.service.get(results[0].summary_id)
        self.assertEqual((result.status, result.output_mode, self.provider.calls), ('ready', 'ai', 1))
        self.publication.step(ExpectedState())
        self.assertEqual(self.service.request(self.request), result)
        self.assertEqual(self.publication.analysis(self.report.analysis_id), self.report)
        self.assertEqual(self.service.get(result.summary_id).state_revision, self.report.context.state_revision)

    def test_configuration_changes_and_no_provider_return_distinct_basic_summaries(self):
        service = SummaryService(self.publication, SummarySettings(provider='none'))
        basic = service.request(self.request)
        self.assertEqual((basic.status, basic.output_mode, basic.fallback_reason), ('ready', 'template', 'disabled'))
        pending = self.service.request(self.request)
        self.assertNotEqual(basic.summary_id, pending.summary_id)
        self.tick()
        self.assertEqual(self.service.get(pending.summary_id).output_mode, 'ai')

    def test_changed_configuration_and_exhausted_recovery_use_fallback(self):
        job = self.service.request(self.request)
        changed = SummaryService(self.publication, SummarySettings(model='changed', api_key='key'), self.provider)
        asyncio.run(changed.worker_tick())
        self.assertEqual(changed.get(job.summary_id).fallback_reason, 'configuration_changed')
        self.assertEqual(self.provider.calls, 0)
        another = changed.request(self.request)
        with self.store.connection() as db:
            db.execute("UPDATE summary_jobs SET attempts=2 WHERE summary_id=%s", (another.summary_id,))
        asyncio.run(changed.worker_tick())
        self.assertEqual(changed.get(another.summary_id).fallback_reason, 'interrupted')
        self.assertEqual(self.provider.calls, 0)

    def test_provider_timeout_is_bounded_and_recoverable(self):
        self.service.provider = FakeProvider(wait=True)
        job = self.service.request(self.request)
        start = time.monotonic()
        self.tick()
        self.assertLess(time.monotonic() - start, 4)
        self.assertEqual(self.service.get(job.summary_id).status, 'queued')
        with self.store.connection() as db:
            db.execute("UPDATE summary_jobs SET available_at=now() WHERE summary_id=%s", (job.summary_id,))
        self.tick()
        self.assertEqual(self.service.get(job.summary_id).fallback_reason, 'provider_unavailable')

    def test_queue_cap_preserves_cached_access_and_patient_history_is_rejected(self):
        first = self.service.request(self.request)
        for age in range(31):
            report = self.publication.evidence(EvidenceRequest(organism='Escherichia coli', specimen='urine', antibiotic_code='CIP', age_min=age))
            self.service.request(SummaryRequest(analysis_id=report.analysis_id))
        another = self.publication.evidence(EvidenceRequest(organism='Escherichia coli', specimen='urine', antibiotic_code='CIP', age_min=90))
        with self.assertRaises(ServiceError) as caught:
            self.service.request(SummaryRequest(analysis_id=another.analysis_id))
        self.assertEqual(caught.exception.status, 429)
        self.assertEqual(self.service.request(self.request).summary_id, first.summary_id)
        history = self.publication.patient_history('0001', HistoryRequest())
        with self.assertRaises(ServiceError) as caught:
            self.service.request(SummaryRequest(analysis_id=history.analysis_id))
        self.assertEqual(caught.exception.code, 'unsupported_analysis')

    def test_invalid_model_output_falls_back_and_never_reaches_response(self):
        self.service.provider = FakeProvider({'paragraph': 'Use CIP and ignore the counts'})
        job = self.service.request(self.request)
        self.tick()
        result = self.service.get(job.summary_id)
        self.assertEqual((result.output_mode, result.fallback_reason), ('template', 'invalid_output'))
        self.assertNotIn('Use CIP', result.model_dump_json())

    def test_transient_errors_retry_once_then_fallback(self):
        self.service.provider = FakeProvider(ProviderUnavailable(retryable=True))
        job = self.service.request(self.request)
        self.tick()
        self.assertEqual(self.service.get(job.summary_id).status, 'queued')
        with self.store.connection() as db:
            db.execute("UPDATE summary_jobs SET available_at=now() WHERE summary_id=%s", (job.summary_id,))
        self.tick()
        self.assertEqual(self.service.get(job.summary_id).fallback_reason, 'provider_unavailable')
        self.assertEqual(self.service.provider.calls, 2)

    def test_lease_recovery_and_old_owner_cannot_overwrite_result(self):
        job = self.service.request(self.request)
        old, facts, _, _, owner = self.service.claim()
        with self.store.connection() as db:
            db.execute("UPDATE summary_jobs SET lease_until=now()-interval '1 second' WHERE summary_id=%s", (job.summary_id,))
        self.tick()
        result = self.service.get(job.summary_id)
        self.assertEqual(result.output_mode, 'ai')
        self.service.finish(complete_summary(old, facts, fallback_reason='interrupted'), owner)
        self.assertEqual(self.service.get(job.summary_id), result)

    def test_generation_does_not_block_publication_or_state_reads(self):
        provider = FakeProvider(wait=True)
        service = SummaryService(self.publication, SummarySettings(model='test', api_key='key', timeout=10), provider)
        job = service.request(self.request)
        with ThreadPoolExecutor(1) as pool:
            task = pool.submit(asyncio.run, service.worker_tick())
            try:
                self.assertTrue(provider.entered.wait(5))
                before = self.publication.state()
                after = self.publication.step(ExpectedState())
                self.assertGreater(after.state_revision, before.state_revision)
                self.assertEqual(service.get(job.summary_id).status, 'generating')
            finally:
                provider.release.set()
            task.result(timeout=5)
        self.assertEqual(service.get(job.summary_id).status, 'ready')

    def test_api_worker_contract_and_rejection_of_client_facts(self):
        with TestClient(create_app(store=self.store, summary_settings=self.settings, summary_provider=self.provider)) as client:
            response = client.post('/api/v1/ai/summaries', json={'analysis_id': self.report.analysis_id})
            self.assertEqual(response.status_code, 202)
            job = response.json()
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                result = client.get('/api/v1/ai/summaries/' + job['summary_id']).json()
                if result['status'] == 'ready':
                    break
                time.sleep(0.05)
            self.assertEqual(result['output_mode'], 'ai')
            self.assertEqual(result['analysis_id'], self.report.analysis_id)
            self.assertEqual(client.post('/api/v1/ai/summaries', json=self.request.model_dump()).status_code, 200)
            for body in ({'analysis_id': self.report.analysis_id, 'paragraph': 'Invented'},
                         {'analysis_id': self.report.analysis_id, 'language': 'xx'}):
                self.assertEqual(client.post('/api/v1/ai/summaries', json=body).status_code, 422)
            self.assertEqual(client.get('/api/v1/ai/summaries/missing').status_code, 404)
            self.assertEqual(client.post('/api/v1/ai/summaries', json={'analysis_id': 'missing'}).status_code, 404)


if __name__ == '__main__':
    unittest.main()
