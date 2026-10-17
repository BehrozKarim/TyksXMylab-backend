"""Durable on-demand jobs with short transactions and a separate bounded worker."""

import asyncio
import hashlib
import logging
from datetime import datetime, timezone
from uuid import uuid4

from psycopg.types.json import Jsonb

from .publication_models import ServiceError
from .summary_facts import complete_summary, extract_facts
from .summary_models import SavedSummary, SummaryFact
from .summary_provider import OpenAISummaryProvider, ProviderUnavailable, SummarySettings


class SummaryService:
    def __init__(self, publication, settings=None, provider=None):
        self.publication, self.store = publication, publication.store
        self.settings = settings or SummarySettings.from_env()
        self.provider = provider or OpenAISummaryProvider(self.settings)

    def request(self, request):
        # This immutable read never takes the replay/feed publication lock.
        report = self.publication.analysis(request.analysis_id)
        facts = extract_facts(report)
        key = hashlib.sha256(f"{report.analysis_id}:{request.language}:{self.settings.fingerprint}".encode()).hexdigest()
        with self.store.connection() as db:
            # Only summary creation shares this lock; publication remains independent.
            db.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"aei-summary-queue:{self.store.schema}",))
            existing = db.execute("SELECT payload FROM summary_jobs WHERE cache_key=%s", (key,)).fetchone()
            if existing:
                return SavedSummary.model_validate(existing[0])
            reason = self.settings.unavailable_reason
            if not reason and db.execute("SELECT count(*) FROM summary_jobs WHERE status != 'ready'").fetchone()[0] >= 32:
                raise ServiceError(429, "summary_queue_full", "Summary queue is busy. Please try again shortly.")
            saved = SavedSummary(summary_id=str(uuid4()), analysis_id=report.analysis_id,
                run_id=report.context.run_id, state_revision=report.context.state_revision,
                method_version=report.context.method_version, as_of=report.context.filters.as_of,
                status="queued", created_at=datetime.now(timezone.utc), provider=self.settings.provider,
                model=self.settings.model)
            if reason:
                saved = complete_summary(saved, facts, fallback_reason=reason)
            db.execute("""INSERT INTO summary_jobs
                (summary_id,cache_key,analysis_id,config_key,status,facts,payload)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""", (saved.summary_id, key, report.analysis_id,
                self.settings.fingerprint, saved.status, Jsonb([f.model_dump(mode="json") for f in facts]),
                Jsonb(saved.model_dump(mode="json"))))
            return saved

    def get(self, summary_id):
        with self.store.connection() as db:
            row = db.execute("SELECT payload FROM summary_jobs WHERE summary_id=%s", (summary_id,)).fetchone()
        if not row:
            raise ServiceError(404, "summary_not_found", "Saved summary not found")
        return SavedSummary.model_validate(row[0])

    def claim(self):
        with self.store.connection() as db:
            row = db.execute("""SELECT summary_id, payload, facts, config_key, attempts FROM summary_jobs
                WHERE (status='queued' AND available_at<=now()) OR (status='generating' AND lease_until<now())
                ORDER BY created_at FOR UPDATE SKIP LOCKED LIMIT 1""").fetchone()
            if not row:
                return None
            summary_id, payload, facts, config_key, attempts = row
            owner = str(uuid4())
            saved = SavedSummary.model_validate(payload).model_copy(update={"status": "generating"})
            db.execute("""UPDATE summary_jobs SET status='generating', owner=%s, attempts=attempts+1,
                lease_until=now()+(%s * interval '1 second'), payload=%s WHERE summary_id=%s""",
                (owner, self.settings.timeout + 60, Jsonb(saved.model_dump(mode="json")), summary_id))
            return saved, tuple(SummaryFact.model_validate(f) for f in facts), config_key, attempts + 1, owner

    def finish(self, saved, owner, *, retry=False):
        with self.store.connection() as db:
            db.execute("""UPDATE summary_jobs SET status=%s, payload=%s, owner=NULL, lease_until=NULL,
                available_at=now()+(%s * interval '1 second')
                WHERE summary_id=%s AND owner=%s AND status='generating'""",
                (saved.status, Jsonb(saved.model_dump(mode="json")), 2 if retry else 0, saved.summary_id, owner))

    async def worker_tick(self):
        claimed = await asyncio.to_thread(self.claim)
        if claimed is None:
            return
        saved, facts, config_key, attempts, owner = claimed
        reason = None
        if config_key != self.settings.fingerprint:
            reason = "configuration_changed"
        elif attempts > 2:
            reason = "interrupted"
        else:
            try:
                draft = await asyncio.wait_for(self.provider.generate(facts), timeout=self.settings.timeout)
                result = complete_summary(saved, facts, draft=draft)
            except (ProviderUnavailable, asyncio.TimeoutError) as error:
                if attempts < 2 and (isinstance(error, asyncio.TimeoutError) or error.retryable):
                    await asyncio.to_thread(self.finish, saved.model_copy(update={"status": "queued"}), owner, retry=True)
                    return
                reason = "provider_unavailable"
            except Exception:
                # Never persist/log raw provider text, errors, request headers or keys.
                reason = "invalid_output"
        if reason:
            result = complete_summary(saved, facts, fallback_reason=reason)
        await asyncio.to_thread(self.finish, result, owner)

    async def run(self, stopped):
        while not stopped.is_set():
            try:
                await self.worker_tick()
            except Exception:
                logging.getLogger(__name__).warning("Summary worker could not complete a job; it remains recoverable")
            try:
                await asyncio.wait_for(stopped.wait(), timeout=0.5)
            except asyncio.TimeoutError:
                pass
