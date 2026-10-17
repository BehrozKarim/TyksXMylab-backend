"""Durable acceptance, bounded background publication and retry-safe complete bundles."""

import hashlib
import logging
from datetime import datetime, timezone
from uuid import uuid4

from psycopg import sql
from psycopg.types.json import Jsonb

from .feed_models import FeedBatch, FeedReceipt
from .models import AnalysisQuery
from .publication_models import ServiceError
from .store import EventConflict


logger = logging.getLogger(__name__)


def utc_today():
    return datetime.now(timezone.utc).date()


RECEIPT_SELECT = """SELECT i.receipt_id, i.run_id, i.source_id, i.delivery_id, i.payload_sha256,
    j.status, i.accepted_as_of, i.accepted_at, j.completed_at, j.published_revision,
    j.error_code, j.error_message FROM feed_inbox i JOIN feed_jobs j USING (receipt_id) """


def receipt_from_row(row):
    names = ("receipt_id", "run_id", "source_id", "delivery_id", "payload_sha256", "status",
             "accepted_as_of", "accepted_at", "completed_at", "published_revision", "error_code", "error_message")
    return FeedReceipt(**dict(zip(names, row)), receipt_url=f"/api/v1/feed/receipts/{row[0]}")


class FeedService:
    def __init__(self, publication):
        self.publication = publication
        self.store = publication.store

    def start(self, request):
        with self.publication.locked() as db:
            previous = self.publication._active(db)
            self.publication._check(previous, request)
            today = utc_today()
            as_of = request.as_of or today
            if request.clock_mode == "wall_utc" and as_of != today:
                raise ServiceError(422, "invalid_feed_clock", "Wall-clock feeds must start at today's UTC date")
            seed = request.seed_through or min(previous.demo_date, as_of)
            if seed > as_of:
                raise ServiceError(422, "invalid_feed_seed", "seed_through cannot exceed as_of")
            AnalysisQuery(as_of=seed, period="rolling_12_months")
            manifest, kind = self.publication._complete_source(db)
            if previous.status in ("running", "receiving"):
                self.publication._save(db, previous.model_copy(update={"status": "paused",
                    "state_revision": previous.state_revision + 1, "updated_at": datetime.now(timezone.utc)}))
            return self.publication._create_run(db, manifest, kind, seed, as_of, mode="feed",
                clock_mode=request.clock_mode, feed_source_id=request.source_id, feed_source_label=request.source_label)

    def _target(self, state, run_id, source_id):
        if state is None or state.run_id != run_id:
            raise ServiceError(409, "inactive_feed_run", "This delivery belongs to an inactive run")
        if state.mode != "feed" or state.status != "receiving" or state.feed_source_id != source_id:
            raise ServiceError(409, "feed_not_receiving", "The active run is not receiving from this feed source")

    def submit(self, batch: FeedBatch):
        # No publication lock: accepting a durable job must not wait for analyses.
        if len(batch.canonical_payload().encode()) > 5_000_000:
            raise ServiceError(413, "feed_batch_too_large", "Limit each delivery to 5 MB")
        with self.store.connection() as db:
            existing = db.execute(RECEIPT_SELECT + "WHERE i.run_id=%s AND i.source_id=%s AND i.delivery_id=%s",
                                  (batch.run_id, batch.source_id, batch.delivery_id)).fetchone()
            if existing:
                if existing[4] != batch.payload_hash:
                    raise ServiceError(409, "delivery_conflict", "This delivery ID already has different content")
                return receipt_from_row(existing)
            state = self.publication._active(db, initialize=False)
            self._target(state, batch.run_id, batch.source_id)
            as_of = batch.as_of or (utc_today() if state.clock_mode == "wall_utc" else state.demo_date)
            if state.clock_mode == "wall_utc" and as_of > utc_today():
                raise ServiceError(422, "future_feed_clock", "A wall-clock delivery cannot advance into the future")
            for bundle in batch.bundles:
                if bundle.culture.sample_date > as_of or any(
                        item.event.received_at.astimezone(timezone.utc).date() > as_of for item in bundle.isolates):
                    raise ServiceError(422, "future_feed_sample", "Collection and result receipt dates must be on or before as_of")
            identity = str(uuid4())
            inserted = db.execute("""INSERT INTO feed_inbox
                (receipt_id, run_id, source_id, delivery_id, payload_sha256, accepted_as_of, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING receipt_id""",
                (identity, batch.run_id, batch.source_id, batch.delivery_id, batch.payload_hash,
                 as_of, Jsonb(batch.model_dump(mode="json", by_alias=True)))).fetchone()
            if inserted:
                db.execute("INSERT INTO feed_jobs (receipt_id, status) VALUES (%s, 'pending')", (identity,))
            row = db.execute(RECEIPT_SELECT + "WHERE i.run_id=%s AND i.source_id=%s AND i.delivery_id=%s",
                             (batch.run_id, batch.source_id, batch.delivery_id)).fetchone()
            if row[4] != batch.payload_hash:
                raise ServiceError(409, "delivery_conflict", "This delivery ID already has different content")
            return receipt_from_row(row)

    def receipt(self, identity):
        with self.store.connection() as db:
            row = db.execute(RECEIPT_SELECT + "WHERE i.receipt_id=%s", (identity,)).fetchone()
            if row is None:
                raise ServiceError(404, "receipt_not_found", "Feed receipt not found")
            return receipt_from_row(row)

    def retry(self, identity):
        with self.publication.locked() as db:
            row = db.execute(RECEIPT_SELECT + "WHERE i.receipt_id=%s FOR UPDATE OF j", (identity,)).fetchone()
            if row is None:
                raise ServiceError(404, "receipt_not_found", "Feed receipt not found")
            self._target(self.publication._active(db, initialize=False), row[1], row[2])
            if row[5] == "failed":
                db.execute("""UPDATE feed_jobs SET status='pending', completed_at=NULL,
                    error_code=NULL, error_message=NULL WHERE receipt_id=%s""", (identity,))
            elif row[5] not in ("pending", "published"):
                raise ServiceError(409, "receipt_not_retryable", "This receipt cannot be retried")
        return self.receipt(identity)

    def worker_tick(self):
        attempted = None
        try:
            # A cheap observation avoids competing with ordinary replay while idle.
            with self.store.connection() as db:
                pending = db.execute("SELECT 1 FROM feed_jobs WHERE status='pending' LIMIT 1").fetchone()
                state = self.publication._active(db, initialize=False)
                clock_due = (state and state.mode == "feed" and state.status == "receiving"
                             and state.clock_mode == "wall_utc" and state.demo_date < utc_today())
            if not pending and not clock_due:
                return
            with self.publication.locked(wait=False) as db:
                state = self.publication._active(db, initialize=False)
                row = db.execute("""SELECT i.receipt_id, i.payload, i.accepted_as_of FROM feed_inbox i
                    JOIN feed_jobs j USING (receipt_id) WHERE j.status='pending'
                    ORDER BY i.sequence LIMIT 1 FOR UPDATE OF j SKIP LOCKED""").fetchone()
                if row:
                    attempted = row[0]
                    batch = FeedBatch.model_validate(row[1])
                    if state is None or state.run_id != batch.run_id:
                        db.execute("""UPDATE feed_jobs SET status='cancelled', completed_at=CURRENT_TIMESTAMP,
                            error_code='inactive_feed_run', error_message='The run was replaced before publication'
                            WHERE receipt_id=%s""", (attempted,))
                        return
                    self._target(state, batch.run_id, batch.source_id)
                    as_of = max(row[2], utc_today()) if state.clock_mode == "wall_utc" else row[2]
                    state = self._publish(db, state, batch, as_of)
                    db.execute("""UPDATE feed_jobs SET status='published', completed_at=CURRENT_TIMESTAMP,
                        published_revision=%s WHERE receipt_id=%s""", (state.state_revision, attempted))
                elif state and state.mode == "feed" and state.status == "receiving" and state.clock_mode == "wall_utc":
                    self._publish(db, state, None, utc_today())
        except ServiceError as error:
            if error.code == "publication_busy":
                return
            self._failed(attempted, error.code, str(error))
        except EventConflict:
            self._failed(attempted, "source_conflict", "Existing source identity has different content")
        except Exception:
            logger.exception("Feed publication failed")
            self._failed(attempted, "publication_failed", "Publication rolled back; inspect server logs before retrying")

    def _failed(self, identity, code, message):
        if identity is not None:
            with self.store.connection() as db:
                # A different worker may have published after our transaction rolled
                # back. Never overwrite a successful or cancelled outcome.
                db.execute("""UPDATE feed_jobs SET status='failed', completed_at=CURRENT_TIMESTAMP,
                    error_code=%s, error_message=%s WHERE receipt_id=%s AND status='pending'""", (code, message, identity))

    def _publish(self, db, state, batch, as_of):
        self.publication.alert_engine.ensure(db, state)
        revision = state.state_revision + 1
        cultures = isolates = 0
        if batch:
            for bundle in batch.bundles:
                self._validate_revision(db, state, bundle)
                self._insert_metadata(db, state, bundle)
            self.store.append_many_in_transaction(db, [item.event for bundle in batch.bundles for item in bundle.isolates])
            for bundle in batch.bundles:
                self._insert_enrichments(db, state, bundle)
                cultures += db.execute("""INSERT INTO run_cultures VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING""", (state.run_id, bundle.culture_event_id, revision)).rowcount
                for item in bundle.isolates:
                    isolates += db.execute("""INSERT INTO run_isolates VALUES (%s, %s, %s)
                        ON CONFLICT DO NOTHING""", (state.run_id, item.event.event_id, revision)).rowcount
        through = max(as_of, state.demo_date)
        if not cultures and not isolates and through == state.demo_date:
            return state  # Re-delivery under another ID does not retrigger alerts.
        latest = db.execute("""SELECT MAX(c.sample_date) FROM culture_events c
            JOIN run_cultures r USING(event_id) WHERE r.run_id=%s""", (state.run_id,)).fetchone()[0]
        digest = state.feed_data_digest
        if batch and (cultures or isolates):
            digest = hashlib.sha256(((digest or "") + batch.payload_hash).encode()).hexdigest()
        state = state.model_copy(update={"state_revision": revision, "demo_date": through,
            "published_cultures": state.published_cultures + cultures,
            "published_isolates": state.published_isolates + isolates,
            "latest_visible_sample_date": latest, "feed_data_digest": digest,
            "updated_at": datetime.now(timezone.utc)})
        self.publication._save(db, state)
        self.publication.alert_engine.evaluate(db, state)
        return state

    def _validate_revision(self, db, state, bundle):
        # Corrections replace the full culture/AST snapshot. Retractions and changes
        # to the isolate set need tombstone semantics and are explicitly rejected.
        previous = db.execute("""SELECT c.revision, c.event_id FROM culture_events c
            JOIN run_cultures r USING(event_id) WHERE r.run_id=%s AND c.culture_id=%s
            ORDER BY c.revision DESC LIMIT 1""", (state.run_id, bundle.culture.culture_id)).fetchone()
        if previous:
            if bundle.culture_revision < previous[0] or bundle.culture_revision > previous[0] + 1:
                raise ServiceError(409, "invalid_culture_revision", "Use the current revision for an exact retry, or the next revision")
            if bundle.culture_revision == previous[0] and bundle.culture_event_id != previous[1]:
                raise ServiceError(409, "source_conflict", "A culture revision already has a different event ID")
            old_ids = {row[0] for row in db.execute("""SELECT e.isolate_id FROM isolate_events e
                JOIN isolate_enrichments m USING(event_id) WHERE m.culture_event_id=%s""", (previous[1],))}
            if old_ids != {item.event.isolate_id for item in bundle.isolates}:
                raise ServiceError(422, "unsupported_isolate_set_change", "Corrections must retain the same isolate IDs; retractions are not supported")
        elif bundle.culture_revision != 1:
            raise ServiceError(409, "missing_culture_revision", "Publish revision 1 before its corrections")
        for item in bundle.isolates:
            row = db.execute("SELECT payload->>'culture_id' FROM isolate_events WHERE isolate_id=%s LIMIT 1",
                             (item.event.isolate_id,)).fetchone()
            if row and row[0] != bundle.culture.culture_id:
                raise ServiceError(409, "isolate_culture_conflict", "An isolate ID cannot move between cultures")

    def _insert_metadata(self, db, state, bundle):
        for table, key, records in (("patients", "patient_id", (bundle.patient,)),
                                    ("wards", "ward_code", (bundle.ward,)),
                                    ("antibiotics", "antibiotic_code", bundle.antibiotics)):
            for record in records:
                payload = record.model_dump(mode="json", by_alias=True)
                if table == "antibiotics":
                    payload["is_screening"] = record.is_screening
                identity = getattr(record, key)
                db.execute(sql.SQL("INSERT INTO {} ({}, manifest_id, payload) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING")
                           .format(sql.Identifier(table), sql.Identifier(key)),
                           (identity, state.source_manifest_id, Jsonb(payload)))
                existing = db.execute(sql.SQL("SELECT payload FROM {} WHERE {}=%s")
                                      .format(sql.Identifier(table), sql.Identifier(key)), (identity,)).fetchone()
                if existing is None or existing[0] != payload:
                    raise EventConflict("Dictionary record conflicts with existing immutable metadata")
        culture = bundle.culture
        payload = culture.model_dump(mode="json")
        db.execute("""INSERT INTO culture_events
            (event_id, culture_id, revision, patient_id, ward_code, sample_date, manifest_id, payload, origin)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'feed') ON CONFLICT DO NOTHING""",
            (bundle.culture_event_id, culture.culture_id, bundle.culture_revision, culture.patient_id,
             culture.ward_code, culture.sample_date, state.source_manifest_id, Jsonb(payload)))
        existing = db.execute("SELECT payload, revision FROM culture_events WHERE event_id=%s", (bundle.culture_event_id,)).fetchone()
        if existing != (payload, bundle.culture_revision):
            raise EventConflict("Culture event conflicts with existing history")

    def _insert_enrichments(self, db, state, bundle):
        for item in bundle.isolates:
            payload = item.enrichment.model_dump(mode="json")
            db.execute("""INSERT INTO isolate_enrichments (event_id, culture_event_id, manifest_id, payload, origin)
                VALUES (%s, %s, %s, %s, 'feed') ON CONFLICT DO NOTHING""",
                (item.event.event_id, bundle.culture_event_id, state.source_manifest_id, Jsonb(payload)))
            existing = db.execute("SELECT payload FROM isolate_enrichments WHERE event_id=%s", (item.event.event_id,)).fetchone()
            if existing is None or existing[0] != payload:
                raise EventConflict("Enrichment conflicts with an existing exact revision")
