"""Click-driven hosted demo using durable plans and the existing feed inbox."""

import argparse
import json
import os
import re
from datetime import datetime, timezone

from fastapi import APIRouter, Response
from pydantic import BaseModel, Field
from psycopg.types.json import Jsonb

from .feed_models import FeedBatch


SOURCE_ID = "hosted-demo"


def initialize_hosted_demo_tables(db):
    db.execute("""CREATE TABLE IF NOT EXISTS hosted_demo_batches (
        run_id TEXT NOT NULL REFERENCES replay_runs,
        ordinal INTEGER NOT NULL,
        delivery_id TEXT NOT NULL,
        payload JSONB NOT NULL,
        PRIMARY KEY (run_id, ordinal),
        UNIQUE (run_id, delivery_id)
    )""")
    db.execute("""CREATE OR REPLACE TRIGGER forbid_hosted_plan_mutation
        BEFORE UPDATE OR DELETE OR TRUNCATE ON hosted_demo_batches
        FOR EACH STATEMENT EXECUTE FUNCTION reject_event_mutation()""")


class HostedDemo:
    def __init__(self, publication):
        self.publication, self.store = publication, publication.store

    def prepare(self, dataset, *, days=7, batch_size=20, restart=False):
        from .demo import demo_plan
        from .feed_sender import batches
        from datetime import date, timedelta

        if not 1 <= days <= 366 or not 1 <= batch_size <= 100:
            raise ValueError("Use 1-366 days and a batch size of 1-100")
        plan = demo_plan(dataset, days, batch_size)
        with self.publication.locked() as db:
            manifest, kind = self.publication._complete_source(db)
            if manifest["manifest_id"] != plan["manifest_id"]:
                raise ValueError("The demo plan must use the imported dataset")
            previous = self.publication._active(db)
            existing = db.execute("SELECT 1 FROM hosted_demo_batches WHERE run_id=%s LIMIT 1",
                                  (previous.run_id,)).fetchone()
            if existing and not restart:
                return previous  # Re-running setup never discards progress.
            if previous.status in ("running", "receiving"):
                if not restart:
                    raise ValueError("A different run is active; use --restart explicitly to replace it")
                self.publication._save(db, previous.model_copy(update={"status": "paused",
                    "state_revision": previous.state_revision + 1, "updated_at": datetime.now(timezone.utc)}))
            seed = date.fromisoformat(plan["seed_through"])
            state = self.publication._create_run(db, manifest, kind, seed, seed, mode="feed",
                clock_mode="simulated", feed_source_id=SOURCE_ID, feed_source_label="Hosted demo feed")
            for index, batch in enumerate(batches(dataset, state.run_id, SOURCE_ID,
                    seed + timedelta(days=1), date.fromisoformat(plan["through"]), batch_size)):
                db.execute("INSERT INTO hosted_demo_batches VALUES (%s,%s,%s,%s)",
                    (state.run_id, index, batch.delivery_id, Jsonb(batch.model_dump(mode="json", by_alias=True))))
            return state

    def _status(self, db):
        state = self.publication._active(db, initialize=False)
        value = dict(phase="error", run_id=state.run_id if state else "", next_batch=None,
                     published_batches=0, skipped_batches=0, last_publication=None,
                     error="Initialize the hosted demo before sending batches.")
        if not state or state.feed_source_id != SOURCE_ID or state.status != "receiving":
            return value
        rows = db.execute("""SELECT b.delivery_id, b.payload->>'as_of',
            jsonb_array_length(b.payload->'bundles'), j.status, j.published_revision
            FROM hosted_demo_batches b LEFT JOIN feed_inbox i ON i.run_id=b.run_id
                AND i.source_id=%s AND i.delivery_id=b.delivery_id
            LEFT JOIN feed_jobs j USING (receipt_id)
            WHERE b.run_id=%s ORDER BY b.ordinal""", (SOURCE_ID, state.run_id)).fetchall()
        if not rows:
            return value
        value.update(phase="complete", error=None)
        next_row = None
        for delivery, as_of, count, status, revision in rows:
            if status == "published":
                value["published_batches"] += 1
                value["last_publication"] = dict(cultures=count, as_of=as_of, revision=revision)
            elif next_row is None:
                next_row = (delivery, as_of, count, status)
        if next_row:
            delivery, as_of, count, status = next_row
            value.update(next_batch=dict(delivery_id=delivery, as_of=as_of, cultures=count),
                         phase="ready" if status is None else "processing" if status == "pending" else "error")
            if value["phase"] == "error":
                value["error"] = "Batch publication failed. Inspect and retry its feed receipt before continuing."
        return value

    def status(self):
        with self.store.connection() as db:
            db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
            return self._status(db)

    def next(self, delivery_id):
        # Serialize clicks across processes. Feed submission commits before releasing
        # this lock, so another click sees the durable receipt even after a restart.
        with self.store.connection() as db:
            db.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"aei-hosted-click:{self.store.schema}",))
            value = self._status(db)
            if (value["phase"] != "ready" or value["next_batch"]["delivery_id"] != delivery_id):
                return False, value
            row = db.execute("SELECT payload FROM hosted_demo_batches WHERE run_id=%s AND delivery_id=%s",
                             (value["run_id"], delivery_id)).fetchone()
            self.publication.feed.submit(FeedBatch.model_validate(row[0]))
            return True, self._status(db)


def install_hosted_demo(app, publication):
    if os.environ.get("AEI_HOSTED_DEMO") != "1":
        return
    if publication is None:
        raise ValueError("Hosted demo requires PostgreSQL")
    service = HostedDemo(publication)
    router = APIRouter(prefix="/api/v1/demo")

    class NextBatch(BaseModel):
        delivery_id: str = Field(min_length=1, max_length=150)

    @router.get("/status")
    def status(response: Response):
        response.headers["Cache-Control"] = "no-store"
        return service.status()

    @router.post("/next")
    def next_batch(request: NextBatch, response: Response):
        accepted, value = service.next(request.delivery_id)
        response.headers["Cache-Control"] = "no-store"
        response.status_code = 202 if accepted else 409
        return value

    app.include_router(router)


def main():
    from .config import database_schema, database_url
    from .dataset import load_bundle
    from .postgres import PostgresEventStore
    from .publication import PublicationService

    parser = argparse.ArgumentParser(description="Import and prepare a persistent hosted demo")
    parser.add_argument("--source", required=True, help="Local ZIP or directory; never uploaded to Git")
    parser.add_argument("--days", type=int, default=7, choices=range(1, 367), metavar="1-366")
    parser.add_argument("--batch-size", type=int, default=20, choices=range(1, 101), metavar="1-100")
    parser.add_argument("--restart", action="store_true", help="Explicitly start a new run while preserving old history")
    args = parser.parse_args()
    schema = database_schema()
    if not os.environ.get("AEI_DATABASE_URL") or not re.fullmatch(r"aei_demo_[a-z0-9_]{1,54}", schema):
        parser.error("Explicitly set AEI_DATABASE_URL and an isolated AEI_DATABASE_SCHEMA starting with aei_demo_")
    dataset = load_bundle(args.source)
    store = PostgresEventStore(database_url(), schema)
    store.import_dataset(dataset)
    state = HostedDemo(PublicationService(store)).prepare(dataset, days=args.days,
        batch_size=args.batch_size, restart=args.restart)
    print(json.dumps({"run_id": state.run_id, "as_of": state.demo_date.isoformat(), "status": state.status}))


if __name__ == "__main__":
    main()
