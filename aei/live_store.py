"""Durable feed inbox and committed dashboard notifications."""

from psycopg.types.json import Jsonb


def initialize_live_tables(db):
    # New records are distinguished from the immutable imported dataset. DDL
    # defaults classify existing rows without rewriting their JSON payloads.
    for table in ("culture_events", "isolate_enrichments"):
        db.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS origin TEXT NOT NULL DEFAULT 'dataset'")
    db.execute("""CREATE TABLE IF NOT EXISTS feed_inbox (
        sequence BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
        receipt_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES replay_runs,
        source_id TEXT NOT NULL,
        delivery_id TEXT NOT NULL,
        payload_sha256 TEXT NOT NULL,
        accepted_as_of DATE NOT NULL,
        accepted_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        payload JSONB NOT NULL,
        UNIQUE (run_id, source_id, delivery_id)
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS feed_jobs (
        receipt_id TEXT PRIMARY KEY REFERENCES feed_inbox,
        status TEXT NOT NULL CHECK (status IN ('pending', 'published', 'failed', 'cancelled')),
        completed_at TIMESTAMPTZ,
        published_revision BIGINT,
        error_code TEXT,
        error_message TEXT
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS feed_pending_idx ON feed_jobs (status, receipt_id)")
    db.execute("""CREATE TABLE IF NOT EXISTS publication_notifications (
        notification_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES replay_runs,
        state_revision BIGINT NOT NULL,
        payload JSONB NOT NULL,
        UNIQUE (run_id, state_revision)
    )""")
    for table in ("feed_inbox", "publication_notifications"):
        db.execute(f"""CREATE OR REPLACE TRIGGER forbid_live_history_mutation
            BEFORE UPDATE OR DELETE OR TRUNCATE ON {table}
            FOR EACH STATEMENT EXECUTE FUNCTION reject_event_mutation()""")


def record_notification(db, state):
    # Writers hold the publication lock, so notification IDs follow commit order.
    db.execute("""INSERT INTO publication_notifications (run_id, state_revision, payload)
        VALUES (%s, %s, %s) ON CONFLICT (run_id, state_revision) DO NOTHING""",
        (state.run_id, state.state_revision, Jsonb(state.model_dump(mode="json"))))
