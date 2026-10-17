"""Additive summary persistence alongside immutable saved analyses."""


def initialize_summary_tables(db):
    db.execute("""CREATE TABLE IF NOT EXISTS summary_jobs (
        summary_id TEXT PRIMARY KEY, cache_key TEXT NOT NULL UNIQUE,
        analysis_id TEXT NOT NULL REFERENCES saved_analyses(analysis_id),
        config_key TEXT NOT NULL, status TEXT NOT NULL CHECK (status IN ('queued','generating','ready')),
        facts JSONB NOT NULL, payload JSONB NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
        owner TEXT, available_at TIMESTAMPTZ NOT NULL DEFAULT now(), lease_until TIMESTAMPTZ,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS summary_pending_idx ON summary_jobs(status, available_at)")
