"""Run membership and frozen analyses alongside immutable source history."""

from psycopg import sql


def publication_lock(db, schema):
    db.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"aei-publication:{schema}",))


def initialize_publication_tables(db):
    db.execute("""CREATE TABLE IF NOT EXISTS replay_runs (
        run_id TEXT PRIMARY KEY,
        manifest_id TEXT NOT NULL REFERENCES dataset_manifests,
        payload JSONB NOT NULL,
        next_tick_at TIMESTAMPTZ
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS active_run (
        singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
        run_id TEXT NOT NULL REFERENCES replay_runs
    )""")
    for table, source in (("run_cultures", "culture_events"), ("run_isolates", "isolate_events")):
        db.execute(sql.SQL("""CREATE TABLE IF NOT EXISTS {} (
            run_id TEXT NOT NULL REFERENCES replay_runs,
            event_id TEXT NOT NULL REFERENCES {},
            published_revision BIGINT NOT NULL CHECK (published_revision >= 1),
            PRIMARY KEY (run_id, event_id)
        )""").format(sql.Identifier(table), sql.Identifier(source)))
    db.execute("""CREATE INDEX IF NOT EXISTS run_culture_revision_idx
        ON run_cultures (run_id, published_revision, event_id)""")
    db.execute("""CREATE TABLE IF NOT EXISTS saved_analyses (
        analysis_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES replay_runs,
        state_revision BIGINT NOT NULL CHECK (state_revision >= 1),
        method_version TEXT NOT NULL,
        kind TEXT NOT NULL,
        query_key TEXT NOT NULL,
        payload JSONB NOT NULL,
        UNIQUE (run_id, state_revision, method_version, kind, query_key)
    )""")
    for table in ("run_cultures", "run_isolates", "saved_analyses"):
        db.execute(sql.SQL("""CREATE OR REPLACE TRIGGER forbid_publication_mutation
            BEFORE UPDATE OR DELETE OR TRUNCATE ON {}
            FOR EACH STATEMENT EXECUTE FUNCTION reject_event_mutation()""").format(sql.Identifier(table)))
