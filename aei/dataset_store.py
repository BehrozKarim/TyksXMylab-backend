"""Atomic PostgreSQL import of validated cultures, events and source metadata."""

from datetime import date

from psycopg import sql
from psycopg.types.json import Jsonb

from .dataset_models import DatasetBundle
from .publication_store import publication_lock
from .store import EventConflict


SOURCE_TABLES = ("dataset_manifests", "patients", "wards", "antibiotics",
                 "culture_events", "isolate_enrichments")


def initialize_dataset_tables(db):
    # One source per schema avoids silent collisions between independently authored datasets.
    db.execute("""CREATE TABLE IF NOT EXISTS dataset_manifests (
        manifest_id TEXT PRIMARY KEY,
        singleton BOOLEAN NOT NULL DEFAULT TRUE UNIQUE CHECK (singleton),
        source_kind TEXT NOT NULL CHECK (source_kind IN ('zip', 'directory')),
        imported_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
        payload JSONB NOT NULL
    )""")
    for table, key in (("patients", "patient_id"), ("wards", "ward_code"),
                       ("antibiotics", "antibiotic_code")):
        db.execute(sql.SQL("""CREATE TABLE IF NOT EXISTS {} (
            {} TEXT PRIMARY KEY, manifest_id TEXT NOT NULL REFERENCES dataset_manifests,
            payload JSONB NOT NULL
        )""").format(sql.Identifier(table), sql.Identifier(key)))
    db.execute("""CREATE TABLE IF NOT EXISTS culture_events (
        event_id TEXT PRIMARY KEY,
        culture_id TEXT NOT NULL,
        revision INTEGER NOT NULL CHECK (revision >= 1),
        patient_id TEXT NOT NULL REFERENCES patients,
        ward_code TEXT NOT NULL REFERENCES wards,
        sample_date DATE NOT NULL,
        manifest_id TEXT NOT NULL REFERENCES dataset_manifests,
        payload JSONB NOT NULL,
        UNIQUE (culture_id, revision)
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS culture_patient_date_idx ON culture_events (patient_id, sample_date, culture_id)")
    db.execute("CREATE INDEX IF NOT EXISTS culture_date_ward_idx ON culture_events (sample_date, ward_code)")
    db.execute("""CREATE TABLE IF NOT EXISTS isolate_enrichments (
        event_id TEXT PRIMARY KEY REFERENCES isolate_events,
        culture_event_id TEXT NOT NULL REFERENCES culture_events,
        manifest_id TEXT NOT NULL REFERENCES dataset_manifests,
        payload JSONB NOT NULL
    )""")
    db.execute("CREATE INDEX IF NOT EXISTS enrichment_culture_idx ON isolate_enrichments (culture_event_id)")
    for table in SOURCE_TABLES:
        db.execute(sql.SQL("""CREATE OR REPLACE TRIGGER forbid_source_mutation
            BEFORE UPDATE OR DELETE OR TRUNCATE ON {}
            FOR EACH STATEMENT EXECUTE FUNCTION reject_event_mutation()""").format(sql.Identifier(table)))


def _append_records(db, table, columns, key, rows):
    """Internal bulk writer: all identifiers are fixed by this module."""
    target, staging = sql.Identifier(table), sql.Identifier("incoming_" + table)
    fields = sql.SQL(", ").join(map(sql.Identifier, columns))
    db.execute(sql.SQL("CREATE TEMP TABLE {} (LIKE {}) ON COMMIT DROP").format(staging, target))
    submitted = 0
    with db.cursor().copy(sql.SQL("COPY {} ({}) FROM STDIN").format(staging, fields)) as copy:
        for row in rows:
            copy.write_row(row)
            submitted += 1
    inserted = db.execute(sql.SQL("""INSERT INTO {} ({})
        SELECT {} FROM {} ORDER BY {} ON CONFLICT DO NOTHING""").format(
            target, fields, fields, staging, sql.Identifier(key))).rowcount
    conflict = db.execute(sql.SQL("""SELECT 1 FROM {} incoming
        LEFT JOIN {} stored USING ({})
        WHERE stored.{} IS NULL OR stored.payload IS DISTINCT FROM incoming.payload
           OR stored.manifest_id IS DISTINCT FROM incoming.manifest_id LIMIT 1""").format(
            staging, target, sql.Identifier(key), sql.Identifier(key))).fetchone()
    if conflict:
        raise EventConflict(f"Existing {table} record has different source data")
    return {"inserted": inserted, "duplicates": submitted - inserted}


def import_bundle(store, bundle: DatasetBundle, through: date | None = None) -> dict:
    if not bundle.report.valid or bundle.report.manifest is None:
        raise ValueError("Only a validated dataset bundle can be imported")
    manifest = bundle.report.manifest
    manifest_id = manifest.manifest_id
    payload = manifest.model_dump(mode="json")
    payload["warnings"] = [warning.model_dump(mode="json") for warning in bundle.report.warnings]
    cultures = [culture for culture in bundle.cultures if through is None or culture.sample_date <= through]
    culture_ids = {culture.culture_id for culture in cultures}
    events = [event for event in bundle.events if event.culture_id in culture_ids]
    event_ids = {event.event_id for event in events}
    counts = {}
    with store.connection() as db:
        db.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"aei-import:{store.schema}",))
        publication_lock(db, store.schema)
        inserted = db.execute("""INSERT INTO dataset_manifests (manifest_id, source_kind, payload)
            VALUES (%s, %s, %s) ON CONFLICT DO NOTHING""",
            (manifest_id, bundle.report.source_kind, Jsonb(payload))).rowcount
        existing = db.execute("SELECT manifest_id, payload FROM dataset_manifests").fetchone()
        if existing != (manifest_id, payload):
            raise EventConflict("This schema belongs to a different source manifest; use a separate --schema")
        counts["dataset_manifests"] = {"inserted": inserted, "duplicates": 1 - inserted}
        for table, key, records in (("patients", "patient_id", bundle.patients),
                                    ("wards", "ward_code", bundle.wards),
                                    ("antibiotics", "antibiotic_code", bundle.antibiotics)):
            def rows():
                for record in records:
                    item = record.model_dump(mode="json", by_alias=True)
                    if table == "antibiotics":
                        item["is_screening"] = record.is_screening
                    yield getattr(record, key), manifest_id, Jsonb(item)
            counts[table] = _append_records(db, table, (key, "manifest_id", "payload"), key, rows())
        counts["culture_events"] = _append_records(db, "culture_events",
            ("event_id", "culture_id", "revision", "patient_id", "ward_code", "sample_date", "manifest_id", "payload"),
            "event_id", ((culture.event_id, culture.culture_id, 1, culture.patient_id, culture.ward_code,
                          culture.sample_date, manifest_id, Jsonb(culture.model_dump(mode="json")))
                         for culture in cultures))
        inserted, duplicates = store.append_many_in_transaction(db, events)
        counts["isolate_events"] = {"inserted": inserted, "duplicates": duplicates}
        counts["isolate_enrichments"] = _append_records(db, "isolate_enrichments",
            ("event_id", "culture_event_id", "manifest_id", "payload"), "event_id",
            ((item.event_id, item.culture_event_id, manifest_id, Jsonb(item.model_dump(mode="json")))
             for item in bundle.enrichments if item.event_id in event_ids))
        total_events = db.execute("SELECT COUNT(*) FROM isolate_events").fetchone()[0]
    return {"manifest_id": manifest_id, "through": through.isoformat() if through else None,
            "inserted": counts["isolate_events"]["inserted"],
            "duplicates": counts["isolate_events"]["duplicates"],
            "total_events": total_events, "tables": counts}
