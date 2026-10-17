"""Read one consistent PostgreSQL snapshot for both evidence windows."""

from dataclasses import dataclass, field
from datetime import date

from .dataset_models import AntibioticRecord, DatasetManifest, IsolateEnrichment
from .models import IsolateEvent


@dataclass(frozen=True)
class EvidenceSnapshot:
    events: tuple[IsolateEvent, ...]
    enrichments: dict[str, IsolateEnrichment]
    antibiotics: dict[str, AntibioticRecord]
    ward_codes: frozenset[str]
    manifest: DatasetManifest
    imported_cultures: int
    imported_culture_start: date | None
    imported_culture_end: date | None
    imported_enrichments: int | None = None
    ward_types: dict[str, str] = field(default_factory=dict)


class EvidenceUnavailable(ValueError):
    pass


def read_evidence_snapshot(store) -> EvidenceSnapshot:
    with store.connection() as db:
        db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        return snapshot_from_connection(db)


def snapshot_from_connection(db, run_id: str | None = None) -> EvidenceSnapshot:
    """Caller holds a repeatable-read snapshot or the publication/import lock."""
    stored = db.execute("SELECT payload FROM dataset_manifests").fetchone()
    if stored is None:
        raise EvidenceUnavailable("Import a validated dataset before requesting enriched evidence")
    manifest = DatasetManifest.model_validate({key: stored[0][key] for key in DatasetManifest.model_fields})
    if run_id is None:
        event_rows = db.execute("SELECT payload FROM isolate_events ORDER BY sequence").fetchall()
        metadata_rows = db.execute("SELECT payload FROM isolate_enrichments").fetchall()
    else:
        event_rows = db.execute("""SELECT e.payload FROM isolate_events e
            JOIN run_isolates r USING (event_id) WHERE r.run_id=%s ORDER BY e.sequence""", (run_id,)).fetchall()
        metadata_rows = db.execute("""SELECT e.payload FROM isolate_enrichments e
            JOIN run_isolates r USING (event_id) WHERE r.run_id=%s""", (run_id,)).fetchall()
    events = tuple(IsolateEvent.model_validate(row[0]) for row in event_rows)
    metadata = [IsolateEnrichment.model_validate(row[0]) for row in metadata_rows]
    antibiotics = [AntibioticRecord.model_validate({key: value for key, value in row[0].items()
                                                   if key != "is_screening"}) for row in
                   db.execute("SELECT payload FROM antibiotics").fetchall()]
    ward_types = {row[0]: row[1]["ward_type"] for row in db.execute("SELECT ward_code, payload FROM wards")}
    wards = frozenset(ward_types)
    count, start, end = db.execute("SELECT COUNT(*), MIN(sample_date), MAX(sample_date) FROM culture_events").fetchone()
    enriched = db.execute("SELECT COUNT(*) FROM isolate_enrichments").fetchone()[0]
    return EvidenceSnapshot(events=events, enrichments={item.event_id: item for item in metadata},
        antibiotics={item.antibiotic_code: item for item in antibiotics}, ward_codes=wards,
        manifest=manifest, imported_cultures=count, imported_culture_start=start, imported_culture_end=end,
        imported_enrichments=enriched, ward_types=ward_types)
