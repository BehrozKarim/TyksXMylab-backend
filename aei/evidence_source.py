"""Read one consistent PostgreSQL snapshot for both evidence windows."""

from dataclasses import dataclass
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


class EvidenceUnavailable(ValueError):
    pass


def read_evidence_snapshot(store) -> EvidenceSnapshot:
    with store.connection() as db:
        db.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
        stored = db.execute("SELECT payload FROM dataset_manifests").fetchone()
        if stored is None:
            raise EvidenceUnavailable("Import a validated dataset before requesting enriched evidence")
        manifest = DatasetManifest.model_validate({key: stored[0][key] for key in DatasetManifest.model_fields})
        events = tuple(IsolateEvent.model_validate(row[0]) for row in
                       db.execute("SELECT payload FROM isolate_events ORDER BY sequence").fetchall())
        metadata = [IsolateEnrichment.model_validate(row[0]) for row in
                    db.execute("SELECT payload FROM isolate_enrichments").fetchall()]
        antibiotics = [AntibioticRecord.model_validate({key: value for key, value in row[0].items()
                                                       if key != "is_screening"}) for row in
                       db.execute("SELECT payload FROM antibiotics").fetchall()]
        wards = frozenset(row[0] for row in db.execute("SELECT ward_code FROM wards").fetchall())
        count, start, end = db.execute("SELECT COUNT(*), MIN(sample_date), MAX(sample_date) FROM culture_events").fetchone()
    return EvidenceSnapshot(events=events, enrichments={item.event_id: item for item in metadata},
        antibiotics={item.antibiotic_code: item for item in antibiotics}, ward_codes=wards,
        manifest=manifest, imported_cultures=count, imported_culture_start=start, imported_culture_end=end)
