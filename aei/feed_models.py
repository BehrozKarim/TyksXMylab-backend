"""Complete-culture deliveries; receiving a batch is distinct from publishing it."""

import hashlib
import json
from datetime import date, timezone
from typing import Literal

from pydantic import AwareDatetime, Field, model_validator

from .dataset_models import AntibioticRecord, CultureRecord, IsolateEnrichment, PatientRecord, WardRecord
from .models import AnalysisQuery, IsolateEvent, StrictModel
from .publication_models import ExpectedState


class FeedIsolate(StrictModel):
    event: IsolateEvent
    enrichment: IsolateEnrichment


class CultureBundle(StrictModel):
    culture_event_id: str = Field(min_length=1, max_length=250)
    culture_revision: int = Field(default=1, ge=1)
    culture: CultureRecord
    patient: PatientRecord
    ward: WardRecord
    antibiotics: tuple[AntibioticRecord, ...] = Field(default=(), max_length=100)
    isolates: tuple[FeedIsolate, ...] = Field(default=(), max_length=50)

    @model_validator(mode="after")
    def complete_and_consistent(self):
        culture = self.culture
        if culture.patient_id != self.patient.patient_id:
            raise ValueError("Culture and patient IDs differ")
        if any(getattr(culture, field) != getattr(self.ward, field) for field in WardRecord.model_fields):
            raise ValueError("Culture ward fields differ from the ward dictionary")
        if culture.age_years != culture.sample_date.year - self.patient.birth_year:
            raise ValueError("Age must match the dataset's year-based age convention")
        if len(self.isolates) != culture.n_isolates or ((culture.result == "positive") != bool(self.isolates)):
            raise ValueError("A complete culture must contain its declared isolates; negative/mixed cultures have none")
        drugs = {drug.antibiotic_code: drug for drug in self.antibiotics}
        if len(drugs) != len(self.antibiotics):
            raise ValueError("Duplicate antibiotic dictionary codes")
        identities, positions = set(), set()
        for item in self.isolates:
            event, metadata = item.event, item.enrichment
            if event.isolate_id in identities or metadata.isolate_no in positions or metadata.isolate_no < 1:
                raise ValueError("Isolate IDs and positive isolate numbers must be unique within a culture")
            identities.add(event.isolate_id)
            positions.add(metadata.isolate_no)
            if (event.revision != self.culture_revision or event.culture_id != culture.culture_id
                    or event.patient_id != culture.patient_id or event.sample_date != culture.sample_date
                    or event.ward_code != culture.ward_code or event.specimen != culture.specimen):
                raise ValueError("Isolate fields must match the complete culture revision")
            if event.received_at.astimezone(timezone.utc).date() < event.sample_date:
                raise ValueError("Isolate UTC receipt date cannot precede collection date")
            if metadata.event_id != event.event_id or metadata.culture_event_id != self.culture_event_id:
                raise ValueError("Enrichment must reference the exact isolate and culture event IDs")
            if (metadata.sex != self.patient.sex or metadata.birth_year != self.patient.birth_year
                    or metadata.home_area != self.patient.home_area or metadata.age_years != culture.age_years
                    or metadata.admission_date != culture.admission_date
                    or any(getattr(metadata, field) not in (0, 1) for field in ("esbl", "mrsa", "vre", "cpe"))):
                raise ValueError("Enrichment demographics, dates or phenotype flags are inconsistent")
            if any(test.antibiotic_code not in drugs for test in event.tests):
                raise ValueError("Supply dictionary records for every reported antibiotic")
            if any(drugs[test.antibiotic_code].reported_for not in ("all", culture.specimen) for test in event.tests):
                raise ValueError("Antibiotic dictionary specimen restriction conflicts with culture")
        return self


class FeedStart(ExpectedState):
    source_id: str = Field(min_length=1, max_length=100)
    source_label: str = Field(min_length=1, max_length=200)
    clock_mode: Literal["wall_utc", "simulated"] = "wall_utc"
    as_of: date | None = None
    seed_through: date | None = None

    @model_validator(mode="after")
    def clock(self):
        if self.clock_mode == "simulated" and self.as_of is None:
            raise ValueError("A simulated feed needs an explicit as_of date")
        if self.as_of:
            AnalysisQuery(as_of=self.as_of, period="rolling_12_months")
        return self


class FeedBatch(StrictModel):
    run_id: str = Field(min_length=1, max_length=100)
    source_id: str = Field(min_length=1, max_length=100)
    delivery_id: str = Field(min_length=1, max_length=150)
    as_of: date | None = None
    bundles: tuple[CultureBundle, ...] = Field(default=(), max_length=100)

    @model_validator(mode="after")
    def unique_cultures(self):
        if len({item.culture.culture_id for item in self.bundles}) != len(self.bundles):
            raise ValueError("Only one revision of each culture is allowed per batch")
        isolate_ids = [item.event.isolate_id for bundle in self.bundles for item in bundle.isolates]
        if len(set(isolate_ids)) != len(isolate_ids):
            raise ValueError("An isolate cannot belong to multiple cultures in a batch")
        if not self.bundles and self.as_of is None:
            raise ValueError("An empty batch must explicitly advance the monitoring date")
        if self.as_of:
            AnalysisQuery(as_of=self.as_of, period="rolling_12_months")
        return self

    def canonical_payload(self):
        payload = self.model_dump(mode="json", by_alias=True, exclude={"delivery_id"})
        for bundle in payload["bundles"]:
            bundle["antibiotics"].sort(key=lambda item: item["antibiotic_code"])
            for isolate in bundle["isolates"]:
                isolate["event"] = json.loads(IsolateEvent.model_validate(isolate["event"]).canonical_json())
            bundle["isolates"].sort(key=lambda item: item["event"]["isolate_id"])
        payload["bundles"].sort(key=lambda item: item["culture"]["culture_id"])
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    @property
    def payload_hash(self):
        return hashlib.sha256(self.canonical_payload().encode()).hexdigest()


class FeedReceipt(StrictModel):
    receipt_id: str
    run_id: str
    source_id: str
    delivery_id: str
    payload_sha256: str
    status: Literal["pending", "published", "failed", "cancelled"]
    accepted_as_of: date
    accepted_at: AwareDatetime
    completed_at: AwareDatetime | None = None
    published_revision: int | None = None
    error_code: str | None = None
    error_message: str | None = None
    receipt_url: str
