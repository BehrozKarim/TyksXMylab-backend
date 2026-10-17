"""Validated source records; these do not change the schema-v1 isolate contract."""

from datetime import date
from typing import Literal

from pydantic import Field, model_validator

from .models import IsolateEvent, StrictModel


class PatientRecord(StrictModel):
    patient_id: str = Field(min_length=1)
    sex: Literal["F", "M"]
    birth_year: int = Field(ge=1, le=9999)
    home_area: str = Field(min_length=1)


class WardRecord(StrictModel):
    ward_code: str = Field(min_length=1)
    ward_fi: str = Field(min_length=1)
    ward_en: str = Field(min_length=1)
    ward_type: Literal["in", "out"]
    institution: Literal["hospital", "primary_care"]


class AntibioticRecord(StrictModel):
    antibiotic_code: str = Field(min_length=1)
    antibiotic: str = Field(min_length=1)
    antibiotic_fi: str = Field(min_length=1)
    antibiotic_class: str = Field(min_length=1, alias="class")
    reported_for: Literal["all", "urine", "blood", "wound"]

    @property
    def is_screening(self) -> bool:
        return "screen" in self.antibiotic.lower() or "screen" in self.antibiotic_class.lower()


class CultureRecord(StrictModel):
    culture_id: str = Field(min_length=1)
    patient_id: str = Field(min_length=1)
    age_years: int = Field(ge=0)
    sample_date: date
    iso_year: int = Field(ge=1, le=9999)
    iso_week: int = Field(ge=1, le=53)
    ward_code: str = Field(min_length=1)
    ward_fi: str = Field(min_length=1)
    ward_en: str = Field(min_length=1)
    ward_type: Literal["in", "out"]
    institution: Literal["hospital", "primary_care"]
    admission_date: date | None
    specimen: Literal["urine", "blood", "wound"]
    specimen_fi: str = Field(min_length=1)
    result: Literal["positive", "negative", "mixed_growth"]
    n_isolates: int = Field(ge=0)

    @property
    def event_id(self) -> str:
        return f"synthetic:culture:{self.culture_id}:1"

    @model_validator(mode="after")
    def consistent_dates(self):
        if (self.iso_year, self.iso_week) != self.sample_date.isocalendar()[:2]:
            raise ValueError("ISO week/year does not match sample_date")
        if self.admission_date and self.admission_date > self.sample_date:
            raise ValueError("admission_date is after sample_date")
        if (self.ward_type == "in") != (self.admission_date is not None):
            raise ValueError("admission_date is required for inpatients and empty for outpatients")
        return self


class IsolateRecord(StrictModel):
    isolate_id: str = Field(min_length=1)
    culture_id: str = Field(min_length=1)
    isolate_no: int = Field(ge=1)
    organism: str = Field(min_length=1)
    quantity: str
    esbl: int = Field(ge=0, le=1)
    mrsa: int = Field(ge=0, le=1)
    vre: int = Field(ge=0, le=1)
    cpe: int = Field(ge=0, le=1)


class ASTRecord(StrictModel):
    isolate_id: str = Field(min_length=1)
    culture_id: str = Field(min_length=1)
    organism: str = Field(min_length=1)
    antibiotic: str = Field(min_length=1)
    antibiotic_code: str = Field(min_length=1)
    antibiotic_fi: str = Field(min_length=1)
    result: Literal["S", "R"]
    guideline: str = Field(min_length=1)


class IsolateEnrichment(StrictModel):
    event_id: str
    culture_event_id: str
    sex: Literal["F", "M"]
    birth_year: int
    home_area: str
    age_years: int
    admission_date: date | None
    isolate_no: int
    quantity: str
    esbl: int
    mrsa: int
    vre: int
    cpe: int


class ImportIssue(StrictModel):
    code: str
    file: str | None = None
    row: int | None = None
    record_id: str | None = None
    message: str


class FileSummary(StrictModel):
    sha256: str
    rows: int


class ObservedPanel(StrictModel):
    organism: str
    specimen: str
    antibiotic_codes: tuple[str, ...]


class DatasetManifest(StrictModel):
    manifest_id: str
    importer_version: str = "dataset-v1"
    files: dict[str, FileSummary]
    source_start: date | None
    source_end: date | None
    observed_panels: tuple[ObservedPanel, ...]


class ValidationReport(StrictModel):
    valid: bool
    source_kind: Literal["zip", "directory"]
    files: dict[str, FileSummary]
    manifest: DatasetManifest | None = None
    errors: tuple[ImportIssue, ...] = ()
    error_count: int = 0
    warnings: tuple[ImportIssue, ...] = ()


class DatasetBundle(StrictModel):
    report: ValidationReport
    patients: tuple[PatientRecord, ...]
    wards: tuple[WardRecord, ...]
    antibiotics: tuple[AntibioticRecord, ...]
    cultures: tuple[CultureRecord, ...]
    events: tuple[IsolateEvent, ...]
    enrichments: tuple[IsolateEnrichment, ...]
