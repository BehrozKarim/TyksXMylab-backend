"""Additive evidence contract; legacy prototype-v2 responses stay unchanged."""

from datetime import date
from typing import Literal

from pydantic import AwareDatetime, Field, model_validator

from .models import AnalysisQuery, StrictModel


class EvidenceQuery(StrictModel):
    as_of: date
    organism: str = Field(min_length=1)
    specimen: Literal["urine", "blood", "wound"]
    antibiotic_code: str = Field(min_length=1)
    ward_code: str | None = Field(default=None, min_length=1)
    sex: Literal["F", "M"] | None = None
    age_min: int | None = Field(default=None, ge=0)
    age_max: int | None = Field(default=None, ge=0)
    dedup_scope: Literal["selected_cohort", "all_wards"] = "selected_cohort"
    known_at: AwareDatetime | None = None

    @model_validator(mode="after")
    def valid_filters(self):
        if self.age_min is not None and self.age_max is not None and self.age_min > self.age_max:
            raise ValueError("age_min cannot exceed age_max")
        # Reuse the established date conventions and supported-date checks.
        if self.as_of is not None:  # The HTTP request resolves an omitted date from its run.
            for period in ("rolling_28_days", "rolling_12_months"):
                AnalysisQuery(as_of=self.as_of, period=period)
        return self


class ConfidenceInterval(StrictModel):
    lower: float | None = None
    upper: float | None = None


class ConfidenceIntervals(StrictModel):
    susceptibility: ConfidenceInterval
    resistance: ConfidenceInterval


class MetadataCompleteness(StrictModel):
    retained_before_demographic_filters: int
    unknown_sex: int
    unknown_age: int
    excluded_unknown: int
    excluded_nonmatching: int


class SourceCoverage(StrictModel):
    source_start: date | None
    source_end: date | None
    overlap_start: date | None
    overlap_end: date | None
    source_date_span_covers_window: bool
    imported_culture_start: date | None
    imported_culture_end: date | None
    source_fully_imported: bool
    source_cultures_expected: int
    source_cultures_imported: int
    source_isolates_expected: int
    source_isolates_enriched: int


class EvidenceWindow(StrictModel):
    period: Literal["rolling_28_days", "rolling_12_months"]
    start_date: date
    end_date: date
    status: Literal["not_applicable", "empty_cohort", "no_tests", "insufficient_data", "available"]
    n_eligible: int
    n_tested: int
    n_s: int
    n_r: int
    n_not_tested: int
    susceptibility_pct: float | None
    resistance_pct: float | None
    ci95: ConfidenceIntervals
    metadata: MetadataCompleteness
    coverage: SourceCoverage


class EvidenceWindows(StrictModel):
    rolling_28_days: EvidenceWindow
    rolling_12_months: EvidenceWindow


class EvidenceContext(StrictModel):
    method_version: Literal["evidence-v1"] = "evidence-v1"
    base_rules_version: Literal["prototype-v2"] = "prototype-v2"
    minimum_tested: Literal[30] = 30
    interval_method: Literal["wilson_95"] = "wilson_95"
    demographic_filter_order: Literal["after_first_isolate"] = "after_first_isolate"
    ward_scope: Literal["selected_ward", "all_wards"]
    source_manifest_id: str
    generated_at: AwareDatetime
    latest_received_at: AwareDatetime | None
    antibiotic_is_screening: bool
    filters: EvidenceQuery


class EvidenceReport(StrictModel):
    context: EvidenceContext
    windows: EvidenceWindows
