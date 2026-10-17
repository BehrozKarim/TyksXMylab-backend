"""Comparison and specimen-history contracts, using the established two periods."""

from datetime import date
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, Field, model_validator

from .dataset_models import CultureRecord, FileSummary, IsolateEnrichment
from .evidence_models import CohortQuery, MetadataCompleteness, SourceCoverage
from .models import AnalysisQuery, IsolateEvent, StrictModel
from .publication_models import ExpectedState, SavedEvidence


class ComparisonQuery(CohortQuery):
    antibiotic_a: str = Field(min_length=1)
    antibiotic_b: str = Field(min_length=1)
    window: Literal["rolling_28_days", "rolling_12_months"] = "rolling_28_days"

    @model_validator(mode="after")
    def distinct_drugs(self):
        if self.antibiotic_a == self.antibiotic_b:
            raise ValueError("Choose two different antibiotics")
        return self


class ComparisonRequest(ComparisonQuery, ExpectedState):
    as_of: date | None = None

    def resolved(self, demo_date):
        values = self.model_dump(include=set(ComparisonQuery.model_fields))
        values["as_of"] = self.as_of or demo_date
        return ComparisonQuery(**values)


class PairedCells(StrictModel):
    both_s: int
    a_only_s: int
    b_only_s: int
    both_r: int


class PairedComparison(StrictModel):
    status: Literal["not_applicable", "empty_cohort", "no_paired_tests", "insufficient_data", "available"]
    antibiotic_a: str
    antibiotic_b: str
    window: Literal["rolling_28_days", "rolling_12_months"]
    start_date: date
    end_date: date
    n_eligible: int
    n_tested_a: int
    n_tested_b: int
    n_paired: int
    n_missing_a: int
    n_missing_b: int
    n_missing_either: int
    cells: PairedCells
    a_susceptibility_pct: float | None
    b_susceptibility_pct: float | None
    net_difference_pp: float | None
    b_only_added_pct: float | None
    at_least_one_s_pct: float | None
    metadata: MetadataCompleteness
    coverage: SourceCoverage


class SourceContext(StrictModel):
    run_id: str
    state_revision: int
    demo_date: date
    published_cultures: int
    published_isolates: int
    latest_visible_sample_date: date | None
    source_manifest_id: str
    source_kind: Literal["zip", "directory"]
    source_files: dict[str, FileSummary]
    importer_version: str
    generated_at: AwareDatetime
    visibility: Literal["published_culture_bundles"] = "published_culture_bundles"


class ComparisonContext(SourceContext):
    method_version: Literal["comparison-v1"] = "comparison-v1"
    base_rules_version: Literal["prototype-v2"] = "prototype-v2"
    cohort_method_version: Literal["evidence-v1"] = "evidence-v1"
    minimum_tested: Literal[30] = 30
    demographic_filter_order: Literal["after_first_isolate"] = "after_first_isolate"
    ward_scope: Literal["selected_ward", "all_wards"]
    denominator: Literal["retained_isolates_tested_for_both"] = "retained_isolates_tested_for_both"
    interpretation: Literal["paired_laboratory_susceptibility"] = "paired_laboratory_susceptibility"
    latest_received_at: AwareDatetime | None
    filters: ComparisonQuery


class SavedComparison(StrictModel):
    analysis_id: str
    context: ComparisonContext
    comparison: PairedComparison


class PatientSearchRequest(ExpectedState):
    search: str = Field(min_length=1, max_length=100)
    as_of: date | None = None
    limit: int = Field(default=20, ge=1, le=100)


class PatientMatch(StrictModel):
    patient_id: str
    latest_sample_date: date
    visible_cultures: int


class PatientSearchResult(StrictModel):
    run_id: str
    state_revision: int
    as_of: date
    search: str
    limit: int
    has_more: bool
    patients: tuple[PatientMatch, ...]


class HistoryQuery(StrictModel):
    patient_id: str = Field(min_length=1)
    as_of: date
    limit: int = Field(default=25, ge=1, le=100)
    cursor: UUID | None = None

    @model_validator(mode="after")
    def valid_cutoff(self):
        AnalysisQuery(as_of=self.as_of, period="rolling_12_months")
        return self


class HistoryRequest(ExpectedState):
    as_of: date | None = None
    limit: int = Field(default=25, ge=1, le=100)
    cursor: UUID | None = None

    def resolved(self, patient_id, demo_date):
        return HistoryQuery(patient_id=patient_id, as_of=self.as_of or demo_date,
                            limit=self.limit, cursor=self.cursor)


class HistoryIsolate(StrictModel):
    event: IsolateEvent
    enrichment: IsolateEnrichment | None


class HistoryCulture(StrictModel):
    event_id: str
    revision: int
    record: CultureRecord
    isolates: tuple[HistoryIsolate, ...]


class HistoryContext(SourceContext):
    method_version: Literal["history-v1"] = "history-v1"
    filters: HistoryQuery
    start_date: date
    end_date_exclusive: date
    label: Literal["Specimens collected before the selected date"] = "Specimens collected before the selected date"
    selection: Literal["all_specimens_without_surveillance_deduplication"] = "all_specimens_without_surveillance_deduplication"


class SavedHistory(StrictModel):
    analysis_id: str
    context: HistoryContext
    cultures: tuple[HistoryCulture, ...]
    has_more: bool
    next_cursor: str | None


SavedAnalysis = SavedEvidence | SavedComparison | SavedHistory
