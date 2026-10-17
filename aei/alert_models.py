"""Versioned, aggregate-only monitoring and investigation contracts."""

import hashlib
import json
import os
from datetime import date
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator

from .evidence_models import MetadataCompleteness, SourceCoverage
from .models import StrictModel
from .publication_models import ExpectedState, SourceContext


def fingerprint(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class AlertMonitor(StrictModel):
    organism: str = Field(default="Escherichia coli", min_length=1)
    specimen: Literal["urine", "blood", "wound"] = "urine"
    ward_code: str | None = Field(default=None, min_length=1)
    antibiotic_code: str = Field(default="CIP", min_length=1)
    sex: Literal["F", "M"] | None = None
    age_min: int | None = Field(default=None, ge=0)
    age_max: int | None = Field(default=None, ge=0)
    dedup_scope: Literal["selected_cohort", "all_wards"] = "selected_cohort"

    @model_validator(mode="after")
    def ordered_ages(self):
        if self.age_min is not None and self.age_max is not None and self.age_min > self.age_max:
            raise ValueError("age_min cannot exceed age_max")
        return self

    @property
    def monitor_id(self):
        return fingerprint(self.model_dump(mode="json"))

    @property
    def cohort_hash(self):
        return fingerprint(self.model_dump(mode="json", exclude={"antibiotic_code"}))


class AlertConfiguration(StrictModel):
    rule_version: Literal["resistance-rise-v1"] = "resistance-rise-v1"
    minimum_tested: Literal[30] = 30
    threshold_pp: float = Field(default=10.0, gt=0, le=100, allow_inf_nan=False)
    confirmation: Literal["proposed_pending_partner_confirmation"] = "proposed_pending_partner_confirmation"
    monitors: tuple[AlertMonitor, ...] = Field(default_factory=lambda: tuple(
        AlertMonitor(ward_code=ward) for ward in ("PHC-S", "PHC-N", "ED", "URO")), max_length=32)

    @model_validator(mode="after")
    def unique_monitors(self):
        if len({monitor.monitor_id for monitor in self.monitors}) != len(self.monitors):
            raise ValueError("Duplicate alert monitors are not allowed")
        return self

    @property
    def configuration_hash(self):
        return fingerprint(self.model_dump(mode="json"))


def load_alert_configuration():
    path = os.environ.get("AEI_ALERT_CONFIG")
    # Explicit missing/invalid configuration fails; never silently fall back.
    return (AlertConfiguration.model_validate_json(Path(path).read_text(encoding="utf-8-sig"))
            if path else AlertConfiguration())


class InvestigationMeasurements(StrictModel):
    raw_isolates_before_dedup_and_demographics: int
    sex_counts: dict[str, int]
    age_bin_counts: dict[str, int]
    collection_setting_counts: dict[str, int]
    hospital_day_3_or_later: int
    hospital_day_1_or_2: int
    timing_unknown: int
    timing_not_applicable: int
    organism_mix_counts: dict[str, int]
    population: Literal["eligible_first_isolates_including_untested"] = "eligible_first_isolates_including_untested"
    organism_mix_population: Literal["first_isolates_with_organism_filter_removed_count_only"] = (
        "first_isolates_with_organism_filter_removed_count_only")


class AlertWindow(StrictModel):
    start_date: date
    end_date_exclusive: date
    n_eligible: int
    n_tested: int
    n_s: int
    n_r: int
    n_not_tested: int
    resistance_pct: float | None
    metadata: MetadataCompleteness
    coverage: SourceCoverage
    measurements: InvestigationMeasurements


class AlertCalculation(StrictModel):
    status: Literal["triggered", "below_threshold", "insufficient_evidence", "unavailable"]
    reason: str | None = None
    baseline: AlertWindow | None = None
    recent: AlertWindow | None = None
    resistance_change_pp: float | None = None
    shared_patients_between_windows: int = 0


class AlertContext(SourceContext):
    method_version: Literal["alert-v1"] = "alert-v1"
    base_rules_version: Literal["prototype-v2"] = "prototype-v2"
    monitoring_as_of: date
    monitor_id: str
    cohort_hash: str
    monitor: AlertMonitor
    configuration_hash: str
    rule_version: Literal["resistance-rise-v1"] = "resistance-rise-v1"
    minimum_tested: Literal[30] = 30
    threshold_pp: float
    confirmation: Literal["proposed_pending_partner_confirmation"] = "proposed_pending_partner_confirmation"
    baseline_definition: Literal["12_calendar_months_before_recent_28_days"] = "12_calendar_months_before_recent_28_days"
    interpretation: Literal["heuristic_review_signal_not_outbreak_or_significance"] = "heuristic_review_signal_not_outbreak_or_significance"
    caveats: tuple[str, ...] = (
        "Patients may occur in both periods; the populations are not assumed independent.",
        "Composition and testing counts describe observations, not causes.",
        "Hospital day 3 or later is a timing proxy and does not establish infection origin.",
        "Organism mix has a separate count-only population, not the resistance denominator.",
    )


class SavedAlertAnalysis(StrictModel):
    analysis_id: str
    context: AlertContext
    evaluation: AlertCalculation


AlertStatus = Literal["new", "ongoing", "cleared", "insufficient_evidence"]


class AlertRecord(StrictModel):
    alert_id: str
    investigation_id: str
    investigation_url: str
    run_id: str
    monitor_id: str
    cohort_hash: str
    antibiotic_code: str
    rule_version: str
    status: AlertStatus
    last_sufficient_state: Literal["new", "ongoing", "cleared"]
    first_trigger_revision: int
    last_trigger_revision: int
    first_trigger_date: date
    last_trigger_date: date
    original_analysis_id: str
    current_analysis_id: str


class AlertTransition(StrictModel):
    state_revision: int
    monitoring_as_of: date
    previous_status: AlertStatus | None
    status: AlertStatus
    analysis_id: str


class AlertListRequest(ExpectedState):
    # Alerts follow the monitoring clock. Historical report filters are forbidden.
    states: tuple[AlertStatus, ...] = ("new", "ongoing", "insufficient_evidence")


class AlertList(StrictModel):
    run_id: str
    state_revision: int
    monitoring_revision: int
    monitoring_as_of: date
    configuration: AlertConfiguration
    configuration_hash: str
    alerts: tuple[AlertRecord, ...]
    monitors: tuple[SavedAlertAnalysis, ...]


class Investigation(StrictModel):
    alert: AlertRecord
    original_detection: SavedAlertAnalysis
    current_analysis: SavedAlertAnalysis | None
    current_analysis_status: Literal["available", "archived_run"]
    transitions: tuple[AlertTransition, ...]
