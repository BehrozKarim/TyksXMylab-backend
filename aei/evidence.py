"""Enriched evidence calculations independent of storage and HTTP."""

from datetime import datetime, timezone
from math import sqrt
from statistics import NormalDist

from .cohorts import resolve_latest, select_first_isolates
from .evidence_models import (
    ConfidenceInterval, ConfidenceIntervals, EvidenceContext, EvidenceQuery, EvidenceReport,
    EvidenceWindow, EvidenceWindows, MetadataCompleteness, SourceCoverage,
)
from .evidence_source import EvidenceSnapshot
from .models import AnalysisQuery


class EvidenceQueryError(ValueError):
    def __init__(self, field: str, message: str):
        self.field = field
        super().__init__(message)


def wilson_interval(successes: int, tested: int) -> ConfidenceInterval:
    """Two-sided 95% Wilson score bounds, expressed in percentage units.

    Formula: https://itl.nist.gov/div898/handbook/prc/section2/prc241.htm
    Reporting suppression is applied by the caller, independently of this formula.
    """
    if tested < 0 or not 0 <= successes <= tested:
        raise ValueError("Expected 0 <= successes <= tested")
    if tested == 0:
        return ConfidenceInterval()
    z = NormalDist().inv_cdf(0.975)
    p = successes / tested
    denominator = 1 + z * z / tested
    center = (p + z * z / (2 * tested)) / denominator
    half = z * sqrt(p * (1 - p) / tested + z * z / (4 * tested * tested)) / denominator
    return ConfidenceInterval(lower=0.0 if successes == 0 else max(0.0, 100 * (center - half)),
                              upper=100.0 if successes == tested else min(100.0, 100 * (center + half)))


def coverage_for(snapshot: EvidenceSnapshot, start, end) -> SourceCoverage:
    manifest = snapshot.manifest
    source_start, source_end = manifest.source_start, manifest.source_end
    overlap_start = max(start, source_start) if source_start else None
    overlap_end = min(end, source_end) if source_end else None
    if overlap_start is None or overlap_end is None or overlap_start > overlap_end:
        overlap_start = overlap_end = None
    expected_cultures = manifest.files["cultures.csv"].rows
    expected_isolates = manifest.files["isolates.csv"].rows
    return SourceCoverage(
        source_start=source_start, source_end=source_end, overlap_start=overlap_start, overlap_end=overlap_end,
        source_date_span_covers_window=bool(source_start and source_end and source_start <= start and end <= source_end),
        imported_culture_start=snapshot.imported_culture_start, imported_culture_end=snapshot.imported_culture_end,
        source_fully_imported=(snapshot.imported_cultures == expected_cultures
                               and len(snapshot.enrichments) == expected_isolates),
        source_cultures_expected=expected_cultures, source_cultures_imported=snapshot.imported_cultures,
        source_isolates_expected=expected_isolates, source_isolates_enriched=len(snapshot.enrichments))


def calculate_evidence(snapshot: EvidenceSnapshot, query: EvidenceQuery) -> EvidenceReport:
    if query.antibiotic_code not in snapshot.antibiotics:
        raise EvidenceQueryError("antibiotic_code", "Unknown antibiotic code")
    wards = snapshot.ward_codes | {event.ward_code for event in snapshot.events}
    if query.ward_code is not None and query.ward_code not in wards:
        raise EvidenceQueryError("ward_code", "Unknown ward code")
    organisms = {panel.organism for panel in snapshot.manifest.observed_panels}
    organisms.update(event.organism for event in snapshot.events)
    if query.organism not in organisms:
        raise EvidenceQueryError("organism", "Unknown organism")
    drug = snapshot.antibiotics[query.antibiotic_code]
    latest, latest_received = resolve_latest(snapshot.events, query.known_at)
    windows = {}
    for period in ("rolling_28_days", "rolling_12_months"):
        selection = AnalysisQuery(as_of=query.as_of, period=period, ward_code=query.ward_code,
            organism=query.organism, specimen=query.specimen, dedup_scope=query.dedup_scope)
        first = select_first_isolates(latest, selection)
        eligible = []
        unknown_sex = unknown_age = excluded_unknown = excluded_nonmatching = 0
        for event in first:
            # Metadata belongs to an event ID, never just a patient or an isolate ID.
            metadata = snapshot.enrichments.get(event.event_id)
            sex = metadata.sex if metadata else None
            age = metadata.age_years if metadata else None
            unknown_sex += sex is None
            unknown_age += age is None
            if ((query.sex is not None and sex is None)
                    or ((query.age_min is not None or query.age_max is not None) and age is None)):
                excluded_unknown += 1
                continue
            if ((query.sex is not None and sex != query.sex)
                    or (query.age_min is not None and age < query.age_min)
                    or (query.age_max is not None and age > query.age_max)):
                excluded_nonmatching += 1
                continue
            eligible.append(event)
        results = [test.result for event in eligible for test in event.tests
                   if test.antibiotic_code == query.antibiotic_code]
        tested, susceptible = len(results), results.count("S")
        resistant = tested - susceptible
        if drug.reported_for not in ("all", query.specimen):
            status = "not_applicable"
        elif not eligible:
            status = "empty_cohort"
        elif tested == 0:
            status = "no_tests"
        elif tested < 30:
            status = "insufficient_data"
        else:
            status = "available"
        available = status == "available"
        windows[period] = EvidenceWindow(
            period=period, start_date=selection.period_start, end_date=query.as_of, status=status,
            n_eligible=len(eligible), n_tested=tested, n_s=susceptible, n_r=resistant,
            n_not_tested=len(eligible) - tested,
            susceptibility_pct=100 * susceptible / tested if available else None,
            resistance_pct=100 * resistant / tested if available else None,
            ci95=ConfidenceIntervals(
                susceptibility=wilson_interval(susceptible, tested) if available else ConfidenceInterval(),
                resistance=wilson_interval(resistant, tested) if available else ConfidenceInterval()),
            metadata=MetadataCompleteness(retained_before_demographic_filters=len(first),
                unknown_sex=unknown_sex, unknown_age=unknown_age, excluded_unknown=excluded_unknown,
                excluded_nonmatching=excluded_nonmatching),
            coverage=coverage_for(snapshot, selection.period_start, query.as_of))
    return EvidenceReport(context=EvidenceContext(
        ward_scope="selected_ward" if query.ward_code else "all_wards", filters=query,
        source_manifest_id=snapshot.manifest.manifest_id, generated_at=datetime.now(timezone.utc),
        latest_received_at=latest_received, antibiotic_is_screening=drug.is_screening),
        windows=EvidenceWindows(**windows))
