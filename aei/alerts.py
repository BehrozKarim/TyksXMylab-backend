"""Pure alert calculation and lifecycle, independent of HTTP and PostgreSQL."""

from collections import Counter
from datetime import timedelta
from fractions import Fraction
from uuid import uuid4

from .alert_models import AlertCalculation, AlertRecord, AlertWindow, InvestigationMeasurements
from .cohorts import resolve_latest
from .evidence import coverage_for, eligible_for_selection
from .models import AnalysisQuery


def alert_bounds(as_of):
    recent_start = as_of - timedelta(days=27)
    try:
        baseline_start = recent_start.replace(year=recent_start.year - 1)
    except ValueError:
        baseline_start = recent_start.replace(year=recent_start.year - 1, day=28)
    return baseline_start, recent_start, as_of + timedelta(days=1)


def _window(snapshot, latest, monitor, start, stop, minimum):
    selection = AnalysisQuery(start_date=start, as_of=stop - timedelta(days=1),
        ward_code=monitor.ward_code, organism=monitor.organism, specimen=monitor.specimen,
        dedup_scope=monitor.dedup_scope)
    _, eligible, completeness = eligible_for_selection(snapshot, latest, monitor, selection)
    results = [test.result for item in eligible for test in item.tests
               if test.antibiotic_code == monitor.antibiotic_code]
    tested, resistant = len(results), results.count("R")
    raw = sum(start <= item.sample_date < stop and item.organism == monitor.organism
              and item.specimen == monitor.specimen
              and (monitor.ward_code is None or item.ward_code == monitor.ward_code) for item in latest)
    sex = Counter({"F": 0, "M": 0, "unknown": 0})
    ages = Counter({"0-17": 0, "18-64": 0, "65+": 0, "unknown": 0})
    settings = Counter({"in": 0, "out": 0, "unknown": 0})
    later = early = unknown = not_applicable = 0
    for item in eligible:
        metadata = snapshot.enrichments.get(item.event_id)
        sex[metadata.sex if metadata else "unknown"] += 1
        age = metadata.age_years if metadata else None
        ages["unknown" if age is None else "0-17" if age < 18 else "18-64" if age < 65 else "65+"] += 1
        setting = snapshot.ward_types.get(item.ward_code, "unknown")
        settings[setting] += 1
        if setting == "out":
            not_applicable += 1
        elif setting != "in" or metadata is None or metadata.admission_date is None:
            unknown += 1
        else:
            days = (item.sample_date - metadata.admission_date).days
            if days < 0:  # An external correction may lack matching valid enrichment.
                unknown += 1
            elif days >= 2:
                later += 1
            else:
                early += 1
    # Separate population: remove organism, retain specimen/ward/demographics and
    # first-per-patient/species/specimen selection. Never use these counts for AST.
    _, mix, _ = eligible_for_selection(snapshot, latest, monitor, selection.model_copy(update={"organism": None}))
    measurements = InvestigationMeasurements(raw_isolates_before_dedup_and_demographics=raw,
        sex_counts=dict(sex), age_bin_counts=dict(ages), collection_setting_counts=dict(settings),
        hospital_day_3_or_later=later, hospital_day_1_or_2=early, timing_unknown=unknown,
        timing_not_applicable=not_applicable,
        organism_mix_counts=dict(sorted(Counter(item.organism for item in mix).items())))
    return AlertWindow(start_date=start, end_date_exclusive=stop, n_eligible=len(eligible), n_tested=tested,
        n_s=tested - resistant, n_r=resistant, n_not_tested=len(eligible) - tested,
        resistance_pct=100 * resistant / tested if tested >= minimum else None,
        metadata=completeness, coverage=coverage_for(snapshot, start, stop - timedelta(days=1)),
        measurements=measurements), {item.patient_id for item in eligible}


def calculate_alert(snapshot, monitor, configuration, as_of):
    """Compare disjoint periods with an exact rational threshold comparison."""
    if monitor.ward_code and monitor.ward_code not in snapshot.ward_codes:
        return AlertCalculation(status="unavailable", reason="unknown_ward")
    if monitor.organism not in {panel.organism for panel in snapshot.manifest.observed_panels}:
        return AlertCalculation(status="unavailable", reason="unknown_organism")
    drug = snapshot.antibiotics.get(monitor.antibiotic_code)
    if drug is None:
        return AlertCalculation(status="unavailable", reason="unknown_antibiotic")
    if drug.is_screening:
        return AlertCalculation(status="unavailable", reason="screening_antibiotic")
    if drug.reported_for not in ("all", monitor.specimen):
        return AlertCalculation(status="unavailable", reason="antibiotic_not_applicable")
    try:
        baseline_start, recent_start, stop = alert_bounds(as_of)
    except (ValueError, OverflowError):
        return AlertCalculation(status="unavailable", reason="unsupported_date_range")
    latest, _ = resolve_latest(snapshot.events)
    baseline, baseline_patients = _window(snapshot, latest, monitor, baseline_start, recent_start,
                                          configuration.minimum_tested)
    recent, recent_patients = _window(snapshot, latest, monitor, recent_start, stop,
                                      configuration.minimum_tested)
    enough = min(baseline.n_tested, recent.n_tested) >= configuration.minimum_tested
    # Floating-point subtraction can turn exactly 10 pp into 9.999999999999998.
    # Decide using integer ratios, and only convert for the unrounded JSON value.
    difference = (100 * (Fraction(recent.n_r, recent.n_tested) - Fraction(baseline.n_r, baseline.n_tested))
                  if enough else None)
    status = ("insufficient_evidence" if not enough else "triggered"
              if difference >= Fraction(str(configuration.threshold_pp)) else "below_threshold")
    return AlertCalculation(status=status, reason=None if enough else "minimum_tested_not_met",
        baseline=baseline, recent=recent, resistance_change_pp=float(difference) if enough else None,
        shared_patients_between_windows=len(baseline_patients & recent_patients))


def advance_alert(previous, report):
    """No alert identity exists until a first trigger; recurrence keeps that ID."""
    triggered = report.evaluation.status == "triggered"
    context = report.context
    if previous is None:
        if not triggered:
            return None
        identity = str(uuid4())
        return AlertRecord(alert_id=identity, investigation_id=identity,
            investigation_url=f"/api/v1/investigations/{identity}", run_id=context.run_id,
            monitor_id=context.monitor_id, cohort_hash=context.cohort_hash,
            antibiotic_code=context.monitor.antibiotic_code, rule_version=context.rule_version,
            status="new", last_sufficient_state="new", first_trigger_revision=context.state_revision,
            last_trigger_revision=context.state_revision, first_trigger_date=context.monitoring_as_of,
            last_trigger_date=context.monitoring_as_of, original_analysis_id=report.analysis_id,
            current_analysis_id=report.analysis_id)
    status = ("ongoing" if triggered else "cleared" if report.evaluation.status == "below_threshold"
              else "insufficient_evidence")
    return previous.model_copy(update={"status": status, "current_analysis_id": report.analysis_id,
        "last_sufficient_state": previous.last_sufficient_state if status == "insufficient_evidence" else status,
        "last_trigger_revision": context.state_revision if triggered else previous.last_trigger_revision,
        "last_trigger_date": context.monitoring_as_of if triggered else previous.last_trigger_date})
