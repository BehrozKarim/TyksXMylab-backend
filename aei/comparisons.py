"""Paired laboratory comparisons on a common, first-isolate denominator."""

from .analysis_models import ComparisonQuery, PairedCells, PairedComparison
from .cohorts import resolve_latest
from .evidence import EvidenceQueryError, coverage_for, eligible_for_period, validate_cohort


def calculate_comparison(snapshot, query: ComparisonQuery):
    drugs = []
    for field in ("antibiotic_a", "antibiotic_b"):
        code = getattr(query, field)
        if code not in snapshot.antibiotics:
            raise EvidenceQueryError(field, "Unknown antibiotic code")
        drug = snapshot.antibiotics[code]
        if drug.is_screening:
            raise EvidenceQueryError(field, "Screening markers cannot be used in therapeutic antibiotic comparisons")
        drugs.append(drug)
    validate_cohort(snapshot, query)
    latest, latest_received = resolve_latest(snapshot.events, query.known_at)
    selection, eligible, metadata = eligible_for_period(snapshot, latest, query, query.window)
    cells = dict(both_s=0, a_only_s=0, b_only_s=0, both_r=0)
    cell_names = {("S", "S"): "both_s", ("S", "R"): "a_only_s", ("R", "S"): "b_only_s", ("R", "R"): "both_r"}
    missing_a = missing_b = missing_either = 0
    for event in eligible:
        panel = {item.antibiotic_code: item.result for item in event.tests}
        a, b = panel.get(query.antibiotic_a), panel.get(query.antibiotic_b)
        missing_a += a is None
        missing_b += b is None
        missing_either += a is None or b is None
        if a is not None and b is not None:
            cells[cell_names[a, b]] += 1
    n = sum(cells.values())
    if any(drug.reported_for not in ("all", query.specimen) for drug in drugs):
        status = "not_applicable"
    elif not eligible:
        status = "empty_cohort"
    elif not n:
        status = "no_paired_tests"
    elif n < 30:
        status = "insufficient_data"
    else:
        status = "available"

    def percentage(count):
        return 100 * count / n if status == "available" else None

    return PairedComparison(status=status, antibiotic_a=query.antibiotic_a, antibiotic_b=query.antibiotic_b,
        window=query.window, start_date=selection.period_start, end_date=query.as_of,
        n_eligible=len(eligible), n_tested_a=len(eligible) - missing_a, n_tested_b=len(eligible) - missing_b,
        n_paired=n, n_missing_a=missing_a, n_missing_b=missing_b, n_missing_either=missing_either,
        cells=PairedCells(**cells), a_susceptibility_pct=percentage(cells["both_s"] + cells["a_only_s"]),
        b_susceptibility_pct=percentage(cells["both_s"] + cells["b_only_s"]),
        net_difference_pp=percentage(cells["b_only_s"] - cells["a_only_s"]),
        b_only_added_pct=percentage(cells["b_only_s"]), at_least_one_s_pct=percentage(n - cells["both_r"]),
        metadata=metadata, coverage=coverage_for(snapshot, selection.period_start, query.as_of)), latest_received
