"""Allowlist aggregate evidence facts; render all numbers and caveats on the server."""

import math
from datetime import datetime, timezone

from .publication_models import SavedEvidence, ServiceError
from .summary_models import SummaryDraft, SummaryFact, SummaryStatement


def extract_facts(report):
    if not isinstance(report, SavedEvidence):
        raise ServiceError(422, "unsupported_analysis", "Summaries currently support saved evidence reports only")
    context, query = report.context, report.context.filters
    for label in (query.organism, query.antibiotic_code, query.ward_code or ""):
        if len(label) > 200 or any(ord(character) < 32 for character in label):
            raise ServiceError(422, "unsupported_report_label", "Report labels cannot be summarized")
    facts = []

    def add(key, text, *paths, section="paragraph", alternative=None):
        facts.append(SummaryFact(fact_id=key, section=section,
            variants=(text, alternative) if alternative else (text,), paths=tuple(paths)))

    ward = f"ward {query.ward_code}" if query.ward_code else "all wards"
    population = "female patients" if query.sex == "F" else "male patients" if query.sex == "M" else "all sexes"
    ages = (f"ages {query.age_min}-{query.age_max}" if query.age_min is not None and query.age_max is not None
            else f"ages {query.age_min} and above" if query.age_min is not None
            else f"ages up to {query.age_max}" if query.age_max is not None else "all ages")
    subject = f"{query.organism} in {query.specimen} from {ward} ({population}, {ages}), {query.antibiotic_code}"
    add("scope", f"This report covers {subject}, through {query.as_of}.", "context.filters",
        alternative=f"Through {query.as_of}, the selected report covers {subject}.")
    partial = False
    for key, title in (("rolling_12_months", "Last 12 months"), ("rolling_28_days", "Last 28 days")):
        window = getattr(report.windows, key)
        prefix = f"windows.{key}"
        counts = (window.n_eligible, window.n_tested, window.n_s, window.n_r, window.n_not_tested)
        expected = ("empty_cohort" if not window.n_eligible else "no_tests" if not window.n_tested
                    else "insufficient_data" if window.n_tested < context.minimum_tested else "available")
        if (min(counts) < 0 or window.n_s + window.n_r != window.n_tested
                or window.n_tested + window.n_not_tested != window.n_eligible
                or (window.status != "not_applicable" and window.status != expected)):
            raise ServiceError(422, "invalid_summary_source", "Saved report has inconsistent reporting counts or status")
        period = f"{title} ({window.start_date} to {window.end_date}, inclusive)"
        if window.status == "available":
            values = (window.susceptibility_pct, window.ci95.susceptibility.lower, window.ci95.susceptibility.upper)
            if any(v is None or not math.isfinite(v) or not 0 <= v <= 100 for v in values):
                raise ServiceError(422, "invalid_summary_source", "Saved report has invalid available estimates")
            lower, upper = values[1:]
            if lower > values[0] or values[0] > upper:
                raise ServiceError(422, "invalid_summary_source", "Saved report has an inconsistent confidence interval")
            metric = "screening-marker S results" if context.antibiotic_is_screening else "susceptibility"
            add(key, f"{period}: {metric} was {values[0]:.1f}% among {window.n_tested} tested isolates.",
                f"{prefix}.susceptibility_pct", f"{prefix}.n_tested", f"{prefix}.start_date", f"{prefix}.end_date",
                alternative=f"{period}: {window.n_tested} isolates were tested, with {values[0]:.1f}% {metric}.")
            add(f"{key}.interval", f"{title}: the 95% Wilson confidence interval is {lower:.1f}% to {upper:.1f}%.",
                f"{prefix}.ci95.susceptibility", "context.interval_method", section="support")
        else:
            # S/R counts, percentages and intervals never enter the model's input
            # for any suppressed window, even if an inconsistent source contains them.
            text = {"empty_cohort": "no eligible isolates matched this selection",
                "no_tests": f"none of {window.n_eligible} eligible isolates had a recorded test for this antibiotic",
                "insufficient_data": f"insufficient data: {window.n_tested} tested isolates, below the minimum of {context.minimum_tested}",
                "not_applicable": "this antibiotic and specimen selection is not applicable"}[window.status]
            add(key, f"{period}: {text}; no percentage is reported.", f"{prefix}.status", f"{prefix}.n_tested",
                f"{prefix}.start_date", f"{prefix}.end_date")
        add(f"{key}.denominator", f"{title}: {window.n_eligible} eligible isolates; {window.n_tested} tested; "
            f"{window.n_not_tested} without a recorded test.", f"{prefix}.n_eligible", f"{prefix}.n_tested",
            f"{prefix}.n_not_tested", section="support")
        if window.n_not_tested:
            add(f"{key}.missing_tests", f"{title}: {window.n_not_tested} eligible isolates lack a recorded test; "
                "percentages describe tested isolates only.", f"{prefix}.n_not_tested", section="limitation")
        if not window.coverage.source_date_span_covers_window:
            partial = True
            add(f"{key}.coverage", f"{title}: the imported source date span does not cover the full requested period.",
                f"{prefix}.coverage.source_date_span_covers_window", section="limitation")
        if not window.coverage.source_fully_imported:
            partial = True
            add(f"{key}.publication", f"{title}: not all expected source records for the period are published.",
                f"{prefix}.coverage.source_fully_imported", section="limitation")
        if window.metadata.excluded_unknown:
            add(f"{key}.demographics", f"{title}: {window.metadata.excluded_unknown} retained isolates were excluded "
                "because required demographic information was missing.", f"{prefix}.metadata.excluded_unknown", section="limitation")
    add("counting", "Counts use the first selected isolate per patient, species, specimen and reporting period.",
        "context.base_rules_version", "context.filters.dedup_scope")
    if partial:
        add("partial_coverage", "Coverage is incomplete; interpretation is limited to the available records.",
            "windows.rolling_12_months.coverage", "windows.rolling_28_days.coverage")
    add("overlap", "The periods overlap, so these figures alone do not establish a change over time.",
        "windows.rolling_12_months.start_date", "windows.rolling_28_days.start_date",
        alternative="The 12-month period includes the recent 28 days; their difference alone does not establish a trend.")
    if context.mode == "feed":
        add("feed_coverage", "Imported-source coverage does not certify the completeness of incoming feed records.",
            "context.mode", section="limitation")
    if context.antibiotic_is_screening:
        add("screening", "The selected code is a screening marker, not a therapeutic susceptibility estimate.",
            "context.antibiotic_is_screening", section="limitation")
    if query.known_at:
        add("known_at", f"Only records known by {query.known_at.isoformat()} were considered.",
            "context.filters.known_at", section="limitation")
    ward_selection = ("Selection occurs within the selected ward." if query.ward_code and query.dedup_scope == "selected_cohort"
                      else "Selection occurs across all wards before retaining isolates from the selected ward." if query.ward_code
                      else "Selection occurs across all wards.")
    add("eligibility", "Each period selects the first isolate per patient, species and specimen. "
        f"{ward_selection} Age and sex filters apply after selection.",
        "context.filters.dedup_scope", "context.demographic_filter_order", section="support")
    add("threshold", "Percentages require at least 30 eligible tested isolates; meeting this minimum does not guarantee precision.",
        "context.minimum_tested", section="support")
    add("interpretation", "Laboratory results do not establish treatment effectiveness or determine therapy for an individual patient.",
        "context.method_version", alternative="These laboratory observations do not establish treatment effectiveness or select therapy for an individual patient.")
    return tuple(facts)


def statement(fact, variant=0):
    return SummaryStatement(fact_id=fact.fact_id, text=fact.variants[variant], paths=fact.paths)


def complete_summary(saved, facts, draft=None, fallback_reason=None):
    required = {f.fact_id: f for f in facts if f.section == "paragraph"}
    if draft is not None:
        draft = SummaryDraft.model_validate(draft)
        ids = [s.fact_id for s in draft.sentences]
        if (len(ids) != len(required) or set(ids) != set(required)
                or ids[0] != "scope" or ids[-1] != "interpretation"):
            raise ValueError("Missing, duplicate, unsupported or misplaced summary facts")
        if any(s.variant >= len(required[s.fact_id].variants) for s in draft.sentences):
            raise ValueError("Unknown sentence variant")
        sentences = tuple(statement(required[s.fact_id], s.variant) for s in draft.sentences)
    else:
        sentences = tuple(statement(f) for f in required.values())
    return saved.model_copy(update={"status": "ready", "output_mode": "ai" if draft is not None else "template",
        "paragraph": " ".join(s.text for s in sentences), "statements": sentences,
        "limitations": tuple(statement(f) for f in facts if f.section == "limitation"),
        "supporting_facts": tuple(statement(f) for f in facts if f.section == "support"),
        "generated_at": datetime.now(timezone.utc), "validation": "approved_sentences" if draft is not None else "template",
        "fallback_reason": fallback_reason})
