"""Deterministic reference processor: no storage, clock, HTTP or Flink dependency."""

from collections import defaultdict
from typing import Iterable, Protocol

from .models import AnalysisQuery, AnalysisResult, ComparisonResult, IsolateEvent, Statistic


class Processor(Protocol):
    def calculate(self, events: Iterable[IsolateEvent], query: AnalysisQuery) -> AnalysisResult: ...


class ReferenceProcessor:
    def calculate(self, events: Iterable[IsolateEvent], query: AnalysisQuery) -> AnalysisResult:
        start = query.period_start

        # Revisions are resolved BEFORE filtering sample dates or cohorts. A correction
        # can move an isolate out of a period; its old revision must not reappear.
        latest: dict[str, IsolateEvent] = {}
        latest_received = None
        for event in events:
            if query.known_at and event.received_at > query.known_at:
                continue
            if latest_received is None or event.received_at > latest_received:
                latest_received = event.received_at
            previous = latest.get(event.isolate_id)
            if previous is None or event.revision > previous.revision:
                latest[event.isolate_id] = event
            elif event.revision == previous.revision and event.canonical_json() != previous.canonical_json():
                raise ValueError("Conflicting revisions for an isolate")

        candidates = []
        for event in latest.values():
            if not start <= event.sample_date <= query.as_of:
                continue
            if query.organism and event.organism != query.organism:
                continue
            if query.specimen and event.specimen != query.specimen:
                continue
            if (query.dedup_scope == "selected_cohort" and query.ward_code
                    and event.ward_code != query.ward_code):
                continue
            candidates.append(event)

        # Source has dates, not collection times. IDs provide a documented stable tie-break.
        candidates.sort(key=lambda event: (event.sample_date, event.culture_id, event.isolate_id))
        first: dict[tuple[str, str, str], IsolateEvent] = {}
        for event in candidates:
            first.setdefault((event.patient_id, event.organism, event.specimen), event)

        groups = defaultdict(list)
        for event in first.values():
            if query.ward_code and event.ward_code != query.ward_code:
                continue
            groups[(event.ward_code, event.organism, event.specimen)].append(event)

        rows = []
        for (ward, organism, specimen), isolates in sorted(groups.items()):
            # Antibiotic selection happens AFTER first-isolate selection. An untested
            # first isolate must not be replaced by a later tested isolate.
            codes = {query.antibiotic_code} if query.antibiotic_code else {
                test.antibiotic_code for isolate in isolates for test in isolate.tests
            }
            for code in sorted(codes):
                results = [test.result for isolate in isolates for test in isolate.tests
                           if test.antibiotic_code == code]
                tested = len(results)
                susceptible = results.count("S")
                resistant = results.count("R")
                available = tested >= 30
                rows.append(Statistic(
                    ward_code=ward, organism=organism, specimen=specimen,
                    antibiotic_code=code, eligible_isolates=len(isolates),
                    tested=tested, susceptible=susceptible, resistant=resistant,
                    untested=len(isolates) - tested,
                    susceptibility_percent=round(100 * susceptible / tested, 2) if available else None,
                    resistance_percent=round(100 * resistant / tested, 2) if available else None,
                    status="available" if available else "insufficient_data",
                ))
        return AnalysisResult(period=query.period_name, start_date=start, end_date=query.as_of, query=query,
                              latest_received_at=latest_received, rows=tuple(rows))


def compare_periods(processor: Processor, events: Iterable[IsolateEvent], query: AnalysisQuery) -> ComparisonResult:
    if query.period is not None or query.start_date is not None or query.window_days is not None:
        raise ValueError("Comparison uses fixed rolling 28-day and 12-month periods; omit period overrides")
    # Both reports use the exact same source snapshot, but deduplicate independently.
    snapshot = list(events)
    common = query.model_dump(exclude={"period", "start_date", "window_days"})
    return ComparisonResult(
        rolling_28_days=processor.calculate(snapshot, AnalysisQuery(**common, period="rolling_28_days")),
        rolling_12_months=processor.calculate(snapshot, AnalysisQuery(**common, period="rolling_12_months")),
    )
