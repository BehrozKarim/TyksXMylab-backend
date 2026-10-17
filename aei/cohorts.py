"""Shared revision resolution and first-isolate selection for both report APIs."""

from datetime import datetime
from typing import Iterable

from .models import AnalysisQuery, IsolateEvent


def resolve_latest(events: Iterable[IsolateEvent], known_at: datetime | None = None):
    latest: dict[str, IsolateEvent] = {}
    latest_received = None
    for event in events:
        if known_at and event.received_at > known_at:
            continue
        if latest_received is None or event.received_at > latest_received:
            latest_received = event.received_at
        previous = latest.get(event.isolate_id)
        if previous is None or event.revision > previous.revision:
            latest[event.isolate_id] = event
        elif event.revision == previous.revision and event.canonical_json() != previous.canonical_json():
            raise ValueError("Conflicting revisions for an isolate")
    return tuple(latest.values()), latest_received


def select_first_isolates(latest: Iterable[IsolateEvent], query: AnalysisQuery) -> tuple[IsolateEvent, ...]:
    """Input must already contain the latest visible revision of each isolate."""
    start = query.period_start
    candidates = []
    for event in latest:
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
    candidates.sort(key=lambda event: (event.sample_date, event.culture_id, event.isolate_id))
    first: dict[tuple[str, str, str], IsolateEvent] = {}
    for event in candidates:
        first.setdefault((event.patient_id, event.organism, event.specimen), event)
    return tuple(event for event in first.values() if not query.ward_code or event.ward_code == query.ward_code)
