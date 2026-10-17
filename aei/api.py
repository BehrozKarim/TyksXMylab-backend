from typing import Annotated

from fastapi import Body, FastAPI, HTTPException, Query

from .config import database_url as configured_database_url
from .evidence import EvidenceQueryError, calculate_evidence
from .evidence_models import EvidenceQuery, EvidenceReport
from .evidence_source import EvidenceUnavailable
from .models import AnalysisQuery, AnalysisResult, ComparisonResult, IsolateEvent
from .postgres import PostgresEventStore
from .processor import Processor, ReferenceProcessor, compare_periods
from .store import EventConflict, EventStore


def create_app(database_url: str | None = None, processor: Processor | None = None,
               *, store: EventStore | None = None) -> FastAPI:
    store = store if store is not None else PostgresEventStore(database_url or configured_database_url())
    calculation = processor or ReferenceProcessor()
    app = FastAPI(
        title="Tyks AEI reference pipeline",
        version="0.3.0",
        description=("PostgreSQL event history and reference calculations for rolling 28-day and "
                     "12-month reports. Flink, alerts and a dashboard are not active yet."),
    )

    @app.get("/health")
    def health():
        return {"status": "ok", "storage": store.backend, "processor": type(calculation).__name__,
                "events": store.count(), "rules_version": "prototype-v2"}

    def append(events):
        try:
            inserted, duplicates = store.append_many(events)
        except EventConflict as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return {"inserted": inserted, "duplicates": duplicates}

    @app.post("/events")
    def ingest(event: IsolateEvent):
        return append([event])

    @app.post("/events/batch")
    def ingest_batch(events: Annotated[list[IsolateEvent], Body(min_length=1, max_length=500)]):
        return append(events)

    @app.get("/isolates/{isolate_id}/history", response_model=list[IsolateEvent])
    def history(isolate_id: str):
        events = store.events(isolate_id)
        if not events:
            raise HTTPException(status_code=404, detail="Isolate not found")
        return events

    @app.get("/statistics", response_model=AnalysisResult)
    def statistics(query: Annotated[AnalysisQuery, Query()]):
        return calculation.calculate(store.events(), query)

    @app.get("/statistics/comparison", response_model=ComparisonResult)
    def comparison(query: Annotated[AnalysisQuery, Query()]):
        try:
            return compare_periods(calculation, store.events(), query)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error

    @app.get("/api/v1/evidence", response_model=EvidenceReport)
    def evidence(query: Annotated[EvidenceQuery, Query()]):
        if not hasattr(store, "evidence_snapshot"):
            raise HTTPException(status_code=503, detail={"code": "evidence_unavailable",
                "message": "Enriched evidence requires a PostgreSQL dataset import"})
        try:
            return calculate_evidence(store.evidence_snapshot(), query)
        except EvidenceUnavailable as error:
            raise HTTPException(status_code=503, detail={"code": "evidence_unavailable", "message": str(error)}) from error
        except EvidenceQueryError as error:
            raise HTTPException(status_code=422, detail={"code": "invalid_filter", "field": error.field,
                                                         "message": str(error)}) from error

    return app
