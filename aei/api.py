from typing import Annotated

from fastapi import Body, FastAPI, HTTPException, Query

from .api_v1 import install_v1, replay_lifespan
from .hosted_demo import install_hosted_demo
from .config import database_schema, database_url as configured_database_url
from .models import AnalysisQuery, AnalysisResult, ComparisonResult, IsolateEvent
from .postgres import PostgresEventStore
from .processor import Processor, ReferenceProcessor, compare_periods
from .publication import PublicationService
from .store import EventConflict, EventStore
from .summary_service import SummaryService


def create_app(database_url: str | None = None, processor: Processor | None = None,
               *, store: EventStore | None = None, summary_settings=None, summary_provider=None) -> FastAPI:
    store = store if store is not None else PostgresEventStore(database_url or configured_database_url(), database_schema())
    calculation = processor or ReferenceProcessor()
    publication = PublicationService(store) if isinstance(store, PostgresEventStore) else None
    summaries = SummaryService(publication, summary_settings, summary_provider) if publication else None
    app = FastAPI(
        title="Tyks AEI reference pipeline",
        version="0.7.0",
        lifespan=replay_lifespan(publication, summaries),
        description=("PostgreSQL event history and reference calculations for rolling 28-day and "
                     "12-month reports, versioned resistance alerts, durable external feeds and SSE updates. "
                     "Flink and a dashboard are not active yet."),
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

    install_v1(app, publication, summaries)
    install_hosted_demo(app, publication)
    return app
