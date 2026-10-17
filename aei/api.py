import os
from pathlib import Path
from typing import Annotated

from fastapi import Body, FastAPI, HTTPException, Query

from .models import AnalysisQuery, AnalysisResult, IsolateEvent
from .processor import Processor, ReferenceProcessor
from .store import EventConflict, SQLiteEventStore


DEFAULT_DB = Path(__file__).resolve().parents[1] / ".data" / "aei.sqlite3"


def create_app(db_path: str | Path | None = None, processor: Processor | None = None) -> FastAPI:
    store = SQLiteEventStore(db_path or os.environ.get("AEI_DB_PATH", str(DEFAULT_DB)))
    calculation = processor or ReferenceProcessor()
    app = FastAPI(
        title="Tyks AEI reference pipeline",
        version="0.1.0",
        description=("Local validation prototype. Explicit analysis dates, append-only isolate history, "
                     "and reference calculations. Flink, alerts and a dashboard are not active yet."),
    )

    @app.get("/health")
    def health():
        return {"status": "ok", "storage": "sqlite", "processor": "reference",
                "events": store.count(), "rules_version": "prototype-v1"}

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

    return app
