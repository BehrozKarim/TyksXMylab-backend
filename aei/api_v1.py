"""Published evidence API, replay lifecycle and consistent versioned errors."""

import asyncio
import logging
import os
import secrets
from contextlib import asynccontextmanager
from typing import Annotated, Literal
from uuid import uuid4

import psycopg
from fastapi import APIRouter, Body, Depends, Header, Query, Request
from fastapi.exception_handlers import http_exception_handler, request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import ValidationError
from starlette.exceptions import HTTPException

from .evidence import EvidenceQueryError
from .alert_models import AlertList, AlertListRequest, Investigation
from .analysis_models import (ComparisonRequest, HistoryRequest, PatientSearchRequest,
                              PatientSearchResult, SavedAnalysis, SavedComparison, SavedHistory)
from .evidence_source import EvidenceUnavailable
from .feed_models import FeedBatch, FeedReceipt, FeedStart
from .publication import export_csv
from .publication_models import (
    EvidenceRequest, ExpectedState, ReplayReset, ReplayStart, ReplayState, SavedEvidence, ServiceError,
)
from .summary_models import SavedSummary, SummaryRequest


def replay_lifespan(service, summaries=None):
    @asynccontextmanager
    async def lifespan(app):
        stopped = asyncio.Event()

        async def run(tick):
            while not stopped.is_set():
                try:
                    await asyncio.to_thread(tick)
                except Exception:
                    logging.getLogger(__name__).exception("Background publication worker could not access state")
                try:
                    await asyncio.wait_for(stopped.wait(), timeout=0.2)
                except asyncio.TimeoutError:
                    pass

        tasks = ([asyncio.create_task(run(tick)) for tick in (service.worker_tick, service.feed.worker_tick)]
                 if service is not None else [])
        if summaries is not None:
            tasks.append(asyncio.create_task(summaries.run(stopped)))
        try:
            yield
        finally:
            stopped.set()
            if tasks:
                await asyncio.gather(*tasks)  # Finish in-flight database transactions.
    return lifespan


def install_v1(app, service, summaries=None):
    origins = [value.strip() for value in os.environ.get(
        "AEI_CORS_ORIGINS", "http://localhost:3000,http://127.0.0.1:3000").split(",") if value.strip()]
    app.add_middleware(CORSMiddleware, allow_origins=origins, allow_methods=["GET", "POST"],
                       allow_headers=["Content-Type", "Authorization", "Last-Event-ID"],
                       expose_headers=["X-Request-ID", "Content-Disposition"])

    @app.middleware("http")
    async def identify(request, call_next):
        request.state.request_id = str(uuid4())
        response = await call_next(request)
        response.headers["X-Request-ID"] = request.state.request_id
        return response

    def error_response(request, status, code, message, details=None):
        request_id = getattr(request.state, "request_id", str(uuid4()))
        return JSONResponse(status_code=status, content={"error": {
            "code": code, "message": message, "details": details or {}}, "request_id": request_id},
            headers={"X-Request-ID": request_id})

    @app.exception_handler(ServiceError)
    async def service_error(request, error):
        return error_response(request, error.status, error.code, str(error), error.details)

    @app.exception_handler(EvidenceUnavailable)
    async def unavailable(request, error):
        return error_response(request, 503, "evidence_unavailable", str(error))

    @app.exception_handler(EvidenceQueryError)
    async def filter_error(request, error):
        return error_response(request, 422, "invalid_filter", str(error), {"field": error.field})

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, error):
        if not request.url.path.startswith("/api/v1/"):
            return await request_validation_exception_handler(request, error)
        issues = [{"location": list(item["loc"]), "message": item["msg"], "type": item["type"]}
                  for item in error.errors()]
        return error_response(request, 422, "invalid_request", "Request validation failed", {"issues": issues})

    @app.exception_handler(ValidationError)
    async def invalid_resolved_query(request, error):
        if not request.url.path.startswith("/api/v1/"):
            raise error
        return error_response(request, 422, "invalid_request", "Resolved request is invalid",
                              {"issues": error.errors(include_context=False, include_input=False, include_url=False)})

    @app.exception_handler(HTTPException)
    async def http_error(request, error):
        if not request.url.path.startswith("/api/v1/"):
            return await http_exception_handler(request, error)
        return error_response(request, error.status_code, "http_error", str(error.detail))

    @app.exception_handler(Exception)
    async def unexpected(request, error):
        if not request.url.path.startswith("/api/v1/"):
            return Response("Internal Server Error", status_code=500)
        logging.getLogger(__name__).error("Versioned API request failed", exc_info=error)
        if isinstance(error, psycopg.OperationalError):
            return error_response(request, 503, "database_unavailable", "Database temporarily unavailable")
        return error_response(request, 500, "internal_error", "The request could not be completed")

    def backend():
        if service is None:
            raise EvidenceUnavailable("Published evidence requires a PostgreSQL dataset import")
        return service

    router = APIRouter(prefix="/api/v1")

    @router.post("/ai/summaries", response_model=SavedSummary, status_code=202)
    def create_summary(request: SummaryRequest, response: Response):
        backend()
        if summaries is None:
            raise ServiceError(503, "summary_unavailable", "Summary service is unavailable")
        result = summaries.request(request)
        response.status_code = 200 if result.status == "ready" else 202
        return result

    @router.get("/ai/summaries/{summary_id}", response_model=SavedSummary)
    def get_summary(summary_id: str):
        backend()
        if summaries is None:
            raise ServiceError(503, "summary_unavailable", "Summary service is unavailable")
        return summaries.get(summary_id)

    def feed_access(authorization: Annotated[str | None, Header()] = None):
        token = os.environ.get("AEI_FEED_TOKEN")
        if token and not secrets.compare_digest((authorization or "").encode(), f"Bearer {token}".encode()):
            raise ServiceError(401, "feed_unauthorized", "A valid feed bearer token is required")

    @router.post("/feed/start", response_model=ReplayState, dependencies=[Depends(feed_access)])
    def feed_start(request: FeedStart):
        return backend().feed.start(request)

    @router.post("/feed/batches", response_model=FeedReceipt, status_code=202, dependencies=[Depends(feed_access)])
    def feed_batch(request: FeedBatch):
        return backend().feed.submit(request)

    @router.get("/feed/receipts/{receipt_id}", response_model=FeedReceipt, dependencies=[Depends(feed_access)])
    def feed_receipt(receipt_id: str):
        return backend().feed.receipt(receipt_id)

    @router.post("/feed/receipts/{receipt_id}/retry", response_model=FeedReceipt, dependencies=[Depends(feed_access)])
    def retry_feed(receipt_id: str):
        return backend().feed.retry(receipt_id)

    @router.get("/updates", response_class=StreamingResponse)
    async def updates(request: Request, after: Annotated[int | None, Query(ge=0)] = None,
                      last_event_id: Annotated[str | None, Header()] = None):
        if last_event_id is not None:
            if not last_event_id.isascii() or not last_event_id.isdecimal() or len(last_event_id) > 19:
                raise ServiceError(422, "invalid_event_cursor", "Last-Event-ID must be a nonnegative integer")
            after = int(last_event_id)  # Browser reconnect cursor overrides an initial query cursor.
        stream = backend().updates
        cursor, initial = await asyncio.to_thread(stream.begin, after)
        return StreamingResponse(stream.stream(request, cursor, initial), media_type="text/event-stream",
                                 headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

    @router.get("/meta")
    def meta():
        return backend().meta()

    @router.get("/state", response_model=ReplayState)
    def state():
        return backend().state()

    @router.get("/evidence", response_model=SavedEvidence, response_model_exclude_unset=True)
    def evidence(query: Annotated[EvidenceRequest, Query()]):
        return backend().evidence(query)

    @router.get("/comparisons", response_model=SavedComparison, response_model_exclude_unset=True)
    def comparisons(query: Annotated[ComparisonRequest, Query()]):
        return backend().comparison(query)

    @router.get("/patients", response_model=PatientSearchResult)
    def patients(query: Annotated[PatientSearchRequest, Query()]):
        return backend().patients(query)

    @router.get("/alerts", response_model=AlertList)
    def alerts(query: Annotated[AlertListRequest, Query()]):
        return backend().alerts(query)

    @router.get("/investigations/{investigation_id}", response_model=Investigation)
    def investigation(investigation_id: str, query: Annotated[ExpectedState, Query()]):
        return backend().investigation(investigation_id, query)

    @router.get("/patients/{patient_id}/history", response_model=SavedHistory, response_model_exclude_unset=True)
    def history(patient_id: str, query: Annotated[HistoryRequest, Query()]):
        return backend().patient_history(patient_id, query)

    @router.get("/analyses/{analysis_id}", response_model=SavedAnalysis, response_model_exclude_unset=True)
    def analysis(analysis_id: str):
        return backend().analysis(analysis_id)

    @router.get("/analyses/{analysis_id}/export")
    def export(analysis_id: str, format: Literal["json", "csv"] = "json"):
        report = backend().analysis(analysis_id)
        headers = {"Content-Disposition": f'attachment; filename="analysis-{report.analysis_id}.{format}"'}
        if format == "csv":
            return Response(export_csv(report), media_type="text/csv", headers=headers)
        return Response(report.model_dump_json(exclude_unset=True), media_type="application/json", headers=headers)

    @router.post("/replay/reset", response_model=ReplayState)
    def reset(request: Annotated[ReplayReset | None, Body()] = None):
        return backend().reset(request or ReplayReset())

    @router.post("/replay/start", response_model=ReplayState)
    def start(request: Annotated[ReplayStart | None, Body()] = None):
        return backend().start(request or ReplayStart())

    @router.post("/replay/pause", response_model=ReplayState)
    def pause(request: Annotated[ExpectedState | None, Body()] = None):
        return backend().pause(request or ExpectedState())

    @router.post("/replay/step", response_model=ReplayState)
    def step(request: Annotated[ExpectedState | None, Body()] = None):
        return backend().step(request or ExpectedState())

    app.include_router(router)
