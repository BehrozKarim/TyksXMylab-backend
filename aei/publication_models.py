"""HTTP and persisted contracts for a published replay run."""

from datetime import date
from typing import Literal

from pydantic import AwareDatetime, Field, model_validator

from .dataset_models import FileSummary
from .evidence_models import EvidenceContext, EvidenceQuery, EvidenceWindows
from .models import StrictModel


class ExpectedState(StrictModel):
    expected_run_id: str | None = Field(default=None, min_length=1)
    expected_revision: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def paired_expectations(self):
        if (self.expected_run_id is None) != (self.expected_revision is None):
            raise ValueError("expected_run_id and expected_revision must be supplied together")
        return self


class EvidenceRequest(EvidenceQuery, ExpectedState):
    as_of: date | None = None

    def resolved(self, demo_date: date) -> EvidenceQuery:
        values = self.model_dump(include=set(EvidenceQuery.model_fields))
        values["as_of"] = self.as_of or demo_date
        return EvidenceQuery(**values)


class ReplayStart(ExpectedState):
    interval_seconds: float = Field(default=1.0, ge=0.05, le=60, allow_inf_nan=False)


class ReplayReset(ExpectedState):
    seed_through: date | None = None
    replay_through: date | None = None


class ReplayState(StrictModel):
    run_id: str
    state_revision: int = Field(ge=1)
    status: Literal["paused", "running", "completed", "error"]
    demo_date: date
    seed_through: date
    replay_through: date
    latest_visible_sample_date: date | None
    published_cultures: int
    published_isolates: int
    source_manifest_id: str
    source_kind: Literal["zip", "directory"]
    interval_seconds: float = 1.0
    created_at: AwareDatetime
    updated_at: AwareDatetime
    last_error: str | None = None
    legacy_event_ingestion: Literal["history_only"] = "history_only"


class PublishedContext(EvidenceContext):
    run_id: str
    state_revision: int
    demo_date: date
    published_cultures: int
    published_isolates: int
    latest_visible_sample_date: date | None
    source_kind: Literal["zip", "directory"]
    source_files: dict[str, FileSummary]
    importer_version: str
    visibility: Literal["published_culture_bundles"] = "published_culture_bundles"


class SavedEvidence(StrictModel):
    analysis_id: str
    context: PublishedContext
    windows: EvidenceWindows


class ServiceError(Exception):
    def __init__(self, status: int, code: str, message: str, details=None):
        super().__init__(message)
        self.status, self.code, self.details = status, code, details or {}
