"""Snapshot-bound summary contracts. Model output contains references, not new claims."""

from datetime import date
from typing import Literal

from pydantic import AwareDatetime, Field

from .models import StrictModel

FACT_VERSION = "summary-facts-v1"
PROMPT_VERSION = "summary-prompt-v1"
OUTPUT_VERSION = "summary-output-v1"


class SummaryRequest(StrictModel):
    analysis_id: str = Field(min_length=1, max_length=100)
    language: Literal["en"] = "en"


class SummaryFact(StrictModel):
    fact_id: str
    section: Literal["paragraph", "support", "limitation"]
    variants: tuple[str, ...]
    paths: tuple[str, ...]


class SentenceChoice(StrictModel):
    fact_id: str = Field(min_length=1, max_length=100)
    variant: int = Field(ge=0, le=1, strict=True)


class SummaryDraft(StrictModel):
    sentences: tuple[SentenceChoice, ...] = Field(min_length=1, max_length=12)


class SummaryStatement(StrictModel):
    fact_id: str
    text: str
    paths: tuple[str, ...]


class SavedSummary(StrictModel):
    summary_id: str
    analysis_id: str
    run_id: str
    state_revision: int
    method_version: str
    as_of: date
    language: Literal["en"] = "en"
    status: Literal["queued", "generating", "ready"]
    output_mode: Literal["ai", "template"] | None = None
    paragraph: str | None = None
    statements: tuple[SummaryStatement, ...] = ()
    limitations: tuple[SummaryStatement, ...] = ()
    supporting_facts: tuple[SummaryStatement, ...] = ()
    created_at: AwareDatetime
    generated_at: AwareDatetime | None = None
    provider: str
    model: str | None = None
    fact_version: str = FACT_VERSION
    prompt_version: str = PROMPT_VERSION
    output_version: str = OUTPUT_VERSION
    validation: Literal["pending", "approved_sentences", "template"] = "pending"
    fallback_reason: Literal["not_configured", "disabled", "provider_unavailable",
        "invalid_output", "interrupted", "configuration_changed"] | None = None
