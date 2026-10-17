from datetime import date, datetime, timezone
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Susceptibility(StrictModel):
    antibiotic_code: str = Field(min_length=1)
    result: Literal["S", "R"]
    guideline: str = Field(min_length=1)


class IsolateEvent(StrictModel):
    """A complete isolate snapshot; a correction appends a higher revision."""

    schema_version: Literal[1] = 1
    event_type: Literal["isolate.upsert"] = "isolate.upsert"
    event_id: str = Field(min_length=1)
    isolate_id: str = Field(min_length=1)
    revision: int = Field(ge=1)
    patient_id: str = Field(min_length=1)
    culture_id: str = Field(min_length=1)
    sample_date: date
    received_at: AwareDatetime
    ward_code: str = Field(min_length=1)
    organism: str = Field(min_length=1)
    specimen: Literal["urine", "blood", "wound"]
    tests: tuple[Susceptibility, ...]

    @model_validator(mode="after")
    def unique_antibiotics(self):
        codes = [test.antibiotic_code for test in self.tests]
        if len(codes) != len(set(codes)):
            raise ValueError("An isolate revision must have at most one result per antibiotic")
        return self

    def canonical_json(self) -> str:
        import json

        payload = self.model_dump(mode="json")
        payload["received_at"] = self.received_at.astimezone(timezone.utc).isoformat()
        payload["tests"] = sorted(payload["tests"], key=lambda test: test["antibiotic_code"])
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))


class AnalysisQuery(StrictModel):
    as_of: date
    start_date: date | None = None
    window_days: int = Field(default=28, ge=1, le=3660)
    known_at: AwareDatetime | None = None
    ward_code: str | None = None
    organism: str | None = None
    specimen: Literal["urine", "blood", "wound"] | None = None
    antibiotic_code: str | None = None
    dedup_scope: Literal["selected_cohort", "all_wards"] = "selected_cohort"

    @model_validator(mode="after")
    def valid_period(self):
        if self.start_date is not None and self.start_date > self.as_of:
            raise ValueError("start_date cannot be later than as_of")
        if self.start_date is None and (self.as_of - date.min).days < self.window_days - 1:
            raise ValueError("window_days extends before the supported date range")
        return self


class Statistic(StrictModel):
    ward_code: str
    organism: str
    specimen: str
    antibiotic_code: str
    eligible_isolates: int
    tested: int
    susceptible: int
    resistant: int
    untested: int
    susceptibility_percent: float | None
    resistance_percent: float | None
    status: Literal["available", "insufficient_data"]


class AnalysisResult(StrictModel):
    rules_version: Literal["prototype-v1"] = "prototype-v1"
    minimum_tested: Literal[30] = 30
    start_date: date
    end_date: date
    query: AnalysisQuery
    latest_received_at: datetime | None
    rows: tuple[Statistic, ...]
