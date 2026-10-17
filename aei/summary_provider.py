"""Configurable OpenAI Responses adapter. Credentials never enter jobs or UI data."""

import hashlib
import json
import math
import os
from dataclasses import dataclass, field
from pathlib import Path

import httpx
from dotenv import dotenv_values

from .summary_models import FACT_VERSION, OUTPUT_VERSION, PROMPT_VERSION
from .summary_prompt import PROMPT

SUMMARY_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


@dataclass(frozen=True)
class SummarySettings:
    provider: str = "openai"
    model: str | None = None
    api_key: str | None = field(default=None, repr=False)
    timeout: float = 20
    max_output_tokens: int = 1024

    def __post_init__(self):
        if self.provider not in ("openai", "none"):
            raise ValueError("AEI_SUMMARY_PROVIDER must be openai or none")
        if not math.isfinite(self.timeout) or not 1 <= self.timeout <= 60:
            raise ValueError("AEI_SUMMARY_TIMEOUT_SECONDS must be 1-60")
        if not 256 <= self.max_output_tokens <= 4096:
            raise ValueError("AEI_SUMMARY_MAX_OUTPUT_TOKENS must be 256-4096")

    @classmethod
    def from_env(cls):
        # Read only this backend's file, without exporting credentials to child processes.
        # Explicit process environment values take precedence, including empty values.
        values = {**dotenv_values(SUMMARY_ENV_FILE, encoding="utf-8-sig", interpolate=False), **os.environ}
        return cls(provider=values.get("AEI_SUMMARY_PROVIDER") or "openai",
            model=values.get("AEI_SUMMARY_MODEL") or None, api_key=values.get("OPENAI_API_KEY") or None,
            timeout=float(values.get("AEI_SUMMARY_TIMEOUT_SECONDS") or "20"),
            max_output_tokens=int(values.get("AEI_SUMMARY_MAX_OUTPUT_TOKENS") or "1024"))

    @property
    def unavailable_reason(self):
        return "disabled" if self.provider == "none" else "not_configured" if not (self.api_key and self.model) else None

    @property
    def fingerprint(self):
        value = [self.provider, self.model, self.unavailable_reason, self.timeout, self.max_output_tokens,
                 FACT_VERSION, PROMPT_VERSION, OUTPUT_VERSION, PROMPT]
        return hashlib.sha256(json.dumps(value).encode()).hexdigest()


class ProviderUnavailable(Exception):
    def __init__(self, retryable=False):
        super().__init__("Summary provider unavailable")
        self.retryable = retryable


class OpenAISummaryProvider:
    def __init__(self, settings, *, transport=None):
        self.settings, self.transport = settings, transport

    async def generate(self, facts):
        ids = [f.fact_id for f in facts if f.section == "paragraph"]
        schema = {"type": "object", "additionalProperties": False, "required": ["sentences"], "properties": {
            "sentences": {"type": "array", "items": {"type": "object", "additionalProperties": False,
                "required": ["fact_id", "variant"], "properties": {
                    "fact_id": {"type": "string", "enum": ids},
                    "variant": {"type": "integer", "enum": [0, 1]}}}}}}
        payload = {"model": self.settings.model, "instructions": PROMPT,
            "input": json.dumps({"facts": [f.model_dump(mode="json") for f in facts]}, ensure_ascii=False),
            "store": False, "max_output_tokens": self.settings.max_output_tokens,
            "text": {"format": {"type": "json_schema", "name": "evidence_summary", "strict": True, "schema": schema}}}
        try:
            async with httpx.AsyncClient(timeout=self.settings.timeout, transport=self.transport,
                                          trust_env=False, follow_redirects=False) as client:
                response = await client.post("https://api.openai.com/v1/responses", json=payload,
                    headers={"Authorization": f"Bearer {self.settings.api_key}"})
        except httpx.TransportError:
            raise ProviderUnavailable(retryable=True) from None
        if response.status_code != 200:
            raise ProviderUnavailable(retryable=response.status_code == 429 or response.status_code >= 500)
        data = response.json()
        if data.get("status") != "completed":
            raise ValueError("Incomplete provider output")
        texts = []
        for item in data.get("output", []):
            if item.get("type") == "message":
                for part in item.get("content", []):
                    if part.get("type") == "refusal":
                        raise ValueError("Provider refused the requested structure")
                    if part.get("type") == "output_text":
                        texts.append(part.get("text", ""))
        if len(texts) != 1 or len(texts[0]) > 10000:
            raise ValueError("Unexpected provider output")
        return json.loads(texts[0])
