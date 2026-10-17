# Saved-report summary panel

The ward overview now offers **Summarize this report**. A request identifies one
saved evidence analysis; the backend loads the report and prepares aggregate facts.
The response stays attached to that analysis ID, run, revision and report date.
New publications do not automatically call the model. The UI discards results for
a replaced report and offers **Summarize updated report** on the replacement.

The visible output is one paragraph, normally 80-120 words, plus applicable
limitations and expandable supporting facts. A longer paragraph is allowed when
required scope/status/coverage facts need the space. The paragraph describes the
selected cohort, both exact reporting windows, counts, available susceptibility,
first-isolate counting, overlapping periods and limits on clinical interpretation.
It does not add a data-origin label. Existing application provenance is unchanged.

## Configuration

The adapter uses `httpx` and OpenAI's Responses API. Install `requirements.txt`
after updating; `python-dotenv` loads the local summary configuration.
Copy `.env.example` to `.env` in this backend directory if `.env` does not exist,
then add your key:

```dotenv
OPENAI_API_KEY=
AEI_SUMMARY_PROVIDER=openai
AEI_SUMMARY_MODEL=gpt-4o-mini
```

The backend summary settings read this exact file regardless of the working
directory. `.env` is ignored by Git. Values are read without variable interpolation
or exporting them into the process environment. Existing process environment
variables take precedence, so remove old overrides if you want to use the file.
Alternatively, set configuration in the backend's process environment:

```powershell
$env:AEI_SUMMARY_PROVIDER = 'openai'
$env:AEI_SUMMARY_MODEL = '<your model supporting Responses structured outputs>'
# Set OPENAI_API_KEY securely in this environment; never put it in frontend variables.
```

Then launch the backend normally or run `scripts/start-demo.ps1`. The launcher
passes OpenAI configuration to the backend and strips `OPENAI_*` and `AEI_*` from
the frontend child environment. Restart the backend after configuration changes.
Only the summary settings read `.env`; database/feed settings still use the
process environment.

| Variable | Default | Meaning |
|---|---|---|
| `AEI_SUMMARY_PROVIDER` | `openai` | `openai` or `none` to use basic summaries only. |
| `AEI_SUMMARY_MODEL` | Unset | Model ID available in your account; no silent model substitution. |
| `OPENAI_API_KEY` | Unset | Backend-only credential. |
| `AEI_SUMMARY_TIMEOUT_SECONDS` | `20` | Overall generation deadline per attempt; 1-60 seconds. |
| `AEI_SUMMARY_MAX_OUTPUT_TOKENS` | `1024` | Output cap; 256-4096 tokens. |

Missing configuration immediately returns a labelled **Basic summary**. It does
not claim that AI ran. With a configured provider, accepted output is labelled
**AI-assisted summary**. Invalid/incomplete output or unavailable generation also
uses the basic fallback. Model quality and live account access must be evaluated
with the configured model; mocked API tests do not establish either.

## Prompt and output control

`aei/summary_prompt.py` follows **Scenario, Task, Reporting rules, Requirements,
Guardrails, Output**, adapted from the requested prompt structure.

This first version uses constrained composition. The backend produces approved
sentence variants from the saved facts; the model chooses phrasing and order using
`{fact_id, variant}` references. It does not produce unchecked medical prose.
The server requires every paragraph fact exactly once, scope first and interpretation
last, then resolves each choice to its exact sentence. Mandatory limitations are
always rendered independently of the model. Unsupported text, missing/duplicated
facts, invalid references, refusals and incomplete responses produce a fallback.

This deliberately limits how much the model can add: it can organize and choose
phrasing, but cannot introduce additional interpretations. Evaluate whether these
choices improve readability compared with the template before expanding its freedom.

Suppressed windows expose status and tested/eligible counts, never S/R breakdowns,
suppressed percentages or confidence intervals to the model. The fact builder reads
no patient/culture history. Available percentages and intervals come from the report;
the model does no counting or statistical calculations. No chat, web retrieval,
prescribing advice, patient summaries or alert summaries are implemented here.

The adapter uses strict JSON-schema output and `store: false` as documented in
[Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs?api-mode=responses)
and the [Responses guide](https://developers.openai.com/api/docs/guides/migrate-to-responses).
This setting is not a claim of zero retention under every provider policy.

## API and persistence

- `POST /api/v1/ai/summaries` with `{"analysis_id":"...","language":"en"}` returns
  HTTP 202 for queued/generating work, or HTTP 200 for a completed/cached summary.
- `GET /api/v1/ai/summaries/{summary_id}` returns `queued`, `generating` or `ready`.
- Only saved evidence analyses are accepted. Unknown IDs return 404; unsupported
  analysis kinds, extra client facts/prompts and unsupported languages return 422.
- A completed result includes paragraph, resolved statements and source-field paths,
  mandatory limitations, supporting facts, timestamps and configuration versions.

`summary_jobs` is an additive PostgreSQL table created during normal store
initialization. Concurrent identical requests share a job. The cache covers analysis,
language, provider/model settings, fact/output versions and prompt content/version.
Completed results, including fallbacks, are reused for that key; there is no automatic
regeneration or retry of an already completed fallback. A new report or configuration
produces a new key while older summaries remain readable.

A separate worker processes one model call at a time per backend process, without
holding the publication lock or a database transaction during the provider call.
Database leases and owner tokens protect recovery and stale-worker writes. Transient
errors/timeouts retry once after two seconds; permanent errors fall back immediately.
An expired lease is reclaimable, with at most two provider attempts per job. Changed
configuration resolves old pending jobs to a fallback instead of using a different
model under the original cache key. The queue admits at most 32 unfinished jobs;
additional unique requests receive HTTP 429. This is a prototype queue bound, not
per-user authentication or a monetary quota.

The panel polls pending work once per second for up to two minutes, then offers
**Check status**. Completed summaries stop polling. Generation failures remain local
to the panel; the evidence, feed and exports continue to work.

## Verification

`tests/test_summary.py` covers facts, suppression at 29/30, interval formatting,
coverage, missing testing, invalid model output, the Responses HTTP contract, API
validation, duplicate requests, leases, retries, caching and publication independence.
Frontend tests cover explicit generation, status polling, fallback labels, evidence
links, mismatched snapshots and late results after report changes.

Run backend tests with `AEI_TEST_DATABASE_URL` set to a disposable-test-capable
PostgreSQL instance and `python -m unittest discover -s tests -p test_summary.py -v`.
Run frontend `npm.cmd test`, `npm.cmd run build` and `npm.cmd run lint` normally.
