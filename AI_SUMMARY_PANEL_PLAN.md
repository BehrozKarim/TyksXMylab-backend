# AI summary panel: proposed development plan

Status: planning only, 2026-10-17. No AI feature has been implemented by this plan.

## 1. Purpose and first release

Help a clinician understand the selected laboratory susceptibility report: what
population it describes, what the results show, and what limits interpretation.
The first release is an on-demand summary panel on the ward evidence page.

Start with one saved evidence analysis containing the rolling 28-day and rolling
12-month windows. Add alert investigation and paired antibiotic comparison summaries
after this first release is evaluated. Open chat, patient-history summarization,
prescribing advice and external clinical knowledge retrieval are outside this release.

Success means faster, accurate understanding with the important limitations retained.
AI wording must be compared with a simple template summary; its benefit is not assumed.

## 2. Doctor-facing experience

Place the panel beside or immediately below the existing evidence cards. Keep those
cards and their counts accessible while the summary loads.

1. The doctor selects the ward, organism, specimen, antibiotic and other filters.
2. The existing evidence request returns a saved report and its `analysis_id`.
3. The doctor selects **Summarize this report**.
4. The panel displays three to five short statements, with expandable supporting facts.
5. **View evidence** opens the relevant counts, dates or provenance for that report.

The panel contains:

- Scope: selected group, antibiotic, report cutoff and source-data label.
- Findings: separate descriptions of the two windows, with tested denominators.
- Limitations: insufficient counts, missing tests and partial coverage when applicable.
- Provenance: links to supporting fields in the saved report; generated time and report version.
- Feedback: useful, missing information or confusing. Free-text feedback is optional later.

Target approximately 80-120 words in English initially. Mandatory limitations remain
visible even when keeping them requires a longer panel. Label AI-generated wording
and the basic template fallback distinctly. Do not display an invented confidence score.

Panel states: idle, generating, ready, basic-summary fallback, unavailable and older
report. Loading is local to the panel. Display only complete, validated output; do not
stream unchecked model text into the clinical view.

## 3. Rules that remain authoritative

- Existing backend calculations determine first-isolate eligibility, tested counts,
  percentages, intervals and status. The model performs no statistical calculation.
- Fewer than 30 eligible tested isolates means **insufficient data**. Never show a
  suppressed percentage or infer it from counts. Other statuses, including no tests,
  empty cohort and not applicable, retain their distinct explanations.
- The rolling 12-month overview includes the recent 28 days. Describe these windows
  separately; do not present them as independent baseline and recent populations or
  assert a temporal trend from their difference.
- Carry exact date bounds and their inclusive/exclusive meaning from the backend.
  Report cutoff and generation time are different concepts.
- Preserve partial source coverage, missing-test and available uncertainty information.
  Meeting the reporting minimum does not establish high certainty.
- Describe laboratory susceptibility, without claiming treatment efficacy, selecting
  therapy or diagnosing an outbreak. No external medical assertions enter this release.
- Derive all mandatory warnings from backend rules, independently of model wording.

## 4. Backend design

Proposed flow:

```text
Saved evidence analysis
  -> allowlisted aggregate facts and required caveats
  -> basic template summary
  -> bounded AI generation
  -> output validation
  -> saved AI summary, or labeled basic fallback
  -> dashboard panel with evidence links
```

Use `PublicationService.analysis(analysis_id)` to load the immutable report. That read
does not acquire the publication lock. Close the database transaction before calling
the AI provider; keep model work outside publication and feed workers.

Build the model input on the server, from the saved analysis rather than client-supplied
statistics. Accept evidence analyses only in the first release; reject patient history
and unsupported analysis kinds. Send aggregate facts, not patient IDs, culture IDs,
raw records or arbitrary user prompts. For suppressed windows, omit S/R breakdowns
and suppressed statistical values from model input; retain status and tested count.

Use structured output containing short statements with supporting fact IDs. The server
resolves those IDs into evidence links. Prefer backend-rendered quantitative sentences;
the model may organize the facts and supply tightly constrained explanatory wording.
Require support for each claim, correct terminology and all mandatory limitations.
Treat dataset labels as data, never as instructions.

Schema, reference and numeric checks do not prove free-text correctness. Evaluate
omissions and meaning separately using reviewed cases. Reject unsupported output;
return the basic summary when generation fails, times out or fails validation.

Proposed API, additive to the current routes:

| Route | Behavior |
|---|---|
| `POST /api/v1/ai/summaries` with `analysis_id` and `language` | Return an existing summary, or accept generation with HTTP 202. |
| `GET /api/v1/ai/summaries/{summary_id}` | Return generation state and the completed validated result or basic fallback. |

Store summary jobs/results in PostgreSQL. A bounded, separate background worker claims
jobs using short transactions. Use a unique cache key covering analysis ID, language,
fact-extraction version, prompt version, output schema version and model configuration.
Concurrent requests for the same key reuse one job. Define timeout, bounded retries and
recovery of interrupted jobs. No new message broker is needed for this prototype.

The response includes summary ID, analysis ID, run ID, revision, analysis kind,
generation state, output mode (`ai` or `template`), statements, required limitations,
supporting references and timestamps. Record model/prompt versions and validation
outcome for reproducibility. Keep provider keys on the backend and avoid raw-data logs.

Generation is on demand and has bounded concurrency/output length. The dashboard
continues to work when AI is disabled or unavailable. Provider/model selection remains
open until a small comparison on our synthetic cases establishes accuracy, latency and cost.

## 5. Live updates and frontend handoff

Bind the summary to the exact displayed `analysis_id`, not just the selected ward.
On a filter change or replacement report, detach the previous summary and discard late
responses for it. A historical summary may be opened only with its matching report.

When a new publication is observed, mark the displayed report/summary as older until
the refreshed report is available. Offer **Summarize updated report** for the replacement.
Do not generate a new AI summary for every SSE event or replay tick.

The frontend can use its existing query layer to poll a pending summary job briefly,
stopping at completion or timeout. SSE continues to notify data publication; token
streaming and new AI-specific SSE events are unnecessary for the first release.

Suggested integration points:

- Backend: new `aei/summary_models.py`, `aei/summary_service.py`, provider adapter and
  summary persistence; additive routes in `aei/api_v1.py` and separate worker lifecycle.
- Frontend: new `src/features/AiSummaryPanel.tsx`, API contracts/client methods and
  a query keyed by analysis ID, placed near `EvidenceCards` in `src/app/App.tsx`.
- Reuse the existing provenance interaction for supporting evidence. Coordinate the
  component contract with the colleague working on the dashboard before UI implementation.

## 6. Delivery order and complexity

| Step | Deliverable | Complexity |
|---|---|---|
| 1 | Fact schema, reviewed synthetic examples, basic template and proposed API contract | Low-medium |
| 2 | Provider adapter, structured generation, validation, saved results and bounded jobs | Medium |
| 3 | Panel integration, source links, fallback and report-change handling | Medium |
| 4 | Automated edge-case evaluation and clinician usability review | High importance; semantic accuracy is the hardest part |
| Later | Alert and paired-comparison summaries using the same panel contract | Medium, with additional domain rules |

For alert summaries, use the saved alert analysis with its non-overlapping baseline:
12 calendar months immediately before the recent 28 days. Carry configured threshold,
counts, rule version and proposed/unconfirmed status. A saved alert evaluation does not
by itself establish the full new/ongoing/cleared lifecycle; include that only from a
matching frozen alert record or investigation. Keep original and current snapshots distinct.

For comparison summaries, use the existing paired report and its common tested
denominator, never a comparison of independently tested groups. Neither an observed
difference nor combined laboratory coverage establishes a preferred treatment.

Guided questions and patient-history summaries require separate scope decisions later.

## 7. Verification and acceptance

Cover these meaningful cases before enabling the panel:

- Exactly 29 versus 30 tested isolates; a suppressed window must never reveal a percentage.
- Empty cohort, no tests, not applicable, missing tests and partial source coverage.
- Both overview windows available, with no incorrect independent-baseline or trend claim.
- Correct values, dates, cohort labels, uncertainty and source references.
- Provider failure, malformed output, invented references, unsupported claims and omissions.
- Duplicate requests, worker interruption and provider timeout without publication blocking.
- Filters or published data change while generation is in flight; old output never attaches
  to a replacement report.

Compare dashboard alone, template summary and AI summary on a small reviewed set of
synthetic scenarios. Include 3-5 clinicians in an initial usability study if available.
Measure interpretation accuracy, missed limitations, time to answer and ease of checking
the source. Record generation latency and cost. A small pilot informs design; it does
not establish clinical readiness. Any known incorrect value, suppression leak or unsupported
clinical claim blocks release of that output. If AI adds no useful improvement over the
template, retain the template while revising the design.

## 8. Decisions still needed

| Decision | Working assumption for planning |
|---|---|
| Initial audience and language | Ward-level surveillance users; English matches the current UI. Confirm with Tyks before broader rollout. |
| Provider, hosting and acceptable data handling | Prototype evaluation uses synthetic aggregate reports. Resolve hosting/retention before processing real clinical data. |
| Latency and spending budget | On-demand generation, caching and bounded concurrency; measure candidates before choosing limits. |
| Clinical wording and usefulness | Review examples with a Tyks clinician; validate that this task matters in their workflow. |

These decisions do not prevent preparing the contract, templates and synthetic evaluation
cases. Provider-dependent implementation follows provider selection.

## 9. Research informing the proposal

The studies below concern chart review and clinical AI interaction, rather than this
specific susceptibility dashboard. Their findings motivate a design to test locally.

- [Physician input approaches to AI chatbots](https://www.nature.com/articles/s41746-025-02184-y):
  interviews found varied interaction preferences and burdens from prompting or broad
  responses. Provide a useful summary without requiring a typed prompt.
- [EHR-integrated AI chart review evaluation](https://www.nature.com/articles/s44401-025-00064-x):
  clinician feedback frequently identified missing information. Preserve required
  limitations separately from generated prose and provide supporting evidence.
- [Ambulatory encounter-summary pilot](https://pmc.ncbi.nlm.nih.gov/articles/PMC12403210/):
  feedback included both excessive length and missing details, as well as latency concerns;
  overall chart-review time reduction was not statistically significant. Use layered detail
  and evaluate benefit rather than assuming a time saving.
