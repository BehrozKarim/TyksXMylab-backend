# Tyks x Mylab backend implementation plan

Status: implementation plan for extending the runnable reference prototype at commit `b54015f` (`Move prototype contents to repository root`). Existing capabilities below describe inspected code; the `/api/v1` contract describes planned additions that the frontend must follow.

Development update: the current backend uses Docker PostgreSQL and `prototype-v2`
with rolling 28-day and rolling 12-month reports. The first importer/storage upgrade
now supports validated ZIP/CSV-folder imports, source manifests, complete cultures
and revision-bound enrichment in one PostgreSQL transaction. The next calculation
upgrade adds `/api/v1/evidence` and CLI `evidence`: both agreed windows, age/sex filters,
combined all-wards evidence, richer statuses, Wilson intervals and source coverage.
Its `evidence-v1` method keeps the existing ward-policy default and date boundaries;
it does not adopt the proposed calendar-year window or global-before-ward default.
See [README](README.md) for the implemented contract and current limitations.
The SQLite, missing-ZIP and `prototype-v1` references below describe the original
planning baseline.

The existing-component API/replay upgrade now also implements `/api/v1/meta`, `/state`,
published `/evidence`, saved analysis retrieval/JSON/CSV export and replay
`start`/`pause`/`step`/`reset`, with uniform versioned errors and configurable CORS.
PostgreSQL source staging is separate from exact run membership; publication, clock
and revision commit atomically. Frozen reports retain run/revision and file hashes.
The first use exposes imported complete bundles at their latest date; reset seeds an
earlier paused run. CLI evidence and replay use this same service. Legacy event writes
remain stored history and do not automatically enter published replay evidence.
This adapts B3, the replay lifecycle and export work to the agreed two periods and
Docker PostgreSQL. The next upgrade now implements `/api/v1/comparisons` (`comparison-v1`),
bounded `/patients` ID search and `/patients/{id}/history` (`history-v1`). Paired
comparisons use the existing cohort policy and require 30 common tested isolates;
history retains every prior published specimen with revision-bound pagination.
Both report types are frozen and exportable. The history range is the prior calendar
anniversary inclusive through the selected cutoff exclusive, as specified below.
The B6 upgrade now implements `/api/v1/alerts`, `/investigations/{id}` and frozen
`alert-v1` analyses/exports. The versioned `resistance-rise-v1` heuristic uses the
non-overlapping baseline in section 9, minimum 30 per period, exact threshold
comparison, run-frozen configurable monitors, stable alert IDs, transition history
and original/current investigation snapshots. Monitor evaluation commits with replay;
low counts never clear an alert. The authored scenario generator demonstrates
detection, insufficient evidence, clearance and recurrence in a separate schema.
Defaults are urinary E. coli / CIP in PHC-S, PHC-N, ED and URO, with 10 percentage
points; these remain proposals pending partner confirmation. The existing
`selected_cohort` default is preserved.

The feed upgrade now implements complete-culture HTTP deliveries, a durable
PostgreSQL inbox, idempotent receipts, background batch publication and SSE live
notifications with reconnect recovery. Feed data, clock, alerts and notification
commit together; failed batches roll back. Runs support simulated and UTC wall clocks,
including quiet-day expiration. Full culture corrections retain history, and a separate
HTTP dataset sender exercises arrivals without rewriting the source. Existing replay
and saved analyses are preserved. See [FEED_INTEGRATION.md](FEED_INTEGRATION.md) for
the contract, constraints, demonstration and frontend handoff. Dashboard wiring,
laboratory-specific access/mapping and production operations remain; Flink is deferred.
The original proposals below are retained for review; README and OpenAPI describe the
implemented route names, response shapes and deliberate differences.

Build a ward antibiotic evidence assistant that updates as synthetic laboratory records arrive. Its main outputs are susceptibility evidence across calendar annual, rolling 12-month and 28-day windows, an A/B laboratory comparison, specimen history, and automatically detected changes that a researcher can investigate.

## 1. Current repository and delivery boundary

The repository already contains a FastAPI application in `aei/api.py`, strict Pydantic event/query models, a `sqlite3` append-only `SQLiteEventStore`, the `Processor` protocol and deterministic `ReferenceProcessor`. `aei/__main__.py` implements import, statistics, schema and day-by-day replay commands. `tests/test_pipeline.py` uses unittest to cover calculation, storage, API and supplied-archive behavior. Preserve and extend this code. No applicable `AGENTS.md` appeared in the workspace ancestors or backend repository during this planning pass.

The current `.gitignore` contains `.data/`, `__pycache__/`, `*.pyc`, `.venv/` and the user's `synthetic_microbiology_dataset/` addition. Preserve these entries and leave every original CSV unchanged. This planning task edits only this plan.

The current requirements pin FastAPI 0.124.4, Pydantic 2.12.5, Uvicorn 0.33.0 and httpx 0.28.1. Reuse those declared dependencies; this plan does not install packages or establish that the current environment has them. The README's PostgreSQL-to-Flink-CDC/Flink-to-materialized-results architecture remains a future integration boundary. Neither a Flink job nor PostgreSQL CDC is implemented.

| Existing capability | Gap to implement for this product |
|---|---|
| `/health`, `/events`, `/events/batch`, `/isolates/{id}/history`, `/statistics` | Add `/api/v1` routes alongside them; keep legacy routes and response semantics. |
| Complete isolate revision events, atomic batches, idempotent retries, conflict rejection and append-only triggers | Preserve these guarantees while adding culture bundles, run visibility and frozen analyses. |
| Latest-revision selection, explicit inclusive date ranges, per-drug tested n and 29/30 suppression | Add named calendar windows, coverage flags, Wilson intervals, richer statuses and paired calculations. |
| Legacy `dedup_scope=selected_cohort` default and optional `all_wards` | Keep legacy behavior; new `surveillance-v1` uses global-before-ward selection and explicit labels. |
| Even without a ward filter, the processor emits separate ward rows | Recompute a retained all-wards set for the new API; never sum deduplicated ward reports. |
| ZIP importer that discards demographics, admission dates, phenotypes and zero-isolate cultures | Add local directory support, metadata enrichment and culture records before exposing those features. |
| CLI replay, including empty dates and declared synthetic receipt times | Reuse its sequencing in HTTP controls; add run/revision state, transactional publication, alerts and stored results. |

The importer currently expects a default ZIP in the parent `tyks` folder and calls `date.fromisoformat`. That ZIP is absent here; the supplied files now live in `synthetic_microbiology_dataset/*.csv` inside this repository and use `DD-MM-YY`. Directory/path and parser work is a prerequisite, not a working current command.

The supplied data contains 10,000 patients, 46,792 cultures, 24,571 isolates and 172,381 susceptibility rows. The sample dates span 2023-10-02 through 2026-09-27. All records describe invented patients.

| Priority | Deliverable |
|---|---|
| P0 | Import and validate all supplied tables; expose one shared cohort engine and the agreed API. |
| P0 | Present all three time windows, correct denominators, suppression, explicit cohort scope and source coverage. |
| P0 | Compare two tested antibiotics on the same retained isolates; display prior collected specimens. |
| P0 | Replay complete culture bundles, poll state, publish revisions and export an exact displayed analysis. |
| P0 | Run one defined heuristic detector and expose an investigation with measured cohort composition. |
| P0 | Support a separately labeled scenario fixture that reliably demonstrates the detector. |
| P1 | Improve exploratory research, additional monitor configurations, performance and presentation exports after P0 works. |

Focus the clinician demo on urinary *Escherichia coli*. Import the full dataset and keep the engine generic, but don't claim a polished workflow for every organism. The backend must require one specimen and one organism for susceptibility and comparison requests; cross-species pooling is outside this contract.

AI recommendations, automated prescriptions, weighted cross-species antibiotic optimization, production authentication and EHR/laboratory-system integration are deferred. The supplied `guideline` value describes AST interpretation, such as EUCAST 2023; it does not provide prescribing guidance.

## 2. Architecture and proposed files

Keep FastAPI, Pydantic, the existing `sqlite3` store and one server process. Extend `aei/` rather than introducing a competing `app/` project. Preserve the `Processor` boundary so a future materialized-results adapter can match the same versioned calculation contract. SQLAlchemy is optional for new read projections later; it is not a reason to replace the existing EventStore or add a P0 dependency.

Run one Uvicorn worker for P0. A process-level lock coordinates publication and analysis creation; retain WAL and the store's bounded connection timeout, and enable foreign-key checks for new projection tables. Keep transaction-scoped connections. Extend the store with a transaction-aware append helper so event inserts, culture records, visibility and state can commit together without nested independent connections.

```text
aei/
  api.py                     # existing create_app factory and legacy routes; add router/lifespan
  models.py                  # preserve schema-v1 isolate and prototype-v1 query/output contracts
  dataset.py                 # extend existing ZIP loader with directory/bundle import
  processor.py               # preserve Processor/ReferenceProcessor; share revision/selection helpers
  store.py                   # preserve append-only EventStore; add transaction-aware publication
  __main__.py                # keep current commands; extend source arguments and replay service use
  api_v1.py                  # additive /api/v1 router
  v1_models.py               # new evidence, pair, history, context and error contracts
  config.py                  # paths, demo clock, rule settings
  read_models.py             # normalized enrichment, culture and result projections
  services/visibility.py     # mandatory run/date restrictions
  services/windows.py        # calendar arithmetic and coverage flags
  services/cohorts.py         # first-isolate selection
  services/evidence.py        # counts, percentages, Wilson intervals
  services/comparisons.py     # common-denominator A/B results
  services/history.py         # raw visible specimen history
  services/replay.py          # clock and complete-bundle publication
  services/detection.py       # versioned rule and state transitions
  services/investigation.py   # composition and descriptive context
  services/analyses.py        # frozen results, cache, export
tests/
  test_pipeline.py           # existing unittest suite stays as the initial regression baseline
  fixtures/                  # small, authored synthetic cases
  test_import.py
  test_windows_cohorts.py
  test_evidence_comparisons.py
  test_replay_snapshots.py
  test_detection_api.py
config/monitors.json          # fixed P0 cohorts and drugs
.data/                       # existing ignored SQLite location
requirements.txt              # preserve current pins; add dependencies only with a concrete need
```

Prefer the existing pure-Python oracle and ordinary SQLite projections for P0. Share retained-isolate selection with the new facade, then calculate its richer responses; don't try to derive intervals or pairs from rounded legacy rows. Recompute for each publication revision and cache frozen results. Keep unittest as the first test runner; pytest can be a later optional convenience.

## 3. Database model, visibility and indices

Retain `isolate_events` as the append-only source of received isolate revisions. Its existing `event_id` primary key, unique `(isolate_id,revision)` constraint, canonical payload comparison and update/delete rejection triggers must survive every migration. Normalized facts below are replayable read projections or source-staging metadata, not a replacement event log.

Add a separate append-only culture-record contract for source cultures, including zero-isolate cultures, and versioned enrichment keyed to an isolate event ID. P0 can enrich imported schema-v1 events from the validated CSV relationships without changing their serialized payloads. If richer external events are introduced, add a versioned event type/model and explicit adapter; don't add undocumented fields to the strict schema-v1 model or rewrite stored payloads.

| Table | Required fields and constraints |
|---|---|
| `isolate_events` | Existing immutable event ID, isolate ID, revision and canonical JSON payload; retain the current EventStore contract. |
| `dataset_manifests` | ID, source kind, imported UTC time, file SHA-256 values, file row counts, source date bounds, importer version. |
| `patients` | `patient_id` PK, sex F/M, birth year, home area; link records to their dataset manifest. |
| `wards` | `ward_code` PK, Finnish/English names, in/out type, institution. |
| `antibiotics` | `antibiotic_code` PK, name, Finnish name, class, reported-for metadata, `is_screening`. |
| `culture_events` / `cultures` | Append-only culture records and their versioned projection: culture ID/revision, patient/ward, sample date, age, admission date, specimen, result, isolate count and ISO year/week. Staged records remain unpublished. |
| `isolates` | Projection keyed by isolate ID/revision with unique event ID; use the event's patient/culture/date/ward/organism/specimen, not an older source row. |
| `isolate_enrichments` | Event ID FK, source-manifest provenance, nullable sex/age/admission/quantity/phenotypes; attach metadata only to the matching validated revision/relationships. |
| `susceptibilities` | Projection keyed by isolate ID/revision/antibiotic; preserve version identity and the complete panel of that event. |
| `runs` | `run_id`, manifest FK, demo date, revision, playback status, speed, last published date, rule configuration version. |
| `run_cultures` / `run_events` | Published culture versions and exact isolate event IDs for a run, each with publication revision; visibility never means all stored source rows. |
| `analysis_snapshots` | Opaque `analysis_id`, kind, run/revision, canonical query, frozen JSON, source/method provenance, created UTC time. |
| `alerts` | Stable ID, run, configured cohort hash, drug, rule version, state, first/last trigger revision, last sufficient state, frozen evaluations. |

Use one source manifest per P0 database or namespace source IDs by manifest before supporting several datasets. A scenario fixture must run in a separate database or manifest namespace; it must never merge with the supplied dataset.

Add indices on culture date/patient/ward lookups, isolate ID/revision, organism/culture, susceptibility drug/isolate/revision, and run visibility by `(run_id,published_revision)`. Preserve version identity in every join so AST or demographics from an obsolete revision cannot attach to the latest one. Keep unique alert identity `(run_id,cohort_hash,antibiotic_code,rule_version)`.

Keep isolate-level phenotypes separate from drug susceptibility. Don't infer that an ESBL/MRSA flag supplies an unrecorded antibiotic result. Likewise, never convert a missing susceptibility row into S or R.

Schema-v1 API events lack sex, age, admission date and phenotype fields. Missing enrichment must remain unknown and appear in completeness counts; never silently derive an age or flag from a different revision. Implement importer enrichment before enabling those filters for the demo source. For future un-enriched external events, explicitly report unknown metadata and validate whether the requested analysis can be supported.

## 4. CSV import and validation

Extend `aei.dataset` with a shared reader for either a ZIP archive or an explicit directory, covering the six CSVs with comma separation and UTF-8/UTF-8-BOM handling. Keep documented `--zip` support; add an unambiguous directory argument and resolve the local dataset from the repository root. Preserve `load_dataset(path)->list[IsolateEvent]` for existing callers by delegating to a richer bundle loader. Preserve IDs as text and store parsed dates in ISO form internally and in the API.

Actual local `sample_date` and `admission_date` values use `DD-MM-YY`, for example `02-10-23`; the current importer accepts only ISO. Support these two explicit formats, with a documented 2000-2099 mapping for short years, so existing ISO archive fixtures retain compatibility. Reject other ambiguous formats rather than guessing locale. Use `age_years` at sampling for age filters, since birth year alone lacks a birthday.

Validate dictionaries/patients, cultures, isolates and AST before publication; create staged culture bundles, enrichment and a manifest report. Publication then appends through the EventStore contract and writes the new projections in one transaction. A failed validation leaves the previous usable data intact. Preserve current retry/conflict behavior: identical deliveries are idempotent, and changed data under the same event ID/revision fails the whole submitted batch. A legitimate correction appends a higher revision rather than updating history.

Validation must cover:

- Expected headers, duplicate primary keys, duplicate isolate/antibiotic pairs and all FK relationships.
- Allowed sex, specimen, culture-result, ward-type and susceptibility values; phenotype flags must be 0/1.
- Valid dates, sample/admission ordering for inpatients, nonnegative age and isolate counts, and ISO week/year consistency.
- `culture.n_isolates` against the actual linked isolate count; distinguish culture positivity from the existence of a resistance result.
- Redundant culture/organism values in susceptibility rows against their isolate; antibiotic and ward dictionary consistency.
- Row counts, observed source bounds, observed organism/specimen panels and a hash of each untouched source file.

Write a validation report with error code, file, row and safe record ID. Treat contradictions as import errors; represent documented source limitations as warnings. Don't fabricate missing clinical fields. The importer should flag that observed intrinsic resistant pairs exist even though the README says some such pairs are omitted.

All 24,571 supplied isolates have AST rows; each observed organism/specimen combination uses a fixed panel of 2-9 drugs. Keep the missing-test branch anyway, because incomplete panels belong in tests and future integrations. Build the catalogue from observed tests plus the supplied dictionary; observed testing does not establish clinical appropriateness.

## 5. Normative dates, cohorts and evidence calculations

### Effective date and source coverage

`as_of` is an ISO date and cannot exceed the run's published demo date. Omission means the current demo date, not the operating system's date. Every query also restricts cultures to the active run and its pinned publication revision, so an old selected date cannot reveal staged records.

Keep sample time separate from knowledge time. Legacy `AnalysisQuery.known_at` already limits received revisions, whereas `as_of` filters collection dates. The new facade pins published event IDs to a run/revision; a historical collection-date report uses that snapshot's known corrections and does not invent a historical result-release time. Preserve legacy `known_at` behavior and record the facade's publication cutoff in provenance.

Use half-open SQL intervals `[start_inclusive,end_exclusive)`. Let `T` be the selected as-of date and `E=T+1 day`.

| Window key | Exact rule |
|---|---|
| `annual` | `[January 1 of calendar_year, min(January 1 of following year,E))`; default year is T's year; reject a year after T's year. |
| `rolling_12m` | `[E minus 12 calendar months,E)`; clamp the day only when the target month lacks it. This is not a fixed 365-day window. |
| `last_28d` | `[T minus 27 days,E)`, which includes T and exactly 28 collection dates. |

At `T=2026-09-27`, rolling 12 months means `[2025-09-28,2026-09-28)` and the recent window means `[2026-08-31,2026-09-28)`. Calendar 2026 is year-to-date. Calendar 2023 also lacks full-year source coverage because the dataset starts on 2023-10-02.

Return requested bounds, source-overlap bounds and separate flags such as `is_year_to_date`, `source_covers_requested_window` and `replay_complete_through_as_of`. Also report the dataset's declared first/last date. These describe known file/replay coverage; no records on a date do not prove that a real laboratory feed was complete. Reaching 30 tested isolates does not establish full-period coverage.

### First-isolate pipeline

Run this pipeline independently for each time window and alert baseline:

1. Restrict event history to the pinned run's published event IDs/revision and any explicit knowledge cutoff. Resolve the highest visible revision per isolate before applying any collection-date, organism, specimen, ward or demographic filter. A correction moving an isolate outside the cohort must not resurrect its old revision.
2. Apply the requested collection-date interval to those latest snapshots. Join only the matching revision's enrichment; do not multiply candidates by joining AST rows yet.
3. Retain the first isolate per `(patient_id,organism,specimen)` through the shared selection helper, ordered by sample date, stable culture ID and isolate ID. A SQL projection may use `ROW_NUMBER()` with the same order. Collection times do not exist, so the ID tie-break is a reproducibility convention.
4. Apply ward and age/sex filters after this global first-isolate selection. A patient whose first qualifying specimen belongs to another ward won't acquire a replacement first isolate merely because the user changes wards.
5. Restrict to the requested single organism and specimen; filtering these before first-isolate ranking is equivalent because they are partition keys, but only after latest-revision resolution. Do not filter by resistance, phenotype or antibiotic availability before ranking.
6. Join the retained isolate IDs/revisions to their complete drug panels. Never choose a later isolate to fill an untested antibiotic. Count retained isolates once, regardless of how many antibiotic rows they have.

This global-before-ward convention maps to existing `dedup_scope=all_wards` and belongs to the new `surveillance-v1` method. Preserve the legacy `/statistics` default `selected_cohort` and its `prototype-v1` output. The existing processor always groups retained isolates by ward, even when ward is omitted; the facade must aggregate the retained all-wards set itself rather than sum legacy ward rows. Age bounds are inclusive; omitted sex/age bounds apply no restriction. An omitted ward means a clearly labeled all-wards cohort, never an automatic fallback from the chosen ward.

For one drug, calculate `n_eligible`, `n_tested`, `n_s`, `n_r` and `n_not_tested=n_eligible-n_tested`, with `n_tested=n_s+n_r`. Culture negatives, mixed growth without an isolate, repeat isolates and other species never enter that drug denominator.

`susceptibility_pct=100*n_s/n_tested`; `resistance_pct=100*n_r/n_tested`. Compute two-sided 95% Wilson intervals for these binary laboratory proportions, using unrounded values internally. Return values in 0-100 units, and let the frontend round display values. Confidence intervals describe the observed cohort; they don't predict treatment success.

| Evidence status | Meaning and percentage handling |
|---|---|
| `not_applicable` | Explicit dictionary specimen restriction conflicts with the requested specimen; return no percentages. Don't infer this merely from missing observed tests. |
| `empty_cohort` | No retained isolate matches the cohort; counts are zero and percentage/interval fields are null. |
| `no_tests` | Eligible isolates exist but none has the requested drug result; percentages and intervals are null. |
| `insufficient_data` | Tested n is 1-29; retain counts but return both percentages and both intervals as null. |
| `available` | Tested n is at least 30; calculate both percentages and intervals. |

Evaluate these conditions in the table's order. Always return `ci95.susceptibility` and `ci95.resistance` as objects containing `lower` and `upper`; set all four endpoints to null when an interval is unavailable. A fallback is a separate requested analysis with its own bounds, filters and label. Never replace a suppressed value in the original cohort response.

## 6. Paired antibiotic comparison and specimen history

For A/B comparison, use the selected window's same retained isolates, then keep only isolates with a recorded S/R result for both drugs. Reject identical drug codes. The common tested denominator is independent of each drug's marginal denominator.

| Cell | Drug A | Drug B |
|---|---|---|
| `both_s` | S | S |
| `a_only_s` | S | R |
| `b_only_s` | R | S |
| `both_r` | R | R |

Return `n_eligible`, `n_paired`, `n_missing_a`, `n_missing_b`, `n_missing_either`, and these four cells inside `comparison.cells`. Their sum must equal `n_paired`, and `n_eligible=n_paired+n_missing_either`. The missing-A and missing-B counts each include isolates missing both tests, so they need not sum to `n_missing_either`. Suppress paired percentages and derived differences when the common denominator is below 30, even if a marginal drug estimate meets 30.

On the common denominator, A's S share is `(both_s+a_only_s)/N`, while B's is `(both_s+b_only_s)/N`. The net B-minus-A difference is `100*(b_only_s-a_only_s)/N`; B-only added laboratory activity is `100*b_only_s/N`. They answer different questions. At-least-one-S is `100*(N-both_r)/N` and makes no claim about interactions between drugs or clinical efficacy.

Exclude FOX from therapeutic comparisons because the dictionary identifies it as a MRSA screen. Evidence screens may expose a clearly labeled screening result. Don't create a best-drug ranking or infer dosing, infection diagnosis or treatment outcomes from these data.

Patient history follows a different rule: return all published cultures in `[cutoff minus 12 calendar months,cutoff)`, strictly before the selected cutoff date. Include negative and mixed cultures, every associated isolate and its recorded AST. Do not apply surveillance deduplication or the cohort's ward/species/sex/age filters to this history. This requires new culture records because the current isolate-only log cannot represent negative, zero-isolate cultures. Resolve the latest visible culture/isolate revisions before date filtering; history must not restore obsolete revisions either.

The history label must say specimens collected before the selected date. Sample collection dates cannot establish when clinicians received results. Patient search derives eligible IDs from any published specimen on or before its search `as_of` date, independently of the history lookback. A patient with only a current-date specimen can therefore be selected and receive HTTP 200 with an empty prior-history list. Looking up an unknown or future-only patient returns the same 404 response. Paginate history using collection date and culture ID, with revision-bound cursors.

## 7. Replay, revision and snapshot lifecycle

Reuse the existing CLI replay's history seeding, date batching and empty-day advancement in a callable replay service. It currently writes isolate events and prints recomputed summaries; it has no HTTP controls, stored analyses or alerts. Stage the validated source outside published run visibility, initialize history through a demo cutoff such as 2026-08-30, then publish batches through 2026-09-27. The frontend labels this a synthetic simulation and polls `GET /state` every two seconds.

A replay step acquires the state lock, opens one write transaction, appends/publishes every culture bundle for the next demo date, advances the demo clock, evaluates configured monitors, and increments `state_revision`. A bundle includes its culture, zero or more complete isolate revision events, enrichment and associated AST. Use a transaction-aware extension of `SQLiteEventStore`; don't call today's independently committing `append_many` from inside another transaction and assume atomicity. Commit all bundle visibility and state together; a failed step publishes nothing.

Advance the clock on dates with no cultures too, because rolling windows can change without an arrival. Revision changes on visible data, effective-clock or playback-control changes. Repeating an already published bundle must not duplicate counts. Preserve the current synthetic `received_at=12:00 UTC on sample_date` import convention and label it; neither that convention nor replay publication time establishes actual laboratory turnaround.

`start` starts one background task and is idempotent while running. `pause` stops further scheduling and acknowledges any complete in-flight transaction. `step` works only while paused; reject concurrent steps. `reset` pauses playback, creates a new `run_id`, resets its cursor/revision and re-seeds visibility. It must not delete or rewrite append-only event history. Exact run/event membership keeps already stored future events from an earlier replay out of the reset run. Keep earlier frozen analyses available for export with their old run context.

Retain the legacy ingest routes and their responses. Their append-only log writes alone must not bypass complete-bundle publication into a replay run. Route eligible bundles through the same publication coordinator; otherwise leave them unpublished to the new run and report that state explicitly. Legacy `/statistics` continues to read its established EventStore view, while `/api/v1` reads the run-pinned view. P0's automatic demo input is the enriched dataset replay.

State includes `status=ready|running|paused|completed|error`, the demo date, latest visible sample date, run ID, revision, source kind and replay bounds. Reaching the source end produces `completed`; it must not suggest a live connection to Tyks systems.

For coordinated reads, accept optional `expected_run_id` and `expected_revision`. The frontend should send both after reading state. A mismatch returns HTTP 409 with the current state and no partial analysis. Reset must fail a stale request even when its revision number matches the new run's revision.

Analysis creation acquires the same lock, validates expected state and computes the full response in one read snapshot. Persist the frozen response before releasing the lock. `GET /evidence` returns all three windows in that one snapshot; it must never combine windows computed against different revisions.

Use a cache key containing run ID, revision, method version, endpoint kind and the canonical full query, including selected drugs and comparison window. Null/omitted defaults must normalize identically. New revisions invalidate current-query cache entries; immutable snapshots remain addressable by `analysis_id`. A later clock tick does not change an existing export.

## 8. Normative HTTP contract

Add the following routes under `/api/v1` through a router included by existing `aei.api.create_app`. Keep `/health`, `/events`, `/events/batch`, `/isolates/{id}/history` and `/statistics` intact. New facade response models must not repurpose the existing `AnalysisResult.rules_version` literal or silently change legacy rounding/status semantics. Configure CORS for the actual local frontend origin. FastAPI's generated OpenAPI document must match both supported contracts.

| Route | Purpose and main inputs |
|---|---|
| `GET /meta` | Ward/specimen/organism choices, observed drug panels, screening flags, data/method limitations, source coverage and supported windows. |
| `GET /state` | Current run, revision, demo clock, playback status and source freshness. |
| `GET /evidence` | Shared cohort query plus `antibiotic_code`; returns annual, rolling 12-month and 28-day evidence together. |
| `GET /comparisons` | Shared cohort query plus `antibiotic_a`, `antibiotic_b`, `window=annual\|rolling_12m\|last_28d`. |
| `GET /patients?search=` | Bounded ID search over patients with eligible visible collected specimens; optional as-of cutoff and revision expectations. |
| `GET /patients/{id}/history` | Preceding-12-month specimen history strictly before `as_of`; paginated if needed. |
| `GET /alerts` | Configured monitor signals for the active run, filters and explicit current evidence state. |
| `GET /investigations/{id}` | Investigation of the alert ID, with original detection context and a separately labeled current analysis. |
| `GET /analyses/{analysis_id}/export?format=csv\|json` | Download exactly the stored analysis; no recalculation against latest state. |
| `POST /replay/start` | Start/resume playback; body can configure validated days-per-tick and interval. |
| `POST /replay/pause` | Pause playback and return acknowledged state. |
| `POST /replay/step` | Publish the next complete date batch while paused. |
| `POST /replay/reset` | Create a new run with a validated cutoff and source/scenario choice. |

The shared cohort query is `ward_code,specimen,organism,sex,age_min,age_max,as_of,calendar_year`. Require exactly one valid specimen and organism on evidence/comparison routes. Omission of `ward_code` means explicit all-wards context; omission of sex/age means no demographic restriction. Reject inverted or negative age ranges, unknown codes, invalid dates, future as-of values and unsupported window strings with 422.

An antibiotic must exist in the catalogue, but lack of testing in a valid cohort returns an evidence status rather than an invented zero resistance rate. Comparison validation must also reject FOX and identical A/B codes. Catalogue labels describe observed tests, not a clinician-approved treatment list.

Every analysis response carries `analysis_id` and `context`. Context contains `run_id,state_revision,method_version,as_of,date_bounds,filters,source_manifest_id`, scope labels and source-coverage flags. Each endpoint must expose applied defaults, not only the values the client supplied. `/state` and replay responses carry run/revision state without an analysis ID because they aren't evidence analyses.

### Evidence example

The following is an illustrative authored fixture response, not a claim about the supplied cohort counts. The three windows happen to contain the same 40 retained, tested isolates in this fixture.

```http
GET /api/v1/evidence?ward_code=PHC-S&specimen=urine&organism=Escherichia%20coli&antibiotic_code=TMP&as_of=2026-09-27&calendar_year=2026&expected_run_id=run_demo&expected_revision=8
```

```json
{
  "analysis_id": "analysis_demo_01",
  "context": {
    "run_id": "run_demo", "state_revision": 8,
    "method_version": "surveillance-v1", "as_of": "2026-09-27",
    "source_manifest_id": "fixture_01",
    "filters": {"ward_code":"PHC-S","specimen":"urine","organism":"Escherichia coli","sex":null,"age_min":null,"age_max":null,"calendar_year":2026},
    "scope_labels": {"ward_scope":"selected_ward","source_kind":"scenario_fixture"},
    "date_bounds": {
      "annual":{"start_inclusive":"2026-01-01","end_exclusive":"2026-09-28"},
      "rolling_12m":{"start_inclusive":"2025-09-28","end_exclusive":"2026-09-28"},
      "last_28d":{"start_inclusive":"2026-08-31","end_exclusive":"2026-09-28"}
    },
    "coverage": {
      "annual":{"is_year_to_date":true,"source_covers_requested_window":true,"replay_complete_through_as_of":true},
      "rolling_12m":{"source_covers_requested_window":true,"replay_complete_through_as_of":true},
      "last_28d":{"source_covers_requested_window":true,"replay_complete_through_as_of":true}
    }
  },
  "antibiotic_code":"TMP",
  "windows": {
    "annual":{"status":"available","n_eligible":40,"n_tested":40,"n_s":32,"n_r":8,"n_not_tested":0,"susceptibility_pct":80.0,"resistance_pct":20.0,"ci95":{"susceptibility":{"lower":65.243,"upper":89.500},"resistance":{"lower":10.500,"upper":34.757}}},
    "rolling_12m":{"status":"available","n_eligible":40,"n_tested":40,"n_s":32,"n_r":8,"n_not_tested":0,"susceptibility_pct":80.0,"resistance_pct":20.0,"ci95":{"susceptibility":{"lower":65.243,"upper":89.500},"resistance":{"lower":10.500,"upper":34.757}}},
    "last_28d":{"status":"available","n_eligible":40,"n_tested":40,"n_s":32,"n_r":8,"n_not_tested":0,"susceptibility_pct":80.0,"resistance_pct":20.0,"ci95":{"susceptibility":{"lower":65.243,"upper":89.500},"resistance":{"lower":10.500,"upper":34.757}}}
  }
}
```

Production responses retain the calculator's precision; these fixture intervals illustrate shape and units. For 29 tested isolates, the window still carries counts but has `status=insufficient_data`, both percentage fields null and `ci95={susceptibility:{lower:null,upper:null},resistance:{lower:null,upper:null}}`.

### Comparison and error examples

```http
GET /api/v1/comparisons?ward_code=PHC-S&specimen=urine&organism=Escherichia%20coli&antibiotic_a=CIP&antibiotic_b=TMP&window=last_28d&expected_run_id=run_demo&expected_revision=8
```

Return the same context structure and an opaque analysis ID, plus a `comparison` object. The following comparison fragment continues an authored N=40 fixture; it is not a result calculated from the supplied CSVs. All percentage fields use 0-100 units. At `n_paired<30`, set the five percentage/difference fields below to null while retaining counts. Do not present the net difference as B's added activity or infer a combination-treatment recommendation.

```json
{
  "comparison": {
    "status": "available",
    "antibiotic_a": "CIP",
    "antibiotic_b": "TMP",
    "window": "last_28d",
    "n_eligible": 40,
    "n_paired": 40,
    "n_missing_a": 0,
    "n_missing_b": 0,
    "n_missing_either": 0,
    "cells": {"both_s": 25, "a_only_s": 5, "b_only_s": 8, "both_r": 2},
    "a_susceptibility_pct": 75.0,
    "b_susceptibility_pct": 82.5,
    "net_difference_pp": 7.5,
    "b_only_added_pct": 20.0,
    "at_least_one_s_pct": 95.0
  }
}
```

```json
{
  "error": {
    "code": "state_changed",
    "message": "The replay state changed; refresh the analysis.",
    "details": {"current_run_id":"run_demo","current_revision":9}
  },
  "request_id": "request_demo_09"
}
```

Use 409 for stale expected state, 422 for query/domain validation, 404 for unavailable patient/analysis/alert IDs, and 503 while no validated run is ready. A missing history result must not expose whether an ID exists only in staged future source rows. Return structured errors consistently instead of FastAPI defaults on some routes and custom shapes on others.

## 9. Automatic deviation detection and investigation

P0 runs a versioned heuristic only. It does not calculate p-values, label statistical significance or claim an outbreak. Configure a small fixed list of ward/organism/specimen/drug cohorts; UI filter changes don't silently create new monitors. Initial choices can target urinary E. coli in PHC-S, PHC-N, ED and URO, with a small observed therapeutic drug list.

For recent start `R=T-27 days`, compare `[R,T+1 day)` against the immediately preceding 12 calendar months `[R minus 12 calendar months,R)`. At T=2026-09-27, the baseline is `[2025-08-31,2026-08-31)` and recent is `[2026-08-31,2026-09-28)`. Deduplicate independently in each interval using the same surveillance method.

Evaluate only when both tested denominators reach 30. Trigger when recent resistance minus baseline resistance is at least the configured 10 percentage points. Store raw counts, unrounded difference, exact bounds, configuration and rule version. Patients may recur across the periods; the investigation must disclose this and treat the signal as a reason for review, without an independence or significance claim.

| State | Required behavior |
|---|---|
| `new` | First sufficient-data trigger for that run/cohort/drug/rule; create one stable alert identity. |
| `ongoing` | A later sufficient-data evaluation still triggers, including recurrence after an earlier clearance. |
| `cleared` | Sufficient current and baseline data no longer meet the threshold. |
| `insufficient_evidence` | A previously tracked signal cannot currently be evaluated because either denominator is too low; retain its last sufficient state. |

Never clear an alert because its recent denominator falls below 30. Before a monitor's first trigger, report evaluation availability in monitor metadata without manufacturing a cleared alert. Upsert the unique alert identity instead of appending a duplicate on every replay tick. Keep transition history and first/last trigger revisions.

`GET /alerts` returns active and requested historical states with drug, cohort, change, tested counts and links to investigation IDs. These are evaluations at the active run's monitoring date, not recomputed alerts for an arbitrary historical report `as_of`. Return `monitoring_as_of` explicitly. In P0, the frontend hides the current review list when a pinned reporting date differs from this monitoring date and offers a visible action to follow the replay date. The investigation returns the original detection snapshot alongside a current revision analysis, with each context labeled separately; for an archived run, retain its original snapshot and mark the current-analysis panel unavailable rather than mixing runs.

Investigation measurements include retained/raw counts, sex/age-bin counts, collection setting, hospital-day-3-or-later timing proxy, and drug testing counts. Use `sample_date-admission_date>=2 days` for that timing proxy; it doesn't establish the origin of infection.

Organism mix is a separately labeled count-only context with the organism filter removed. Keep its own population definition and counts because it is not the E. coli resistance denominator. Fixed-sex or narrow-age cohorts may show no composition change. Report measured differences without presenting them as causes.

The supplied panels are complete within organism/specimen combinations, so testing-completeness investigation may correctly show no variation. Do not invent anomalies to fill the screen. Add an authored, separately labeled scenario with sufficient baseline/recent denominators and a known resistance increase; load it under its own manifest and run ID, preserving original CSVs.

## 10. Snapshot storage and reproducible export

Persist the full analysis response, canonical query, method/rule versions, source hashes, code build identifier when available, creation timestamp and exact date bounds. Export by `analysis_id` only; export must remain stable after replay, date changes or reset. Unknown IDs return 404, while an existing old-run analysis remains downloadable with its original context.

JSON export contains the frozen response and provenance manifest. CSV export uses documented long-form rows with `record_type`, window, organism, specimen, ward, drug(s), counts and measurement fields. Repeat run/revision, method version, source manifest/hash, as-of date, bounds and canonical filter JSON on each row so ordinary CSV tools retain provenance.

Keep suppressed percentages and intervals empty/null in exports. Counts remain available. Export the source-coverage and suppression reasons as well; a researcher must be able to distinguish an empty cohort, missing tests, low n and incomplete source period. Pair export must preserve the both-tested denominator and four cells.

Stream the stored snapshot or its deterministic CSV conversion without recomputing SQL. A person downloading an analysis they saw before the latest replay tick must receive that analysis, even while the application is showing a newer revision.

## 11. Build phases, dependencies and completion checks

| Phase | Tasks and dependencies | Exit condition |
|---|---|---|
| B0: baseline/contract | Record the current unittest result, existing route/schema behavior and dataset-path failure; define additive Pydantic contracts/defaults and coordinate with frontend. | Preserve baseline tests and document the missing-ZIP prerequisite; agree on context, nulls, statuses, units and revision behavior. |
| B1: source/enrichment | Extend `aei.dataset` and CLI with directory/ISO-or-short-date support, culture records, matching enrichment, projection migrations and manifest validation; retain ZIP compatibility. | Actual local files import with matching counts; schema-v1 events retain canonical identity; invalid bundles fail atomically. |
| B2: shared calculations | Extract/reuse latest-revision and first-selection helpers; preserve legacy processor outputs; add run visibility, calendar windows, new statuses and Wilson intervals. | Existing regression tests and new revision/boundary/denominator tests pass; all-wards evidence recomputes retained isolates rather than summing rows. |
| B3: additive facade | Include the `/api/v1` router in `create_app`, expose meta/state/evidence, initialize a paused run and configure CORS; depends on B0-B2. | Frontend displays all three windows and suppression from the real API while every legacy route remains available. |
| B4: comparisons/history | Implement paired cells, code validation, bounded patient search and raw history; depends on B2/B3. | Paired missing-test and no-future-history checks pass. |
| B5: replay/snapshots | Reuse CLI replay sequencing; add transaction-aware EventStore publication, HTTP controls, run memberships, polling state and immutable analyses; depends on B3. | New bundles update evidence automatically; stale requests fail; reset preserves append-only history. |
| B6: detector/investigation | Configure one heuristic, lifecycle, contextual measurements and labeled scenario; depends on B2/B5. | A scenario creates one alert and investigation; low n never clears it. |
| B7: export/demo | Add CSV/JSON export, scenario reset and end-to-end rehearsal; depends on B4-B6. | Export matches the displayed revision while replay continues. |

Frontend work can proceed after B0 using fixture responses, then switch to B3's real vertical slice. Avoid building every endpoint before checking that the frontend can display a genuinely suppressed recent cohort and recover from a 409 response.

Existing commands are `python -m unittest discover -s tests -v`, `python -m aei schema`, and `python -m uvicorn aei.api:create_app --factory --host 127.0.0.1 --port 8000`. `import-dataset`, `stats` and `replay` also exist, but the import/replay default source path is currently unusable in this checkout. The supplied-archive unittest calls that same absent default ZIP; fix directory support and its fixture selection before claiming the full suite passes. No tests were executed or dependencies installed in this planning task.

During B1, add mutually exclusive `--dataset-dir PATH` and existing `--zip PATH` source options, documenting deterministic default resolution and keeping explicit ZIP callers working. A proposed post-B1 import is `python -m aei import-dataset --dataset-dir synthetic_microbiology_dataset`. Update README's historical working-directory text and commands in that later implementation phase. Keep the existing global `--db` argument order and CLI replay's documented behavior; use a separate `.data/` database for a legacy CLI replay.

## 12. Acceptance test matrix

Use small authored fixtures for calculation boundaries; run one importer integration check against the ignored supplied data when available. Don't put the full dataset into git or fabricate clinical outcomes for tests.

| Area | Required check |
|---|---|
| Legacy regression | Keep `tests/test_pipeline.py` and existing route/schema assertions; add new tests without replacing unittest or changing `prototype-v1` defaults. |
| Import shape | Verify headers, row counts, source hashes and observed date range; preserve input bytes. |
| Source compatibility | Directory and ISO ZIP fixtures yield equivalent isolate events; default resolution finds the local directory when the historical ZIP is absent. |
| Import integrity | Reject orphan records, conflicting duplicate AST pairs, invalid S/R values and admission dates after collection; roll back the import. |
| Date parsing | Parse `02-10-23` as 2023-10-02 and reject ambiguous unsupported formats. |
| 28-day bounds | Include T and T-27; exclude T-28 and T+1. |
| Calendar arithmetic | Test leap-day/end-of-month clamping, the documented rolling 12-month example and a selected completed calendar year. |
| Source coverage | Mark 2023 annual as source-incomplete and 2026 annual as year-to-date; n>=30 must not remove coverage warnings. |
| Global first isolate | Repeat same-patient/species/specimen once; retain different species/specimen groups; test date/ID ties. |
| Revision before filters | A newer revision moving date, ward, organism or patient outside a cohort removes the old snapshot; it must not reappear after filtering. Preserve legacy `known_at` correction behavior. |
| All-wards facade | Compute from latest retained isolates, not rounded or summed ward reports; document the `all_wards` mapping and legacy `selected_cohort` default. |
| Ward/age ordering | A first isolate outside the selected ward or age band prevents replacement by a later matching one. |
| Enrichment/history | Dataset sex/age/admission/phenotypes attach to the correct event revision; missing external metadata stays unknown; zero-isolate culture records appear in history. |
| Period independence | A patient can contribute a different first isolate in annual and recent windows; recompute when the earliest record leaves a window. |
| Missing drug | Do not choose a later tested isolate; verify n_eligible, tested n and n_not_tested. |
| Threshold | At 29 tested, percentages/intervals are null; at 30 they appear. Apply this independently per drug and paired denominator. |
| Multi-drug rows | One isolate with nine AST rows never becomes nine eligible isolates; counts and intervals use distinct retained isolates. |
| Valid empty states | Distinguish explicit specimen incompatibility, zero eligible isolates, missing tests and low n. |
| Pair calculations | Four cells sum to common n; net B-A differs from B-only activity; missing either test excludes the pair. |
| Catalogue checks | Reject unknown drug, A=B and FOX therapeutic comparison; don't call unobserved testing clinical inappropriateness. |
| Search/history | Future-only IDs remain hidden; a visible current-only patient returns an empty prior-history list; include earlier negative/mixed specimens and repeated isolates; exclude history samples on/after the strict cutoff. |
| Replay atomicity | A failure before commit reveals no partial bundle or revision; a repeated bundle leaves counts unchanged. |
| EventStore invariants | Retain update/delete-rejection triggers, canonical retry equality and atomic conflicting-batch rollback while adding projections/publication. |
| Clock-only step | Advance on an empty collection date and invalidate window-dependent analyses. |
| Stale requests | Wrong revision or old run ID returns 409; all evidence windows share one pinned context. |
| Pinned reports/monitoring | Current alerts carry `monitoring_as_of`; a pinned historical report does not show later monitoring flags as if they belonged to that report date. |
| Reset/export | Reset creates a new run without deleting history; old-run future events remain invisible until published; prior displayed analysis exports remain stable. |
| Detection | Sufficient fixture data with >=10pp rise triggers once; a stable sufficient cohort doesn't. |
| Alert lifecycle | Repeated evaluations don't duplicate alerts; sufficient non-trigger clears; low n gives insufficient evidence, never clearance. |
| Investigation | Contextual organism counts have their own scope; composition differences never appear as causal explanations. |
| UI/export agreement | CSV and JSON retain the exact counts, nulls, method, bounds and revision displayed by the frontend. |

P0 is complete when a reviewer can choose a ward, inspect all three windows, compare two antibiotics, open specimen history, watch replay change the evidence, investigate an automatic scenario signal and export that same analysis. Run a small response-time check on the actual source data; optimize only if the chosen demo machine struggles.

## 13. P1 and later work

P1 can add configurable researcher monitors, additional saved cohorts, richer descriptive comparisons, monitor recurrence summaries, cached query profiling and more export formats. An optional SQLAlchemy read-model adapter or pytest runner must preserve existing storage and test contracts; neither is a prerequisite for the hackathon result.

Keep `flink/README.md` as the future integration boundary: PostgreSQL append-only source, Flink CDC/Flink, versioned materialized results and API reads. No real Flink work belongs in P0. A later processor must match the reference oracle's revisions, knowledge cutoffs, independent first-isolate windows and clock-driven expiration; the API should read its completed materializations rather than synchronously invoke a Flink job.

Production work requires a real laboratory feed contract, result-release timestamps, correction/cancellation semantics, access control, audit/security requirements, operational monitoring and clinical validation. Patient-specific prescribing or a therapeutic guideline layer requires additional verified information and clinical ownership that this dataset doesn't supply.

## 14. Source references and limits

- Current [README](README.md), [API factory](aei/api.py), [models](aei/models.py), [processor](aei/processor.py) and [EventStore](aei/store.py) establish implemented behavior at `b54015f`. Preserve its legacy calculation and append-only contracts while introducing the new method version.
- Existing [dataset loader](aei/dataset.py), [CLI](aei/__main__.py), [unittest suite](tests/test_pipeline.py), [requirements](requirements.txt) and [Flink boundary](flink/README.md) define import/replay limitations, pinned dependencies and deferred integration.
- Challenge brief: `../Tyks x Mylab cas informations.md`, lines 9-15, requests up-to-date ward relevance, continuous updates, previous four weeks and annual results; lines 19-25 define judging criteria, including anomaly detection. Lines 38-40 give calculation tips and leave tooling open.
- Dataset [README](synthetic_microbiology_dataset/README.md), lines 61-69, gives counts, source dates and first-isolate/30-isolate guidance; lines 79-89 define table columns. Lines 37-38 discuss collection from hospital day 3; lines 71-75 describe data simplifications.
- Actual [cultures](synthetic_microbiology_dataset/cultures.csv) contain the non-ISO date strings and collection/admission fields. [Isolates](synthetic_microbiology_dataset/isolates.csv) contain organism and phenotype flags; [susceptibility](synthetic_microbiology_dataset/susceptibility.csv) contains the drug outcomes and interpretation standard.
- The [antibiotic dictionary](synthetic_microbiology_dataset/antibiotics.csv) marks FOX as a screen; [patients](synthetic_microbiology_dataset/patients.csv) supplies demographics, and [wards](synthetic_microbiology_dataset/wards.csv) supplies location labels.

Dataset files remain local and ignored; their links work in this workspace after the supplied dataset is placed in the backend folder. The API choices, Wilson method, global-before-ward convention, heuristic threshold and replay design are proposed implementation decisions. Keep their versions in every analysis so later changes remain reviewable.
