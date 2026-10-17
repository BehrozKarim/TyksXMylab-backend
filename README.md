# Tyks AEI backend

Current flow: **CSV seed + external HTTP feed -> durable PostgreSQL history/queue ->
published run -> Python reference processor and alerts -> SSE dashboard notifications**.
Evidence, comparisons, patient history and investigations retain saved JSON/CSV exports.
The original event-ingestion and statistics endpoints also remain available.
PostgreSQL runs in Docker with a persistent
volume. Annual results now mean **rolling last 12 calendar months**, alongside the
rolling last 28 days. The backend includes configurable resistance alerts and
investigation APIs, complete-culture feed ingestion and reconnectable live updates.
The separate `TyksXMylab-frontend` consumes these APIs; Flink remains deferred.
For the public hosted demo, see [Render deployment](DEPLOYMENT.md).
See [feed integration and the dashboard handoff](FEED_INTEGRATION.md) for the sender,
API contract and EventSource example.

## Run the app and synthetic feed together

After the backend virtual environment and sibling frontend dependencies are installed:

```powershell
.\scripts\start-demo.ps1
```

The launcher starts Docker PostgreSQL, the backend, the dashboard and the HTTP dataset
sender using a dedicated demo schema. Click **Send next lab batch** in the dashboard
to deliver each batch; nothing is sent automatically. It resumes published progress; `-Restart` starts
a fresh demo run while preserving saved reports. Ctrl+C stops its servers and leaves
PostgreSQL running. See [DEMO.md](DEMO.md) for setup, source paths, ports and verification.

## Report summaries

The ward overview offers an on-demand summary of its saved evidence report, with
configurable OpenAI generation, validated sentence composition, persistent jobs and
a labelled basic fallback. See [AI_SUMMARY.md](AI_SUMMARY.md) for configuration,
the prompt, API contract and verification. Model calls run outside feed publication.

## Start the database and backend

Run from this repository root using Python 3.11 and Docker Desktop (Linux containers):

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
docker compose up -d --wait postgres
.\.venv\Scripts\python.exe -m aei init-db
```

The default development connection is
`postgresql://aei:aei_local_only@127.0.0.1:55432/aei`. The port is bound to loopback.
Override the backend connection with `AEI_DATABASE_URL` and the API/CLI schema with
`AEI_DATABASE_SCHEMA` (default `aei`; CLI `--schema` takes precedence). Compose also accepts
`POSTGRES_PASSWORD` and `POSTGRES_PORT` overrides; set a matching backend URL when
changing them. A password environment change does not reset an existing volume's
database password. Use these default credentials only for local development.

Data persists in the Compose project's named volume. `docker compose stop` stops
the database without deleting its data. Logical WAL is enabled for future CDC;
no Flink replication slot or job is created yet.

## Load data or migrate SQLite history

The supplied data is not committed to Git. Explicit `--zip` or `--dataset-dir`
selects the source. Otherwise `AEI_DATASET_PATH` takes precedence, followed by
the ZIP beside this project, then `synthetic_microbiology_dataset/` in this project.
An explicit missing source fails rather than silently selecting another dataset.

```powershell
# Existing prototype users: preserve every recorded revision from SQLite.
.\.venv\Scripts\python.exe -m aei migrate-sqlite --sqlite .data/aei.sqlite3

# Validate all six CSV files without connecting to PostgreSQL.
.\.venv\Scripts\python.exe -m aei validate-dataset --zip ../synthetic_microbiology_dataset.zip --report .data/validation.json

# Import cultures, metadata and isolate events in one transaction.
# Also works after a previous isolate-only import: existing events are preserved.
.\.venv\Scripts\python.exe -m aei import-dataset --zip ../synthetic_microbiology_dataset.zip --report .data/import.json

# Alternative source: a directory containing the same six CSV files.
.\.venv\Scripts\python.exe -m aei validate-dataset --dataset-dir synthetic_microbiology_dataset

# API documentation: http://127.0.0.1:8000/docs
.\.venv\Scripts\python.exe -m uvicorn aei.api:create_app --factory --host 127.0.0.1 --port 8000
```

Migration opens SQLite read-only and retains every event ID, revision and payload.
Import/migration can be repeated safely: identical events are ignored; conflicts
roll back the entire batch. SQLite is retained only for migration and isolated unit
tests. The running backend uses PostgreSQL with no automatic SQLite fallback.

### Source validation and additional storage

Both sources require `patients.csv`, `wards.csv`, `antibiotics.csv`, `cultures.csv`,
`isolates.csv` and `susceptibility.csv`, with the supplied headers and comma-delimited
UTF-8 (optional BOM). Dates accept only `YYYY-MM-DD` or `DD-MM-YY`; short years mean
2000-2099. IDs remain strings, including leading zeros. Source files are never rewritten.

Validation rejects duplicate IDs/AST pairs, missing links, dictionary mismatches,
invalid values, contradictory culture/isolate counts, incorrect ISO weeks and
admissions after sampling. Errors identify code, file, CSV line and record ID;
the report retains the first 100 errors and the total error count. Validation
failures exit with status 2 before opening a database connection. Missing AST is
allowed and reported; no antibiotic outcome is inferred from a phenotype.

Each manifest records source-file SHA-256 hashes, row counts, observed culture-date
bounds, observed organism/specimen/drug panels and importer version. Warnings identify
the synthetic receipt-time convention and the supplied README's omitted-pair discrepancy.
Observed date bounds describe file contents, not proof of a complete live laboratory feed.

PostgreSQL adds these immutable tables alongside the unchanged isolate event payloads:

| Table | Stored information |
| --- | --- |
| `dataset_manifests` | Source identity, validation provenance and first import timestamp. |
| `patients` | Sex, birth year and home area. |
| `wards`, `antibiotics` | Source dictionaries, including drug screening labels. |
| `culture_events` | Every culture, including negative/mixed cultures with no isolates. |
| `isolate_enrichments` | Demographics, age at sampling, admission date, quantity and phenotype flags attached to the exact isolate event revision. |

All source records, cultures, isolate events and enrichments commit together. Conflicts
or a failure in any part roll back the complete import. Repeated imports of identical
files are safe, including a ZIP and directory containing identical bytes. One manifest
is allowed per PostgreSQL schema: a changed source or separate scenario requires a
separate `--schema`. Equivalent CSVs re-encoded with different dates have different
source hashes and therefore also require a separate schema. An external corrected
isolate revision receives no metadata from an older revision automatically.

`import-dataset --through YYYY-MM-DD` imports complete cultures and their isolates
through that inclusive date. Repeating the same source with a later cutoff extends
the import. Dictionaries/patients and the manifest describe the whole source, while
the returned per-table counts describe the selected import. Source staging and replay
visibility are separate: a later import does not silently change an existing run.
Complete the import, then reset replay to publish a new view. Patient-history and
comparison APIs read only the records published in that view.

## Reports and API

```powershell
.\.venv\Scripts\python.exe -m aei stats --as-of 2026-09-27 --period rolling_12_months --ward ED --organism "Escherichia coli" --specimen urine --antibiotic CIP
.\.venv\Scripts\python.exe -m aei compare --as-of 2026-09-27 --ward ED --organism "Escherichia coli" --specimen urine --antibiotic CIP
```

- `GET /health`: storage backend, processor, event count.
- `POST /events`: append one complete isolate revision.
- `POST /events/batch`: append 1-500 revisions in one transaction.
- `GET /isolates/{isolate_id}/history`: every revision of that isolate.
- `GET /statistics?as_of=2026-09-27`: latest 28 days by default.
- `GET /statistics?as_of=2026-09-27&period=rolling_12_months`: latest 12 months.
- `GET /statistics/comparison?as_of=2026-09-27`: both periods from one source snapshot.
- `GET /api/v1/evidence`: enriched evidence for one organism, specimen and antibiotic,
  with both periods, demographic filters, confidence intervals and source coverage;
  returns a saved `analysis_id` and its run/revision.
- `GET /api/v1/meta`: filter dictionaries, observed panels, screening labels, supported
  periods, source bounds, defaults and limitations.
- `GET /api/v1/state`: active run, revision, demo date and published culture/isolate counts.
- `GET /api/v1/comparisons`: paired susceptibility of two antibiotics in one selected period.
- `GET /api/v1/patients?search=`: bounded ID search over patients with visible specimens.
- `GET /api/v1/patients/{patient_id}/history`: paginated prior specimen/AST history.
- `GET /api/v1/analyses/{analysis_id}`: retrieve the original saved response.
- `GET /api/v1/analyses/{analysis_id}/export?format=json|csv`: download that response.
- `POST /api/v1/replay/reset|start|pause|step`: control the synthetic replay.
- `POST /api/v1/feed/start`: create a receiving run with simulated or UTC wall clock.
- `POST /api/v1/feed/batches`: durably accept complete cultures; return a 202 receipt.
- `GET /api/v1/feed/receipts/{id}` and `POST .../{id}/retry`: inspect or retry delivery.
- `GET /api/v1/updates`: SSE committed-state notifications with reconnect recovery.

Filters: `ward_code`, `organism`, `specimen`, `antibiotic_code`, `dedup_scope`.
`known_at` optionally restricts results to revisions received by that timestamp.
`as_of` controls sample dates, not knowledge time. Without `known_at`, the latest
known revisions are used. Legacy statistics require an explicit `as_of`; published
evidence defaults to the run's reporting date. Replay/simulated feed clocks are explicit;
`wall_utc` feed mode advances its reporting date with the current UTC day.

### Enriched evidence

Run `import-dataset` first to populate the dictionaries, manifest and revision-bound
metadata. Existing `/statistics` responses and `prototype-v2` rounding/status rules
remain unchanged. The additive evidence contract identifies itself as `evidence-v1`.

```powershell
.\.venv\Scripts\python.exe -m aei evidence --as-of 2026-09-27 --ward ED --organism "Escherichia coli" --specimen urine --antibiotic CIP --sex F --age-min 65
```

Equivalent HTTP request:

```text
GET /api/v1/evidence?as_of=2026-09-27&ward_code=ED&organism=Escherichia%20coli&specimen=urine&antibiotic_code=CIP&sex=F&age_min=65
```

Required inputs are `organism`, `specimen` and `antibiotic_code`. Optional
filters are `ward_code`, `sex`, inclusive `age_min`/`age_max`, `dedup_scope`, and
`known_at`. Optional `as_of` defaults to the published demo date and cannot exceed it.
Unknown codes, missing required inputs, invalid dates and inverted age
ranges return HTTP 422. A missing validated dataset/unsupported store returns 503.
On first use, a completed run exposes the complete culture bundles already imported,
through their latest sample date. Use reset to start a replay from an earlier cutoff.

Each response returns `analysis_id`, `context` and `windows.rolling_28_days` / `rolling_12_months`:

- Revision resolution and first-isolate selection are shared with the existing
  reference processor. Each period selects independently. The default ward policy
  remains `selected_cohort`; `all_wards` remains an explicit alternative.
- Omitting the ward returns one combined all-wards cohort. It reselects first
  isolates across wards; it does not add ward-specific denominators or percentages.
- Age/sex filters apply **after** first-isolate selection. A later matching isolate
  never replaces an earlier nonmatching or unknown one. Age comes from the selected
  specimen's recorded age, not an estimate from birth year.
- Metadata is joined by exact event ID. New corrections without enrichment remain
  unknown; old-revision metadata is never reused. Without demographic filters those
  isolates remain eligible. With a requested demographic filter, unknown records
  are excluded and counted in `metadata.excluded_unknown`.
- `metadata` also reports the retained count before demographic filtering, unknown
  sex/age counts, and known nonmatching exclusions. Unknown counts describe the
  population before demographic filtering and are not extra eligible isolates.

| Status | Meaning |
| --- | --- |
| `not_applicable` | The dictionary explicitly restricts the drug to another specimen. |
| `empty_cohort` | No eligible isolates after selection/filtering; inspect metadata exclusions too. |
| `no_tests` | Eligible isolates exist, but none was tested against this drug. |
| `insufficient_data` | Between 1 and 29 eligible tested isolates. |
| `available` | At least 30 eligible tested isolates. |

Only `available` exposes percentages and two-sided 95% Wilson score intervals.
All other states retain counts and return null percentages and interval endpoints.
Evidence values use 0-100 units and retain calculation precision for display rounding.
The [Wilson formula](https://itl.nist.gov/div898/handbook/prc/section2/prc241.htm)
describes uncertainty in the observed laboratory proportion, not treatment success.
Screening drugs are labeled through `context.antibiotic_is_screening`.

Coverage separates the source's observed date span from how much has been imported:

- `source_date_span_covers_window` tests the requested bounds against the manifest's
  observed source dates. `overlap_start`/`overlap_end` are null for disjoint periods.
- `source_fully_imported` compares imported culture/enrichment counts to the full
  manifest, so `--through` cannot silently claim a complete source import.
- Actual imported culture bounds and expected/imported counts are returned separately.
  These flags describe source files and database contents, not proof of real feed
  completeness. They remain independent of the 30-isolate threshold and `known_at`.

Published evidence separates physical source coverage from run visibility:
`context.published_cultures`, `published_isolates`, `demo_date` and
`latest_visible_sample_date` describe the visible run. A fully staged source can
still have most records hidden during replay. Counts in `coverage` describe the
physical import at analysis creation, not a claim that future samples were included.

Both windows are calculated under the same PostgreSQL publication/import lock and
saved before that lock is released. Reports contain the exact run/revision, normalized
filters, method/importer versions and source-file SHA-256 hashes. Repeating the same
query at the same run/revision reuses its saved report; omitted defaults and equivalent
knowledge-time offsets normalize identically. Steps (including empty days), start and
pause change the revision; reset creates a new run. Old reports and exports never change.
No source patient identifiers appear in aggregate exports.

JSON exports contain the saved document. Evidence CSV exports contain two rows, one per period,
with flattened dotted field names for context, filters, provenance, coverage and interval
bounds. Suppressed percentages/intervals are empty; counts and status are retained.
Spreadsheet formula prefixes in source labels are escaped with a leading apostrophe.

For coordinated reads, send `expected_run_id` and `expected_revision` together, using
`/api/v1/state`. Evidence, comparison, patient search, history, alerts and investigations accept them as query parameters; replay controls accept them
in the JSON body. A mismatch returns HTTP 409 with `error.details.state` and creates
no analysis. Omitting both uses whichever state is current when the request acquires
the lock. Clients can subscribe to `/api/v1/updates` and refresh on committed changes;
polling `/state` remains a fallback. State and alert reads return the last committed
revision while a worker processes the next batch. See [the SSE handoff](FEED_INTEGRATION.md#dashboard-handoff).

Versioned errors consistently use
`{"error":{"code":"...","message":"...","details":{}},"request_id":"..."}`,
with the same ID in `X-Request-ID`. Legacy routes retain their original error shapes.
Set `AEI_CORS_ORIGINS` to a comma-separated list of allowed frontend origins; defaults
are `http://localhost:3000` and `http://127.0.0.1:3000`.

### Antibiotic comparison

```text
GET /api/v1/comparisons?organism=Escherichia%20coli&specimen=urine&antibiotic_a=CIP&antibiotic_b=TMP&ward_code=ED&window=rolling_12_months&as_of=2026-09-27
```

Require one organism/specimen and two different antibiotic codes. `window` accepts
`rolling_28_days` (default) or `rolling_12_months`. Other cohort filters match evidence:
ward, sex, inclusive age bounds, `known_at` and the existing `selected_cohort` default
deduplication scope. The CLI `compare` and `/statistics/comparison` still compare
reporting periods; `/api/v1/comparisons` compares antibiotics.

The `comparison-v1` method shares evidence's revision resolution, first-isolate and
demographic selection. It then keeps only retained isolates tested for **both** drugs.
A later tested isolate never replaces an untested first isolate. Results include:

- `n_eligible`, each drug's marginal `n_tested_a`/`n_tested_b`, and common `n_paired`.
- `n_missing_a`, `n_missing_b` and `n_missing_either`. Isolates missing both appear in
  each individual missing count, but only once in `n_missing_either`.
- Four cells: `both_s`, `a_only_s` (A=S/B=R), `b_only_s` (A=R/B=S), and `both_r`.
  Their sum is `n_paired`; `n_paired + n_missing_either = n_eligible`.

On the common denominator N, the five measures are:

| Field | Calculation |
| --- | --- |
| `a_susceptibility_pct` | `100 * (both_s + a_only_s) / N` |
| `b_susceptibility_pct` | `100 * (both_s + b_only_s) / N` |
| `net_difference_pp` | `100 * (b_only_s - a_only_s) / N` (B minus A, percentage points) |
| `b_only_added_pct` | `100 * b_only_s / N` |
| `at_least_one_s_pct` | `100 * (N - both_r) / N` |

Status precedence is `not_applicable` for a dictionary specimen conflict,
`empty_cohort`, `no_paired_tests`, `insufficient_data` for 1-29 pairs, then `available`.
All five measures are null unless status is `available`, even if both marginal drug
counts reach 30. Counts, demographic completeness and source coverage stay visible.
Same-drug, unknown-drug and screening-drug comparisons return 422. FOX remains
available as labeled single-drug evidence; `/meta.comparison_antibiotics` excludes screens.
These are laboratory observations, with no drug ranking, synergy or treatment-efficacy claim.

Comparison responses carry a saved `analysis_id` and full source/run/filter context.
JSON export preserves the response; CSV has one `paired_comparison` row, including
all cells, denominators, missing counts, suppressed values and provenance.

### Resistance alerts and investigations

```text
GET /api/v1/alerts
GET /api/v1/alerts?states=cleared&states=ongoing
GET /api/v1/investigations/{investigation_id}
GET /api/v1/analyses/{analysis_id}
GET /api/v1/analyses/{analysis_id}/export?format=json
GET /api/v1/analyses/{analysis_id}/export?format=csv
```

The `resistance-rise-v1` rule is a **proposed heuristic for review**, with no p-value,
significance, outbreak, causal or treatment-efficacy claim. For monitoring date T,
recent start R is T minus 27 days. Compare recent `[R, T+1 day)` with the disjoint
baseline `[R minus 12 calendar months, R)`. On 2026-09-27 these are
`[2026-08-31, 2026-09-28)` and `[2025-08-31, 2026-08-31)`. Start dates are inclusive;
`end_date_exclusive` is excluded. A leap-day baseline anniversary clamps to February 28.
This baseline deliberately differs from the dashboard's rolling-year report ending at T.

Each period independently uses the shared latest-revision and first-isolate rules,
including the existing `selected_cohort` ward default, deterministic same-day ties,
demographic filters after first selection, and no replacement of an untested first
isolate. Each period needs **30 tested isolates**. The default trigger is recent
resistance minus baseline resistance **>=10 percentage points** (20% to 30%, for example).
The decision uses exact rational arithmetic; the response retains the unrounded change.
If either period is below 30, its percentage and the comparison change are null.
Raw counts, missing-test counts, demographic completeness and source coverage remain visible.

The default four monitors cover urinary E. coli / CIP in PHC-S, PHC-N, ED and URO.
The example [alerts.example.json](alerts.example.json) matches these defaults. Configure
a fixed list of at most 32 monitors and a positive threshold up to 100 percentage
points using `AEI_ALERT_CONFIG`; `minimum_tested` remains 30. Optional monitor fields
are `sex`, `age_min`, `age_max` and `dedup_scope`; omitted/null ward means all wards.

```powershell
$env:AEI_ALERT_CONFIG = (Resolve-Path .\alerts.example.json).Path
.\.venv\Scripts\python.exe -m uvicorn aei.api:create_app --factory --host 127.0.0.1 --port 8000
```

Configuration is **frozen for each run**, with a configuration hash in every report.
Restart with the desired file and reset replay to apply a change to a new run.
Existing run history is preserved. `{"monitors":[]}` disables monitors for a new run.
Missing/invalid configuration files fail explicitly; duplicates and invalid thresholds
are rejected. Unknown wards/organisms/drugs, screening drugs and incompatible specimen
restrictions produce an explicit `unavailable` monitor with a reason, never a false alert.
Threshold, baseline and monitor choices remain marked `proposed_pending_partner_confirmation`.

Every run initialization and daily replay publication evaluates all monitors in the
**same transaction** as the culture bundles, clock and revision, including empty dates.
A detector failure rolls back the publication. An existing pre-alert run is adopted
at its current date without inventing earlier monitoring history. Dashboard filters,
ordinary reads and start/pause controls do not create monitors or advance the lifecycle.
Legacy `/events` writes remain history-only and cannot trigger these run-based alerts.

| Alert state | Meaning |
| --- | --- |
| `new` | First sufficient-data trigger in this run. |
| `ongoing` | A later publication still triggers, including recurrence after clearance. |
| `cleared` | Both denominators are sufficient and the increase is below threshold. |
| `insufficient_evidence` | A tracked alert cannot be evaluated; retain `last_sufficient_state`. Low counts never clear it. |

Before the first trigger, only monitor evaluation metadata exists. There is no
invented cleared alert. One stable identity covers run/cohort/drug/rule, with first/last
trigger dates and revisions. Transitions and all monitor evaluations are immutable.
The mutable `alerts` table stores the latest lifecycle state; `alert_configurations`,
`alert_batches`, `alert_evaluations`, `alert_transitions` and frozen analyses retain history.

`/alerts` defaults to new, ongoing and insufficient-evidence alerts. Repeat `states`
to request other lifecycle states, including cleared alerts in the active run. The
response includes `run_id`, current `state_revision`, last `monitoring_revision`,
`monitoring_as_of`, configuration, `alerts`, and `monitors`. Join an alert's `monitor_id`
to `monitors[].context.monitor_id` for its cohort, drug, counts, change and latest
evaluation status. `monitoring_revision` can lag the state revision after start/pause;
the monitoring date and data remain unchanged. Unknown query filters, including
`as_of`, are rejected. Hide the current alert list when a pinned dashboard reporting
date differs from `monitoring_as_of`, and offer following the replay date.

Each alert provides `investigation_url`, `investigation_id` and `original_analysis_id`.
The investigation returns separately labeled `original_detection` (frozen),
`current_analysis` (saved at the active revision), and lifecycle `transitions`.
After reset, old investigations remain available with `current_analysis=null` and
`current_analysis_status=archived_run`; they never borrow data from the new run.

Investigation windows include eligible/tested/S/R/untested counts, raw isolates before
deduplication/demographic filters, sex counts, age bins (0-17, 18-64, 65+), collection
setting and missing metadata. These context counts describe eligible first isolates,
including untested isolates. For inpatients, collection minus admission >=2 days is
the hospital-day-3-or-later proxy; it does not establish infection origin. Counts
separate earlier timing, unknown timing and outpatient not-applicable timing.
Organism mix removes only the organism filter and has its own **count-only** population.
It is not the monitored species' resistance denominator. Shared patient counts disclose
recurrence between periods; no independence assumption is made.

All monitor/current investigation snapshots use `alert-v1` saved analysis IDs and the
existing JSON/CSV export routes. CSV produces baseline/recent `alert_window` rows with
provenance, measurements and empty suppressed percentages; an unavailable monitor
produces an `alert_unavailable` row. Export always reads the frozen analysis, including
after replay/reset. Investigation envelopes can change as the active run advances;
use the contained analysis ID for a reproducible export.

### Separate authored alert demonstration

The deterministic scenario generator creates a **new directory** with six valid CSVs
and an explicit `SCENARIO.json` description. It refuses to overwrite any directory.
Its patient identifiers and home-area values label it as authored synthetic data.
Import it into its own schema/manifest; never substitute it for the supplied dataset.

```powershell
.\.venv\Scripts\python.exe -m aei.alert_scenario --output .data/alert-demo
.\.venv\Scripts\python.exe -m aei --schema aei_alert_demo replay --dataset-dir .data/alert-demo --from-date 2026-09-27 --through 2026-10-01 --delay 0
```

At the 2026-09-26 seed there are 29 recent tests and no alert. On September 27 the
ED / urinary E. coli / CIP monitor reaches 30 in both periods, rises from 20% to 30%
resistance and creates one alert. September 28 becomes insufficient evidence,
September 29 clears, and September 30 recurs as ongoing under the same ID. October 1
adds only a negative culture and remains ongoing. Other default wards are explicitly
unavailable in this small fixture. The complete lifecycle is tested against PostgreSQL.

### Patient specimen history

```text
GET /api/v1/patients?search=000&as_of=2026-09-27&limit=20
GET /api/v1/patients/{patient_id}/history?as_of=2026-09-27&limit=25
```

Replace `{patient_id}` with an ID returned by search. Search uses a literal,
case-sensitive substring (1-100 characters); `%` and `_` are not wildcards. It returns
at most `limit` IDs (default 20, maximum 100), ordered by ID, with visible culture
counts, latest collected dates, `has_more`, and the run/revision. Narrow the search
when `has_more` is true. Eligibility requires any published specimen collected on or
before `as_of`; it is independent of the history lookback and includes negative specimens.
Search is a lookup response, not a saved analysis.

The `history-v1` view returns **specimens collected before the selected date**, in
`[prior calendar-year anniversary, as_of)`. For cutoff 2026-09-27, it includes
2025-09-27 through 2026-09-26. February 29 clamps to February 28 in the previous year.
Unlike the inclusive surveillance periods, the cutoff date itself is excluded.
Both search/history default to the demo date and reject future `as_of` values.

History includes every earlier published culture, negative/mixed cultures, repeated
isolates, all species/wards and each isolate's complete recorded AST panel. It applies
no surveillance first-isolate, drug, ward, species, sex or age filter. Unsupported
cohort filters are rejected. Latest visible culture/isolate revisions are resolved
before patient/date filtering; matching enrichment is attached only by exact event ID.
Collection dates do not establish when clinicians received results. This view shows
individual S/R test results without calculating a patient-level susceptibility percentage.

A current-date-only patient, or one whose specimens are all older than the lookback,
returns 200 with an empty prior-history list. Unknown and future-only patients receive
the same 404 error. IDs from staged future data do not appear in search.

History pages use `limit` (default 25, maximum 100) and sort by collection date, then
culture ID, both descending. Each page is a saved analysis with source/run context.
When `has_more` is true, pass `next_cursor` as the next request's `cursor`, retaining
the patient, cutoff and limit. The opaque cursor references a saved page and is bound
to its run, revision and filters. A replay step/reset/control change returns 409 for
an old cursor; restart pagination from the first page. Malformed or mismatched cursors
return 422. Existing saved pages remain downloadable across replay changes.

History JSON exports preserve the page exactly. CSV repeats context and page metadata
on `culture`, `isolate` and `ast` rows, retaining zero-isolate cultures and isolates
without AST. An empty page produces a `history_summary` row. A history export contains
the selected patient's identifiers and recorded laboratory data, unlike aggregate
evidence/comparison exports. Export additional pages separately when history spans pages.

### Rolling-year definition

The 28-day period is `as_of - 27 days` through `as_of`, inclusive. The 12-month
period starts the day **after the previous calendar-year anniversary** and ends
on `as_of`, inclusive. It is not a fixed 365-day approximation.

| Report date | 28-day period | 12-month period |
| --- | --- | --- |
| 2026-09-27 | 2026-08-31 to 2026-09-27 | 2025-09-28 to 2026-09-27 |
| 2024-03-01 | 2024-02-03 to 2024-03-01 | 2023-03-02 to 2024-03-01 (366 days) |

February 29 uses February 28 as its anniversary in a non-leap year, so a report
ending 2024-02-29 starts 2023-03-01. Each period selects first isolates independently;
annual percentages are not sums or averages of monthly percentages.

Custom `start_date` or `window_days` are supported on `/statistics` but cannot be
combined with a named `period`. The comparison route always uses the two standard
periods and rejects period overrides. Reports identify rules version `prototype-v2`.

### Counting contract

- Events are complete isolate snapshots, including the full antibiotic test panel.
- Resolve the latest revision before filtering sample dates or cohorts.
- Select the first isolate per patient, organism and specimen within each period.
- Same-day ties use culture ID, then isolate ID; collection times are absent.
- Default `dedup_scope=selected_cohort` filters by ward before selecting first isolates.
  `all_wards` selects first across wards, then filters the selected ward. The final
  ward-transfer policy remains to be confirmed with the challenge partners.
- Select antibiotics after choosing first isolates. An untested first isolate is
  not replaced by a later tested isolate; this policy also awaits confirmation.
- Each output group is one ward/organism/specimen/antibiotic combination. Ward
  reports cannot simply be added to produce hospital-wide percentages.
- Denominator: selected isolates actually tested against that antibiotic.
- Below 30 tested isolates, percentages are null and status is `insufficient_data`.
  Counts remain visible. Empty cohorts return `rows: []`, not a zero-percent result.
- Susceptibility = `100*S/(S+R)`; resistance = `100*R/(S+R)`, rounded to two decimals.

The importer validates patient/culture/isolate/test links. Negative cultures with
no isolates do not contribute to these percentages. The original ZIP is unchanged.
The source lacks result-arrival timestamps; imported events use **12:00 UTC on the
sample date as an explicit synthetic arrival convention**.

## Replay

Use a separate PostgreSQL schema to keep demo state independent of the main run:

```powershell
.\.venv\Scripts\python.exe -m aei --schema aei_replay replay --from-date 2026-08-31 --through 2026-09-27 --delay 0.2
```

The driver validates and stages the complete source, creates a new run seeded through
the day before `--from-date`, then publishes complete daily bundles. It prints state,
new-culture and new-isolate counts. Negative/mixed cultures and days without arrivals
still advance the clock. It accepts `--dataset-dir` too, and can be repeated in the
same schema: old source events and saved analyses remain intact. CLI `evidence` and
HTTP evidence use the same publication/snapshot service; `python -m aei state` shows
the active run. Legacy `stats`/`compare` intentionally read the full event history.

For HTTP replay, import the full source and run the API with lifespan enabled (the
documented Uvicorn command does this by default):

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/v1/replay/reset -Method Post -ContentType application/json -Body '{"seed_through":"2026-08-30","replay_through":"2026-09-27"}'
Invoke-RestMethod http://127.0.0.1:8000/api/v1/replay/step -Method Post
Invoke-RestMethod http://127.0.0.1:8000/api/v1/replay/start -Method Post -ContentType application/json -Body '{"interval_seconds":1}'
Invoke-RestMethod http://127.0.0.1:8000/api/v1/replay/pause -Method Post
Invoke-RestMethod http://127.0.0.1:8000/api/v1/state
```

- `reset` requires a complete source import. Without dates it seeds 28 days before
  source end (clamped to source start minus one day), then replays through source end.
  An explicit end must lie within the source span, and seed cannot exceed it.
- `step` requires `paused` and advances exactly one date. Concurrent publication or
  analysis can return 409 `publication_busy`; refresh state and retry. Send expected
  state tokens to prevent a duplicate UI request from advancing a second day.
- `start` resumes a paused run. Repeated starts while running keep the existing schedule.
  The configurable delay is 0.05-60 seconds, with a worker polling every 0.2 seconds.
  A persisted due time plus the database lock prevents multiple API workers from
  publishing the same tick. Playback resumes after server restart if still running.
- `pause` waits for a complete in-flight transaction before acknowledging. Reaching
  the selected end gives `completed`; reset begins another run. A publication failure
  rolls back visibility and clock, records `error`, and requires a reset after repair.

`replay_runs` and `active_run` hold operational state. Append-only `run_cultures` and
`run_isolates` store exact visible source versions and the revision that published
each one. Append-only `saved_analyses` stores report payloads. Every step commits bundle
membership, demo date, revision and alert evaluations together. Reset never deletes source history or
old reports. The metadata catalogs describe the full source, while evidence reads
only exact event IDs published in the active run.

This replay is a synthetic simulation, not a live Tyks connection. Legacy `/events`
writes retain their original history/statistics behavior but **do not publish into
the active run**, including corrections. State labels this as
`legacy_event_ingestion: history_only`. Use the [complete-culture HTTP feed](FEED_INTEGRATION.md)
to publish external arrivals with metadata, alerts and live notifications. Its standalone
dataset sender demonstrates arrivals without changing the original dates or files.

## Verify

```powershell
# Unit/API/reference tests; supplied-data test skips if the source is absent.
.\.venv\Scripts\python.exe -m unittest discover -s tests -v

# Include real PostgreSQL integration tests in disposable aei_test_* schemas.
$env:AEI_TEST_DATABASE_URL = 'postgresql://aei:aei_local_only@127.0.0.1:55432/aei'
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Tests cover leap years, independent period selection, missing tests, the 29/30
threshold, corrections, knowledge cutoffs, atomic conflicts, concurrent retries,
append-only history, read-only SQLite migration, API behavior and the supplied data.
Importer tests additionally cover ZIP/folder equivalence, short dates, validation,
negative cultures, metadata revisions, concurrent imports and complete rollback.
Evidence tests cover demographic selection, unknown metadata, combined ward counts,
Wilson intervals, distinct suppression states, source coverage and concurrent snapshot reads.
Publication tests cover complete/empty-day replay, rollback, stale run/revision tokens,
concurrent steps/workers, pause acknowledgment, restart persistence, exports, HTTP/CLI
agreement, structured errors and preventing staged future records from entering evidence.
Comparison/history tests cover common-denominator suppression, all four S/R cells,
drug-order reversal, missing tests, screening exclusions, independent windows,
strict history boundaries, negative specimens, repeated isolates, revision corrections,
hidden future IDs, cursor isolation, concurrent reset and preserved exports.
Alert tests cover disjoint calendar bounds, exact threshold arithmetic, counts at
29/30, first-isolate and demographic rules, missing tests, timing/context counts,
the full lifecycle, frozen configuration, archived investigations, schema upgrades,
concurrent readers/steps, detector rollback, append-only history and saved exports.
Feed tests cover complete-bundle validation, durable receipts, concurrent acceptance,
retry idempotence, late data, full corrections, restart recovery, rollback and quiet-day
expiration. A real HTTP server test exercises the sender, SSE delivery and reconnect
recovery; an authored feed scenario verifies the full alert lifecycle.
`python -m aei schema` prints the versioned input contract.

The supplied-data check replayed all 28 days through 2026-09-27, ending with 46,792
cultures and 24,571 isolates. An independent SQL query matched the ED / E. coli /
urine / CIP / female / age 65+ cohort: 15 tested (15 S, 0 R) in 28 days and 210 tested
(199 S, 11 R) in 12 months. Reset reproduced the seed evidence and retained old
JSON/CSV exports; the source archive and main event history were unchanged. On this
development machine before alert evaluation was added, publication of the 28 steps took about 3.5 seconds excluding
staging, an uncached evidence query 2.6 seconds, and a cached query 0.03 seconds.
These are local smoke-check timings, not production throughput guarantees.

The comparison/history supplied-data check also passed: for the same ED / E. coli /
urine / female / age 65+ cohort, CIP versus TMP had 15 paired tests in 28 days
(percentages suppressed) and 210 in 12 months. The annual four-cell counts were
166 both-S, 33 A-only-S, 9 B-only-S and 2 both-R, matching an independent SQL query.
A selected patient's 18 prior cultures (9 negative/mixed) across nine pages matched
the source specimens and every recorded AST result. Reset kept old history exports
unchanged and hid future-only patient IDs. No source files or main event history changed.

Still pending: partner confirmation of the proposed alert configuration and detailed
counting policies, laboratory-specific access/mapping, dashboard integration and
production operations. Flink is deferred. External HTTP feed publication and SSE are
implemented alongside evidence, comparison, history, alerts and saved exports.
Calculations load published events in memory and serialize with publication. The
durable queue and bounded batches keep acceptance separate from analytical work;
they do not provide a production throughput guarantee. See [the Flink integration boundary](flink/README.md).

The alert upgrade passed **147 tests**, including 30 alert/scenario tests and real
PostgreSQL integration tests. A separate supplied-data check matched independent SQL
for all four monitors on 2026-09-27 (urinary E. coli / CIP): ED baseline/recent tested
624/45, URO 338/33, PHC-N 611/67 and PHC-S 620/74, including exact S/R counts and changes.
All four were below the 10-point threshold on that date. Three subsequent replay
steps from a September 24 seed kept monitoring and publication revisions aligned;
frozen exports, the source archive and existing main event history remained unchanged.
With four monitors, that local check took about 22 seconds for initial full publication
and 19 seconds for three daily steps. These timings include full in-memory snapshots
and shared database locking; they are not a live-feed latency guarantee.

The feed/SSE upgrade passed **174 tests** with real PostgreSQL integration enabled.
A separate disposable-schema check sent the supplied dataset's final 39 cultures over
HTTP, received the committed SSE update, and finished with 46,792 cultures and 24,571
isolates visible. Retrying the same delivery preserved its receipt and revision; all
four alert monitors matched the publication revision. Independent SQL reproduced the
ED / E. coli / urine / CIP / female / age 65+ counts: 15 tested (15 S, 0 R) over 28 days
and 210 tested (199 S, 11 R) over 12 months. That local delivery took about 2.8 seconds,
excluding seed publication. Source bytes and the existing main event history were
unchanged. The HTTP reconnect test separately verified delivery of missed notifications.
