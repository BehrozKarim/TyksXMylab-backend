# Tyks AEI backend

Current flow: **CSV archive/folder or API -> append-only PostgreSQL event history -> Python
reference processor -> JSON statistics**. PostgreSQL runs in Docker with a persistent
volume. Annual results now mean **rolling last 12 calendar months**, alongside the
rolling last 28 days. Flink, alerts and a dashboard are not active yet.

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
Override the backend connection with `AEI_DATABASE_URL`. Compose also accepts
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
the returned per-table counts describe the selected import. This is not run-based
replay visibility or a patient-history API; those remain later development steps.

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

Filters: `ward_code`, `organism`, `specimen`, `antibiotic_code`, `dedup_scope`.
`known_at` optionally restricts results to revisions received by that timestamp.
`as_of` controls sample dates, not knowledge time. Without `known_at`, the latest
known revisions are used. No system date is selected implicitly.

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

Use a separate empty PostgreSQL schema to preserve the main event history:

```powershell
.\.venv\Scripts\python.exe -m aei --schema aei_replay replay --from-date 2026-08-31 --through 2026-09-27 --delay 0.2
```

The driver loads earlier events, then advances daily, including days without new
results, and prints recalculation summaries. It is a simulation, not a live lab
feed. Use another empty replay schema when repeating the demonstration. This existing
driver still replays isolate events only; culture-bundle publication and HTTP replay
controls are a later stage. It also accepts `--dataset-dir`.

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
`python -m aei schema` prints the versioned input contract.

Still pending: Flink processing, persisted result/alert history, alert definitions,
live notifications and the dashboard. Statistics currently run on demand or during
replay and load source history in memory; this is a correctness reference, not a
production throughput claim. See [the Flink integration boundary](flink/README.md).
