# Tyks AEI backend

Current flow: **CSV archive or API -> append-only PostgreSQL event history -> Python
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

The supplied synthetic ZIP is not committed to Git. By default it is expected
beside the project directory. Set `AEI_DATASET_PATH` or pass `--zip` if elsewhere.

```powershell
# Existing prototype users: preserve every recorded revision from SQLite.
.\.venv\Scripts\python.exe -m aei migrate-sqlite --sqlite .data/aei.sqlite3

# Fresh installations: import the supplied archive instead.
.\.venv\Scripts\python.exe -m aei import-dataset --zip ../synthetic_microbiology_dataset.zip

# API documentation: http://127.0.0.1:8000/docs
.\.venv\Scripts\python.exe -m uvicorn aei.api:create_app --factory --host 127.0.0.1 --port 8000
```

Migration opens SQLite read-only and retains every event ID, revision and payload.
Import/migration can be repeated safely: identical events are ignored; conflicts
roll back the entire batch. SQLite is retained only for migration and isolated unit
tests. The running backend uses PostgreSQL with no automatic SQLite fallback.

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
feed. Use another empty replay schema when repeating the demonstration.

## Verify

```powershell
# Unit/API/reference tests; dataset test skips if the supplied ZIP is absent.
.\.venv\Scripts\python.exe -m unittest discover -s tests -v

# Include real PostgreSQL integration tests in disposable aei_test_* schemas.
$env:AEI_TEST_DATABASE_URL = 'postgresql://aei:aei_local_only@127.0.0.1:55432/aei'
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Tests cover leap years, independent period selection, missing tests, the 29/30
threshold, corrections, knowledge cutoffs, atomic conflicts, concurrent retries,
append-only history, read-only SQLite migration, API behavior and the supplied data.
`python -m aei schema` prints the versioned input contract.

Still pending: Flink processing, persisted result/alert history, alert definitions,
live notifications and the dashboard. Statistics currently run on demand or during
replay and load source history in memory; this is a correctness reference, not a
production throughput claim. See [the Flink integration boundary](flink/README.md).
