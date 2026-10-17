# Tyks AEI: reference pipeline before Flink

This first development slice checks ingestion, event history and the calculation
rules on the supplied synthetic data. It is a runnable backend, not a dashboard.

Current flow: **CSV archive or API → append-only SQLite event history → reference
processor → JSON statistics**. SQLite makes this slice runnable without Docker.
The planned architecture remains **PostgreSQL event history → Flink CDC/Flink →
PostgreSQL results → API/dashboard**. SQLite is not a CDC or Flink implementation.

## Run locally

From this directory (`tyks-MY-LAB/aei-prototype`), using Python 3.11:

```powershell
# Only needed if these packages are not already installed:
python -m pip install -r requirements.txt

# Import once. Running it again does not duplicate events.
python -m aei import-dataset

# The source ends on 2026-09-27; use an explicit reporting date.
python -m aei stats --as-of 2026-09-27 --ward ED --organism "Escherichia coli" --specimen urine --antibiotic CIP

# Interactive API documentation: http://127.0.0.1:8000/docs
python -m uvicorn aei.api:create_app --factory --host 127.0.0.1 --port 8000
```

API routes:

- `GET /health`: implementation and event count.
- `POST /events`: append one complete isolate revision.
- `POST /events/batch`: append 1–500 revisions in one transaction.
- `GET /isolates/{isolate_id}/history`: all recorded revisions.
- `GET /statistics?as_of=2026-09-27`: rolling 28 days by default.
- `GET /statistics?as_of=2026-09-27&period=rolling_12_months`: rolling 12 months.
- `GET /statistics/comparison?as_of=2026-09-27`: both periods from one source snapshot.

Statistics filters: `ward_code`, `organism`, `specimen`, `antibiotic_code`.
`start_date` or `window_days` can override the default 28-day start. These custom
overrides cannot be combined with a named `period`. `known_at` optionally
restricts results to revisions received by that timestamp. `as_of` controls sample
date inclusion, not knowledge time; without `known_at`, latest known revisions are
used. Dates are inclusive. No implicit system date is used.

SQLite data is stored in `.data/aei.sqlite3`, ignored by Git. Override with global
CLI `--db PATH` before the command, or `AEI_DB_PATH` for the API. Bind the API to
localhost; authentication and production deployment are not part of this slice.

## Rolling annual reporting

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

```powershell
python -m aei stats --as-of 2026-09-27 --period rolling_12_months
python -m aei compare --as-of 2026-09-27
```

## Replay the moving window

Use a new database path for each replay:

```powershell
python -m aei --db .data/replay.sqlite3 replay --from-date 2026-08-31 --through 2026-09-27 --delay 0.2
```

The driver first loads events before the starting date, then advances one day at
a time, adds that day's events and recalculates. It includes days with no events
so expired samples leave the window. Output is a progress summary, not an alert.
`--delay` sets wall-clock pacing only. The source supplies sample dates, not arrival
timestamps: imported events use **12:00 UTC on the sample date as a synthetic
arrival convention**. This does not simulate real laboratory turnaround times.

## Implemented calculation contract

- One event is a complete isolate revision, including its full antibiotic panel.
- Resolve latest revisions before applying sample date/cohort filters.
- Select the first isolate per patient, organism and specimen within each period.
- Same-day ties use culture ID, then isolate ID (the source lacks collection times).
- Default `dedup_scope=selected_cohort` applies ward filtering before selection.
  `dedup_scope=all_wards` selects first across wards, then filters the selected ward.
  This is an explicit prototype choice, not a settled clinical rule.
- Apply antibiotic selection **after** choosing the first isolate. An untested
  first isolate is not replaced by a later tested isolate.
- Report each ward/organism/specimen/antibiotic group separately. Hospital-wide
  percentages cannot be obtained by summing these deduplicated ward reports.
- Denominator is the number of eligible isolates tested against that antibiotic.
- Percentages are `null` below 30 tested isolates, including zero tested. Counts
  remain visible. An empty cohort returns `rows: []`, not a zero-percent result.
- Susceptibility = `100*S/(S+R)`; resistance = `100*R/(S+R)`, rounded to two decimals.
- Input `event_id` and `(isolate_id, revision)` are unique. Identical deliveries
  are ignored; conflicts return HTTP 409 and roll back the submitted batch.

The importer validates links between patients, cultures, isolates and results.
It imports isolate events only: negative cultures without isolates are not used
in these susceptibility percentages. Original CSVs and ZIP remain unchanged.

## Scope still pending

AEI naming/formula approval, ward-transfer policy, same-day tie handling, alert
baseline/thresholds and real arrival behavior remain open. Annual reporting uses
the confirmed rolling last 12 calendar months.

Flink, PostgreSQL CDC, stored statistics/alert history, automated live notifications,
alerts and the dashboard are **not implemented yet**. Event history is persisted;
statistics are currently calculated on demand or by the replay driver. The
reference processor reads the stored history each time and is for correctness
validation, not a performance claim for a production stream.

## Verify

```powershell
python -m unittest discover -s tests -v
python -m aei schema
```

Tests cover the 29/30 boundary, missing tests, repeated patients, period boundaries,
first-isolate replacement as the window advances, corrections, historical knowledge
cutoffs, ward-filter policy, retry safety, atomic conflicts, append-only storage,
API validation and the actual supplied archive.

See [the Flink integration boundary](flink/README.md) for the next stage.
