# External feed and dashboard updates

The backend accepts complete culture results over HTTP. PostgreSQL retains the
delivery, a background worker publishes it, and Server-Sent Events (SSE) notify
the dashboard after publication. Flink is deferred. The supplied-data sender is
a separate HTTP client; a real laboratory adapter must implement the same contract.

For a single-command app demonstration, run `scripts/start-demo.ps1`; see
[DEMO.md](DEMO.md). The manual commands below remain useful for operating each part
individually.

```text
Laboratory adapter / dataset sender
  -> POST complete culture bundles -> durable PostgreSQL inbox -> 202 + receipt
  -> background worker -> validate source identities and revisions
  -> commit history + visible membership + reporting clock + alerts + notification
  -> SSE state event -> dashboard refreshes its selected API views
```

## Endpoints

| Endpoint | Purpose |
| --- | --- |
| `POST /api/v1/feed/start` | Start a new receiving run, seeded from the imported dataset. |
| `POST /api/v1/feed/batches` | Queue up to 100 complete cultures; return HTTP 202 and a receipt. |
| `GET /api/v1/feed/receipts/{receipt_id}` | Read `pending`, `published`, `failed` or `cancelled`. |
| `POST /api/v1/feed/receipts/{receipt_id}/retry` | Requeue a failed delivery after its cause is resolved. |
| `GET /api/v1/updates` | SSE stream of committed run/revision changes, also covering replay. |
| `GET /api/v1/state` | Read the last committed state. |

OpenAPI at `/docs` describes the complete JSON schema. The full culture contract
is defined in [feed_models.py](aei/feed_models.py); the executable example producer
is [feed_sender.py](aei/feed_sender.py).

Each batch includes `run_id`, `source_id`, a stable `delivery_id`, optional `as_of`,
and `bundles`. Each bundle contains its culture event ID and revision, the culture,
patient and ward records, antibiotic dictionary entries, and **all** isolates with
their complete AST snapshot and exact-revision enrichment. Negative/mixed cultures
contain zero isolates. Missing AST stays missing. Current models accept S/R results
and the existing dataset's specimen/demographic conventions.

Patient/culture/isolate links, dates, dictionary restrictions, counts and metadata
must agree. Receipt timestamps must carry a timezone; clock comparisons use UTC.
IDs must remain stable across deliveries and be unique across the source history.
The same patient ID links a new specimen to that patient's previous specimens.
The batch limit is 100 cultures, 50 isolates per culture and 5 MB of normalized JSON.

HTTP 202 means **durably accepted**, not yet visible. Poll the returned `receipt_url`
until `published`; `published_revision` identifies the committed run revision.
Validation returns 422, reused delivery IDs with changed content return 409, and
oversized normalized batches return 413. Identity conflicts found during publication
produce a failed receipt with an error code. All data changes in that batch roll back.

## Reliability and corrections

- Retrying the same `(run_id, source_id, delivery_id)` and equivalent payload returns
  the original receipt. Reusing it for different content is rejected. After a lost
  response, send the same delivery again; do not invent new event IDs.
- Identical events under a new delivery ID do not duplicate published history or
  retrigger alerts. Clock advancement can still create a new publication.
- Corrections use the next contiguous culture revision, new culture/isolate event
  IDs, the same culture and isolate identities, and a complete replacement snapshot.
  Culture and isolate revision numbers must match. Revision 1 must be visible first.
- Adding/removing isolates from an existing culture, retractions and changes to
  immutable patient/dictionary records are rejected. Their semantics need a later
  contract. Submit prerequisite revisions before their corrections and check receipts.
- Late samples/corrections can change current evidence; they never rewind the clock.
  Earlier saved JSON/CSV reports retain the values and provenance originally shown.
- The inbox and notification log are persistent and append-only. Publication,
  receipt completion, alert evaluation and notification commit in one transaction.
  A process crash leaves uncommitted work pending for restart. Failed receipts need
  an explicit retry; fix invalid content using a new delivery ID.
- Multiple API workers coordinate through the existing PostgreSQL publication lock.
  State, receipt and alert reads can return the last committed revision while the
  next batch is processing. Evidence calculations still share the publication lock.

## Clock, runs and provenance

Import the complete seed dataset before starting a feed. `seed_through` controls
which imported cultures initially become visible; other staged samples stay hidden
until explicitly delivered. Source dates and IDs are never rewritten.

`clock_mode: "simulated"` requires an explicit starting `as_of`. Each batch can
advance that date, including an empty batch on a quiet day. This is the mode for
demonstrating historical data arriving now. Dataset `received_at` values retain
their documented synthetic convention; receipt `accepted_at` records actual arrival.

`clock_mode: "wall_utc"` is the default. It starts at today's UTC date, accepts
samples no later than the delivery's reporting date, and advances on quiet UTC days
so rolling windows expire without arrivals. The existing field `demo_date` holds
the reporting date in either mode. `replay_through` is not a feed end limit.

There is one active run and one named feed source per run. `feed/start` creates a
**new** run seeded from the original dataset; prior feed runs and analyses remain
archived. To resume after an ordinary restart, use the existing receiving `run_id`
from `/state` instead of starting another run. `replay/reset` returns to dataset
replay; pending deliveries for a replaced run are cancelled. Replay start/pause/step
controls reject feed mode. Start accepts optional expected run/revision tokens.

Published contexts include `mode`, `clock_mode`, source ID/label and
`feed_data_digest`, a cumulative hash of newly published batch payload hashes.
Receipts retain each payload hash and full accepted payload. File hashes and source
coverage describe the original imported dataset only (`coverage_scope` is
`imported_dataset_only`); `includes_feed_data` identifies additional feed publication.
These counts do not certify completeness of a real laboratory feed. Published counts
count event revisions, including corrections, rather than distinct patients/specimens.

## Try the supplied dataset over HTTP

Use a separate schema to keep the main run unchanged. Terminal 1, from the repository
root with Docker PostgreSQL running:

```powershell
$env:AEI_DATABASE_SCHEMA = 'aei_feed_demo'
$env:AEI_FEED_TOKEN = 'local-demo-token'
.\.venv\Scripts\python.exe -m aei import-dataset --zip ../synthetic_microbiology_dataset.zip
.\.venv\Scripts\python.exe -m uvicorn aei.api:create_app --factory --host 127.0.0.1 --port 8000
```

Terminal 2, from the same repository root:

```powershell
$env:AEI_FEED_TOKEN = 'local-demo-token'
$headers = @{ Authorization = "Bearer $env:AEI_FEED_TOKEN" }
$feed = Invoke-RestMethod http://127.0.0.1:8000/api/v1/feed/start -Method Post -Headers $headers -ContentType application/json -Body '{"source_id":"dataset-simulator","source_label":"Supplied synthetic dataset","clock_mode":"simulated","as_of":"2026-09-24","seed_through":"2026-09-24"}'
.\.venv\Scripts\python.exe -m aei.feed_sender --source ../synthetic_microbiology_dataset.zip --run-id $feed.run_id --from-date 2026-09-25 --through 2026-09-27 --delay 1
```

The sender also accepts a CSV directory as `--source`, `--base-url`, `--source-id`
and `--batch-size 1-100`. It waits for each batch to publish before sending the next,
prints receipts, and safely retries a lost acceptance response with the same ID.
Re-running it against the same run does not duplicate the published events.
Initial publication and each analytical update may take seconds on the full source.
`--delay` is additional pacing, not a guaranteed dashboard update interval.

## Dashboard handoff

Use one EventSource per dashboard, and close it when the component unmounts:

```javascript
const updates = new EventSource(`${apiBase}/api/v1/updates`);
updates.addEventListener("state", (message) => {
  const state = JSON.parse(message.data);
  scheduleRefresh(state.run_id, state.state_revision); // Dashboard callback.
});
// On component cleanup: updates.close();
```

Implement `scheduleRefresh` to coalesce successive notifications, read current
`/state`, then request the selected evidence/comparison/history/alert views with
its `expected_run_id` and `expected_revision`. If a read returns 409 `stale_state`,
refresh state and retry. Keep the previous complete view visible while loading and
discard responses for superseded revisions. A run ID change also invalidates history
pagination cursors. Previously saved reports remain available by analysis ID.

The stream sends an initial state, then `event: state` frames with monotonically
increasing `id` values. Browsers automatically reconnect using `Last-Event-ID`; the
server replays committed notifications after that cursor, including arrivals while
disconnected. A manually restored client can use `?after=<id>`. The header takes
precedence on subsequent browser reconnects. A cursor beyond this database's history
returns 422; reconnect without it after deliberately changing backend/schema.

Replayed notifications can describe older runs/revisions, so use them as refresh
signals rather than assuming every notification is still the active state. Payloads
contain run metadata and counts, with no patient-level data. Heartbeat comments are
sent every 15 seconds, and the reconnect delay is 2 seconds. The server checks its
durable notification log every 0.5 seconds. Disable SSE proxy buffering and configure
proxy timeouts to allow the heartbeat. CORS uses `AEI_CORS_ORIGINS`.

`AEI_FEED_TOKEN`, when set, requires a Bearer token on the four feed endpoint types.
It is optional for local development and does not add authorization to the existing
read/replay endpoints or SSE. The dashboard should not receive the feed secret.
Full user authorization, deployment security and real laboratory access remain
separate integration work.

## Remaining integration

The HTTP feed, persistent queue, background publication, SSE and dataset sender are
implemented. Your dashboard colleague needs to connect the EventSource callback to
the UI refresh logic. A real laboratory connection still needs its protocol, access
and field mapping, including reliable release times and correction semantics.
Calculations currently process published snapshots in Python; larger workloads may
need incremental processing. Flink/CDC can be considered later using the same
committed publication and notification contract.
