# Start the app with a synthetic live feed

From the backend repository in PowerShell:

```powershell
.\scripts\start-demo.ps1
```

If local PowerShell policy blocks the script, run it with a process-scoped policy:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\start-demo.ps1
```

This starts Docker PostgreSQL, prepares a dedicated demo schema, starts the backend
and sibling frontend, and opens the dashboard. Press **Send next lab batch** in
**Demo controls** to send synthetic records through the actual HTTP feed. Nothing
is sent automatically. It uses the existing publication engine and SSE connection.
It does not rewrite sample dates, inject UI values or need Flink.

## First-time setup

- Docker Desktop must be running with Linux containers.
- Create the backend `.venv` with Python 3.11 and install `requirements.txt`, as in README.
- Clone `TyksXMylab-frontend` beside this repository and run `npm.cmd ci` in that folder.
  Use the frontend version with `DemoControls.tsx` and the `/__demo` Vite proxy.
  Use a Node version supported by its README (the checked environment uses Node 22).
- Keep the supplied synthetic ZIP or CSV directory locally. Source data is not in Git.

The launcher checks prerequisites and reports missing dependencies. It does not install
packages, pull Git changes, change PowerShell policy globally or stop unrelated services.

Source selection: explicit `-Source`, then `AEI_DATASET_PATH`, then the first existing
location among `synthetic_microbiology_dataset/` inside the backend, the ZIP beside
the backend, and `.data/synthetic_microbiology_dataset.zip` inside the backend. An
explicit missing source fails instead of falling back to different data.

## What to watch

The default demo seeds records through three days before the source's last date.
For the supplied dataset that is **24 September 2026**. The remaining records for
25-27 September arrive in batches of at most 20 cultures, retaining their original
dates, identities and AST results. The reporting clock is explicitly simulated.

After the dashboard starts, the simulator waits for a button click. **Demo controls**
shows the next batch's culture count and date. Each click durably submits one batch;
the button stays disabled while publication is processing. On completion it shows
the published count/revision and offers the next batch. Repeated clicks and requests
from stale tabs cannot skip ahead: each click identifies the advertised delivery ID.
There is no timer or automatic-feed toggle in this version.

Open `/ward?date_mode=follow`, or select **Use latest date** if a report is pinned.
The page should show **Synthetic live demo**, **Live updates connected** and
**Receiving results**. Watch the report date and footer Snapshot number advance
without reloading. Open **How calculated** to inspect the exact displayed revision.

The terminal prints each published receipt, culture count, report date and revision.
Browser developer tools can also show new `state` events on `/api/v1/updates` followed
by refreshed evidence requests. A percentage may stay the same if arrivals do not
affect the selected population or first-isolate denominator. An insufficient-data
message remains correct below 30 eligible tested isolates.

At completion the dashboard stays open. **Receiving results** means the backend can
still accept deliveries; it does not mean the finite simulator has more records.
The panel shows **All demo batches published** and disables the button; the terminal
also reports **Demo complete**. Press **Ctrl+C** to stop the
launcher and the backend/frontend processes it started. PostgreSQL and reports persist.

## Resume or restart

Run the same command again to resume the same demo. Published deliveries are skipped;
pending deliveries keep their stable IDs and receipts. Reopening a completed demo
does not create fake new results. Source validation still runs, but a completed import
is reused. Delivery errors remain visible in the panel and terminal, with sending
disabled. A plain relaunch resumes the same IDs after a transport failure; it does
not automatically retry a receipt marked failed. Inspect that receipt or start a
fresh demonstration. Already accepted work may finish publishing while stopped.

To start again from the seed date:

```powershell
.\scripts\start-demo.ps1 -Restart
```

Restart creates a new receiving run in the demo schema and preserves earlier saved
analyses/history. It does not drop schemas, change the normal `aei` run or stop Docker.
Stop the existing launcher before starting another; a database lock prevents two
launchers from owning the same demo schema. Changed dates/batch size require `-Restart`.
A different dataset requires a different demo schema because source history is immutable.

The default schema is `aei_demo_live`. `AEI_DATABASE_SCHEMA` is deliberately ignored;
custom schema names must begin `aei_demo_`. Existing schemas without the launcher's
ownership record are rejected. `AEI_DATABASE_URL` still selects PostgreSQL; the default
is the Compose instance on loopback port 55432. Match that variable to any custom
`POSTGRES_PORT`/`POSTGRES_PASSWORD`, as described in README.

## Options

| PowerShell option | Default | Purpose |
|---|---|---|
| `-Source <path>` | Local discovery | ZIP or CSV directory. |
| `-FrontendPath <path>` | Sibling `TyksXMylab-frontend` | Frontend checkout; no files are edited. |
| `-Schema <name>` | `aei_demo_live` | Dedicated, launcher-owned PostgreSQL schema. |
| `-Days <n>` | `3` | Final 1-366 source days to deliver. |
| `-BatchSize <n>` | `20` | 1-100 cultures per batch. |
| `-BackendPort <port>` | `8000` | Backend loopback port. |
| `-FrontendPort <port>` | `5173` | Dashboard loopback port. |
| `-Restart` | Off | Create a new demo run, retaining older reports. |
| `-SkipDocker` | Off | Use already running PostgreSQL. |
| `-NoBrowser` | Off | Print the dashboard URL without opening a browser. |
| `-ExitAfterFeed` | Off | Stop owned servers when delivery finishes, useful in checks. |

Example with more, smaller batches to step through:

```powershell
.\scripts\start-demo.ps1 -Restart -Days 7 -BatchSize 10
```

Choose different ports if an existing development server is already running:

```powershell
.\scripts\start-demo.ps1 -BackendPort 8002 -FrontendPort 5174
```

The frontend receives its API URL and API mode through child-process environment
variables, overriding `.env` settings without changing files. CORS is configured for
the chosen frontend port. A fresh feed token is supplied only to the backend/sender,
not to the browser or frontend child. The launcher also starts a loopback-only
control listener on a free port. Vite proxies `/__demo/status` and `/__demo/next` to
it using a separate, server-side credential; neither credential reaches browser code.
The control listener accepts send requests only from the local dashboard origin.
`VITE_DEMO_CONTROLS=1` enables the panel for this launch. Ordinary frontend startup
has no demo controls, and the backend's clinical API gains no simulator endpoints.
The `-Delay` and `-StartupDelay` options from the timed launcher have been removed.
Backend/frontend logs are under
`.data/live-demo/<schema>/<timestamp>/`. A failed service stops the other owned service;
port conflicts are reported before starting or resetting a demo.

The equivalent Python entry point is `python -m aei.demo --help` using the backend
virtual environment. PowerShell `-Restart`, for example, maps to Python `--restart`.

The current frontend includes trend refresh throttling (30 seconds), a manual chart
refresh, retention of results after refresh errors, and comparison coverage mapping.
Evidence cards refresh after publication; the trend can intentionally show an older,
labelled snapshot until its next refresh. Uncached analyses can still delay publication.
The sender waits for each publication and does not change calculation rules.
