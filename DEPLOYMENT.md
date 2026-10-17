# Public Render demo

Deploy the backend and frontend repositories as separate Render Blueprints. The
backend Blueprint creates one free Python web service and one free PostgreSQL 17
database in Frankfurt. The frontend Blueprint creates a free static site.

Viewing, AI summaries and the planned **Send next lab batch** button are public:
there is no login or shared password. The separate external laboratory feed retains
its server-side bearer token. Anyone with the demo link can advance the shared run
and request summaries. Only the demonstration dataset belongs in this deployment.

## 1. Backend and database

Push the deployment changes first, then in Render select **New > Blueprint**, connect
`TyksXMylab-backend`, choose the branch containing these changes and use `render.yaml`.
Review that both compute plans are **Free**. Leave the repository root directory empty.

Free web services reject `maxShutdownDelaySeconds`; leave that field out of the
Blueprint. Render uses its default 30-second shutdown limit. The Uvicorn command
allows 20 seconds to drain requests, leaving time for application cleanup.

Supply `OPENAI_API_KEY` as a backend environment secret. If AI is not wanted yet, set
`AEI_SUMMARY_PROVIDER=none`; the existing report-based summary remains available.
Supply `AEI_CORS_ORIGINS` with the eventual frontend HTTPS origin (no path or trailing
slash). If that URL is not known yet, use `http://localhost:5173` temporarily and
replace it after creating the frontend. CORS is a browser integration setting, not
access control. Do not use it as a password substitute.

The Blueprint generates `AEI_FEED_TOKEN` and connects `AEI_DATABASE_URL` to the
database's **internal** connection URL. Neither is a frontend variable. The backend
creates its tables on startup; no paid pre-deploy command or shell is required.
`/health` becomes available before data is imported; `/api/v1/state` requires the seed.

## 2. Import once from your computer

The source ZIP is intentionally not in Git. Do not upload it through a public API,
copy the development database, or run an import on every server restart.

In the Render database's networking settings, temporarily allow your current public
IP (`/32`). Copy the **external** PostgreSQL connection URL. In a fresh PowerShell
terminal at the backend repository root, enter it without putting it in command history:

```powershell
$renderDatabaseSecret = Read-Host 'Render external PostgreSQL URL' -AsSecureString
$renderDatabaseCredential = [System.Net.NetworkCredential]::new('', $renderDatabaseSecret)
$env:AEI_DATABASE_URL = $renderDatabaseCredential.Password
$env:PGSSLMODE = 'require'
$env:AEI_DATABASE_SCHEMA = 'aei_demo_hosted'
.\.venv\Scripts\python.exe -m aei.hosted_demo --source .data/synthetic_microbiology_dataset.zip
Remove-Item Env:AEI_DATABASE_URL, Env:PGSSLMODE, Env:AEI_DATABASE_SCHEMA
Remove-Variable renderDatabaseSecret, renderDatabaseCredential
```

Use your actual source path if different. The command validates/imports the complete
dataset, publishes the initial history and saves the final seven days as batches of
at most 20 cultures. Dates and results are unchanged. Repeating it resumes the same
plan. Only explicit `--restart` starts a new run; old history is retained.
You can choose `--days 7 --batch-size 20` during initial preparation.

After import, remove the temporary database IP rule. The hosted API keeps connecting
over Render's internal network. Close the import terminal. Free Render services have
no interactive shell, so this local setup command deliberately works against the
remote database without requiring one.

## 3. Frontend

Create a second Blueprint from `TyksXMylab-frontend` and its `render.yaml`, on the branch
containing the deployment changes. Set `VITE_API_BASE_URL` to the actual backend URL,
for example `https://YOUR-API.onrender.com/api/v1`. This is a public address, not a secret.
The SPA rewrite handles refreshes on `/ward`, `/alerts` and investigation URLs.

Copy the actual frontend HTTPS origin into the backend's `AEI_CORS_ORIGINS` and redeploy
the backend. Changing a `VITE_*` variable requires rebuilding the frontend.
Do not set `OPENAI_API_KEY`, the database URL or the feed token in frontend variables.

The hosted button calls `/api/v1/demo/status` and `/api/v1/demo/next` directly on the
API. It no longer depends on the Windows launcher or Vite's development proxy.
The local launcher continues to use `/__demo` as before.

## 4. Verify the public demo

1. Open the backend `/health` and wait for it to wake up.
2. Open the dashboard and reload a deep link such as `/alerts`.
3. Open **Demo feed** in the header and press **Send next lab batch** once. Processing should finish and the state revision
   should increase. The dashboard should refresh through its SSE connection.
4. Repeat the previous delivery ID: it must not publish another batch. All visitors
   share the same run; the button shows the current next batch.
5. Click **AI summary** once to open the panel and start generation. Check the returned label:
   **AI-assisted summary** confirms model output; **Basic summary** is the existing
   fallback. Reopening a pending or completed summary reuses the same report-bound job.
6. Restart the backend and confirm the published count and next batch are retained.

Batch plans, receipts, saved analyses and AI jobs live in PostgreSQL. An accepted
batch resumes after service sleep/restart; the source archive is not needed at runtime.
Failed batches stop the sequence and retain their receipt for investigation/retry.
The hosted demo never sends the next batch automatically.

## Free tier limits

Render's free web service sleeps after 15 minutes without inbound traffic and can
take roughly a minute to wake up. Background work pauses while it sleeps. Open the
app before presenting. The free PostgreSQL database has 1 GB storage, expires after
30 days and has no managed backups. Do not rely on the demo URL as permanent storage.
Saved reports and historical runs accumulate; monitor database usage in Render.

The free API has limited CPU and memory. Start with one worker and a small audience;
large uncached analyses still hold the publication lock. Local functional checks do
not guarantee latency under Render's resource limits. AI API usage is separately
billed and is not included in free hosting. No Flink or separate worker service is
required for this demo.

Local verification used the full 46,792-culture source in a disposable schema. A
public batch request advanced revision 1 to 2 and emitted a real HTTP SSE update;
duplicate delivery did not advance again, and a backend restart preserved progress.
The tested schema occupied approximately 125 MB and that single-backend process
peaked at approximately 372 MB working set on Windows. These measurements support
trying the free tier with a small audience; they are not Render load-test results.
The smoke test used the basic summary fallback and made no paid AI call.

Official references: [free service limits](https://render.com/docs/free),
[Blueprint configuration](https://render.com/docs/blueprint-spec),
[static-site routing](https://render.com/docs/redirects-rewrites).
