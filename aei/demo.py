"""Local demo supervisor: real HTTP deliveries, isolated state, unchanged source data."""

import argparse
import os
from pathlib import Path
import re
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import time
import webbrowser
from datetime import date, timedelta

import httpx
import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from .config import database_url
from .dataset import load_bundle
from .demo_control import DemoControls
from .feed_sender import batches, send_batch
from .postgres import PostgresEventStore
from .publication import PublicationService


ROOT = Path(__file__).resolve().parents[1]
OWNER = "demo-launcher-v1"
SOURCE_ID = "startup-demo"


class DemoError(RuntimeError):
    pass


def say(message):
    print(message, flush=True)


def resolve_source(explicit=None, root=ROOT):
    requested = explicit or os.environ.get("AEI_DATASET_PATH")
    candidates = ([Path(requested)] if requested else [root / "synthetic_microbiology_dataset",
        root.parent / "synthetic_microbiology_dataset.zip", root / ".data/synthetic_microbiology_dataset.zip"])
    for path in candidates:
        if path.is_file() or path.is_dir():
            return path.resolve()
    raise DemoError("Dataset not found. Supply --source (PowerShell: -Source) with the ZIP or CSV directory.")


def demo_plan(dataset, days, batch_size):
    manifest = dataset.report.manifest
    if not manifest or not manifest.source_start or not manifest.source_end:
        raise DemoError("The demo requires a dataset containing dated cultures.")
    seed = max(manifest.source_start - timedelta(days=1), manifest.source_end - timedelta(days=days))
    return {"manifest_id": manifest.manifest_id, "seed_through": seed.isoformat(),
            "through": manifest.source_end.isoformat(), "batch_size": batch_size}


class DemoSession:
    """A dedicated schema and a session lock held for the entire launcher lifetime."""

    def __init__(self, url, schema):
        if not re.fullmatch(r"aei_demo_[a-z0-9_]{1,54}", schema):
            raise DemoError("Demo schema must start with aei_demo_ and contain only lowercase letters, digits and underscores.")
        self.url, self.schema = url, schema
        self.guard = None

    def __enter__(self):
        self.guard = psycopg.connect(self.url, connect_timeout=5, autocommit=True)
        try:
            locked = self.guard.execute("SELECT pg_try_advisory_lock(hashtext(%s))",
                                        (f"aei-demo:{self.schema}",)).fetchone()[0]
            if not locked:
                raise DemoError("Another demo launcher already owns this schema. Stop it before launching again.")
            tables = {r[0] for r in self.guard.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema=%s", (self.schema,))}
            if tables and "demo_launcher" not in tables:
                raise DemoError("This schema contains data not owned by the demo launcher. Choose another --schema.")
            with self.guard.transaction():
                self.guard.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(self.schema)))
                self.guard.execute(sql.SQL("""CREATE TABLE IF NOT EXISTS {}.demo_launcher (
                    id INTEGER PRIMARY KEY CHECK (id=1), owner TEXT NOT NULL, payload JSONB NOT NULL)""")
                    .format(sql.Identifier(self.schema)))
                self.guard.execute(sql.SQL("INSERT INTO {}.demo_launcher VALUES (1,%s,%s) ON CONFLICT DO NOTHING")
                    .format(sql.Identifier(self.schema)), (OWNER, Jsonb({})))
                row = self.guard.execute(sql.SQL("SELECT owner FROM {}.demo_launcher WHERE id=1")
                    .format(sql.Identifier(self.schema))).fetchone()
                if row[0] != OWNER:
                    raise DemoError("Unrecognized demo schema owner; refusing to change it.")
            return self
        except BaseException:
            self.guard.close()
            raise

    def __exit__(self, *_):
        self.guard.close()  # Releases the session advisory lock.

    def read(self):
        return self.guard.execute(sql.SQL("SELECT payload FROM {}.demo_launcher WHERE id=1")
                                  .format(sql.Identifier(self.schema))).fetchone()[0]

    def save(self, value):
        self.guard.execute(sql.SQL("UPDATE {}.demo_launcher SET payload=%s WHERE id=1")
                           .format(sql.Identifier(self.schema)), (Jsonb(value),))

    def prepare(self, dataset, plan, restart=False):
        previous = self.read()
        if previous and previous["plan"]["manifest_id"] != plan["manifest_id"]:
            raise DemoError("This demo schema belongs to a different dataset. Choose another --schema; source history is preserved.")
        if previous and previous["plan"] != plan and not restart:
            raise DemoError("Demo dates or batch size changed. Use --restart to begin a new demo run.")
        store = PostgresEventStore(self.url, self.schema)
        if not previous:
            say("Importing the synthetic source into the dedicated demo schema (first launch only)...")
            store.import_dataset(dataset)
        if not previous or restart:
            self.save({"plan": plan, "needs_start": True, "run_id": None})
        return store

    def ensure_run(self, client, expected_run_id):
        response = client.get("/api/v1/state")
        response.raise_for_status()
        state = response.json()
        if state["run_id"] != expected_run_id:
            raise DemoError("The API is not serving the expected demo run. Check the backend port and logs.")
        saved = self.read()
        if saved["needs_start"]:
            response = client.post("/api/v1/feed/start", json={
                "source_id": SOURCE_ID, "source_label": "Synthetic live demo", "clock_mode": "simulated",
                "as_of": saved["plan"]["seed_through"], "seed_through": saved["plan"]["seed_through"],
                "expected_run_id": state["run_id"], "expected_revision": state["state_revision"]})
            response.raise_for_status()
            state = response.json()
            self.save({**saved, "needs_start": False, "run_id": state["run_id"]})
            say(f"Started demo run at {state['demo_date']} (revision {state['state_revision']}).")
        elif (state["run_id"] != saved["run_id"] or state["mode"] != "feed"
              or state["status"] != "receiving" or state["clock_mode"] != "simulated"
              or state["feed_source_id"] != SOURCE_ID):
            raise DemoError("The saved demo run was replaced or stopped. Use --restart to begin again.")
        else:
            say(f"Resuming demo at {state['demo_date']} (revision {state['state_revision']}).")
        return state

    def receipt_status(self, run_id, delivery_id):
        # Also verifies that the session lock connection is still alive before each send.
        row = self.guard.execute(sql.SQL("""SELECT j.status FROM {}.feed_inbox i
            JOIN {}.feed_jobs j USING(receipt_id) WHERE i.run_id=%s AND i.source_id=%s AND i.delivery_id=%s""")
            .format(sql.Identifier(self.schema), sql.Identifier(self.schema)),
            (run_id, SOURCE_ID, delivery_id)).fetchone()
        return row[0] if row else None


def deliver(client, session, dataset, state, *, delay=0, wait, publication_timeout=300, control=None):
    plan = session.read()["plan"]
    sent = skipped = 0
    for batch in batches(dataset, state["run_id"], SOURCE_ID,
                         date.fromisoformat(plan["seed_through"]) + timedelta(days=1),
                         date.fromisoformat(plan["through"]), plan["batch_size"]):
        if session.receipt_status(state["run_id"], batch.delivery_id) == "published":
            skipped += 1
            if control:
                control.update(skipped_batches=skipped)
            continue
        wait(0)
        if control:
            control.wait_for_click(batch, wait)
        receipt = send_batch(client, batch, wait_seconds=publication_timeout)
        sent += 1
        if control:
            control.update(published_batches=sent, last_publication={"cultures": len(batch.bundles),
                "as_of": batch.as_of.isoformat(), "revision": receipt["published_revision"]})
        say(f"Published {len(batch.bundles)} cultures | report date {batch.as_of} | "
            f"revision {receipt['published_revision']} | receipt {receipt['receipt_id']}")
        wait(delay)
    say(f"Demo complete: {sent} batches published, {skipped} previously published batches skipped.")
    if control:
        control.update(phase="complete", next_batch=None)
    return sent, skipped


def child_options():
    return ({"creationflags": subprocess.CREATE_NO_WINDOW} if os.name == "nt" else {"start_new_session": True})


class Processes:
    def __init__(self, log_dir):
        self.log_dir, self.children = log_dir, []

    def start(self, name, command, cwd, env):
        path = self.log_dir / f"{name}.log"
        stream = path.open("ab")
        try:
            process = subprocess.Popen(command, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
                stdout=stream, stderr=subprocess.STDOUT, **child_options())
        finally:
            stream.close()
        self.children.append((name, process, path))

    def check(self):
        for name, process, path in self.children:
            if process.poll() is not None:
                raise DemoError(f"{name} exited with code {process.returncode}. See {path}")

    def wait(self, seconds):
        deadline = time.monotonic() + seconds
        while True:
            self.check()
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return
            time.sleep(min(remaining, 0.2))

    def ready(self, url, timeout=90):
        deadline = time.monotonic() + timeout
        with httpx.Client(timeout=2, trust_env=False) as client:
            while time.monotonic() < deadline:
                self.check()
                try:
                    if client.get(url).status_code == 200:
                        return
                except httpx.HTTPError:
                    pass
                self.wait(0.25)
        raise DemoError(f"Timed out waiting for {url}. See {self.log_dir}")

    def close(self):
        for _, process, _ in reversed(self.children):
            if process.poll() is not None:
                continue
            if os.name == "nt":
                # Only PIDs returned by this supervisor's Popen calls are targeted.
                subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"],
                    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=15, **child_options())
            else:
                os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                if os.name != "nt":
                    os.killpg(process.pid, signal.SIGKILL)
                else:
                    process.kill()
                process.wait(timeout=5)


def free_port(port):
    with socket.socket() as listener:
        try:
            listener.bind(("127.0.0.1", port))
        except OSError as exc:
            raise DemoError(f"Port {port} is already in use. Stop that service or select another demo port.") from exc


def parser():
    command = argparse.ArgumentParser(description=__doc__)
    command.add_argument("--source", type=Path, help="Synthetic ZIP or CSV directory")
    command.add_argument("--frontend", type=Path, default=ROOT.parent / "TyksXMylab-frontend")
    command.add_argument("--database-url", default=database_url())
    command.add_argument("--schema", default="aei_demo_live", help="Dedicated aei_demo_* schema; ignores AEI_DATABASE_SCHEMA")
    command.add_argument("--backend-port", type=int, default=8000)
    command.add_argument("--frontend-port", type=int, default=5173)
    command.add_argument("--days", type=int, default=3, help="Number of final source days to deliver (default: 3)")
    command.add_argument("--batch-size", type=int, default=20)
    command.add_argument("--restart", action="store_true", help="Create a fresh demo run; retain saved history")
    command.add_argument("--skip-docker", action="store_true", help="Use already running PostgreSQL")
    command.add_argument("--no-browser", action="store_true")
    command.add_argument("--exit-after-feed", action="store_true", help="Stop owned servers after delivery (automation)")
    return command


def run(args):
    if not 1 <= args.days <= 366 or not 1 <= args.batch_size <= 100:
        raise DemoError("Use --days 1-366 and --batch-size 1-100.")
    if (not all(1024 <= p <= 65535 for p in (args.backend_port, args.frontend_port))
            or args.backend_port == args.frontend_port):
        raise DemoError("Choose distinct backend/frontend ports between 1024 and 65535.")
    session = DemoSession(args.database_url, args.schema)
    source = resolve_source(args.source)
    frontend = args.frontend.resolve()
    vite = frontend / "node_modules/vite/bin/vite.js"
    node = shutil.which("node")
    if not node or not vite.is_file():
        raise DemoError(f"Node and frontend dependencies are required. Run npm.cmd ci in {frontend} first.")
    for port in (args.backend_port, args.frontend_port):
        free_port(port)
    if not args.skip_docker:
        docker = shutil.which("docker")
        if not docker:
            raise DemoError("Docker Desktop is required, or use --skip-docker for an existing PostgreSQL server.")
        say("Starting Docker PostgreSQL and waiting for readiness...")
        result = subprocess.run([docker, "compose", "up", "-d", "--wait", "--wait-timeout", "90", "postgres"],
                                cwd=ROOT, timeout=120, **child_options())
        if result.returncode:
            raise DemoError("PostgreSQL startup failed. Check Docker Desktop and the Compose output above.")
    say(f"Validating synthetic source: {source}")
    dataset = load_bundle(source)
    plan = demo_plan(dataset, args.days, args.batch_size)
    base = f"http://127.0.0.1:{args.backend_port}"
    dashboard = f"http://127.0.0.1:{args.frontend_port}"
    with session:
        store = session.prepare(dataset, plan, args.restart)
        expected = PublicationService(store).state()
        log_dir = ROOT / ".data/live-demo" / args.schema / time.strftime("%Y%m%d-%H%M%S")
        log_dir.mkdir(parents=True, exist_ok=True)
        processes = Processes(log_dir)
        token = secrets.token_urlsafe(32)
        backend_env = {**os.environ, "AEI_DATABASE_URL": args.database_url, "AEI_DATABASE_SCHEMA": args.schema,
            "AEI_FEED_TOKEN": token, "AEI_CORS_ORIGINS": f"{dashboard},http://localhost:{args.frontend_port}",
            "PYTHONIOENCODING": "utf-8"}
        frontend_env = {k: v for k, v in os.environ.items() if not k.startswith("AEI_")}
        frontend_env.update(VITE_DATA_MODE="api", VITE_API_BASE_URL=f"{base}/api/v1", BROWSER="none")
        try:
            say(f"Starting backend at {base}; logs: {log_dir}")
            processes.start("backend", [sys.executable, "-m", "uvicorn", "aei.api:create_app", "--factory",
                "--host", "127.0.0.1", "--port", str(args.backend_port)], ROOT, backend_env)
            processes.ready(f"{base}/health")
            with httpx.Client(base_url=base, headers={"Authorization": f"Bearer {token}"},
                              timeout=60, trust_env=False) as client:
                state = session.ensure_run(client, expected.run_id)
                with DemoControls(state["run_id"], [dashboard, f"http://localhost:{args.frontend_port}"]) as control:
                    # Vite's server-side proxy holds the control credential. The
                    # browser receives neither the control token nor feed token.
                    frontend_env.update(VITE_DEMO_CONTROLS="1", DEMO_CONTROL_URL=control.url,
                                        DEMO_CONTROL_TOKEN=control.token)
                    say(f"Starting dashboard at {dashboard}/ward?date_mode=follow")
                    processes.start("frontend", [node, str(vite), "--host", "127.0.0.1", "--port",
                        str(args.frontend_port), "--strictPort"], frontend, frontend_env)
                    processes.ready(dashboard)
                    if not args.no_browser:
                        webbrowser.open(f"{dashboard}/ward?date_mode=follow")
                    say("Dashboard ready. Press 'Send next lab batch' to deliver results. Ctrl+C stops this launcher and its servers.")
                    try:
                        deliver(client, session, dataset, state, wait=processes.wait, control=control)
                    except (RuntimeError, ValueError, OSError, psycopg.Error, httpx.HTTPError) as error:
                        # Preserve the UI and stable delivery ID for inspection;
                        # never silently advance past a failed or uncertain receipt.
                        control.update(phase="error", error="Delivery could not be confirmed. Check the launcher terminal; restart to resume the same batch.")
                        say(f"Demo delivery stopped: {error}")
                        if args.exit_after_feed:
                            raise DemoError(str(error)) from error
                    if not args.exit_after_feed:
                        say("Dashboard remains open. Ctrl+C to stop; use -Restart next time to replay the demo.")
                        while True:
                            processes.wait(1)
        finally:
            processes.close()
            say("Demo servers stopped. PostgreSQL and saved reports are retained.")


def install_interrupt_handlers():
    def interrupt(_signal, _frame):
        raise KeyboardInterrupt

    for name in ("SIGINT", "SIGTERM", "SIGBREAK"):
        if hasattr(signal, name):
            signal.signal(getattr(signal, name), interrupt)
    if os.name == "nt":
        # Native shells/process groups can pass an inherited ignore-Ctrl+C flag.
        # Reset it for this supervisor only; hidden child servers are stopped below.
        import ctypes
        ctypes.windll.kernel32.SetConsoleCtrlHandler(None, False)


def main():
    install_interrupt_handlers()
    try:
        run(parser().parse_args())
    except KeyboardInterrupt:
        say("Demo stopped.")
        return 0
    except (DemoError, ValueError, OSError, psycopg.Error, httpx.HTTPError, subprocess.SubprocessError) as error:
        say(f"Demo could not continue: {error}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
