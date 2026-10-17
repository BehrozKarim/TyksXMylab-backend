"""Loopback-only demo controls, proxied by Vite; never mounted in the clinical API."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import secrets
from threading import Event, Lock, Thread


class DemoControls:
    def __init__(self, run_id, origins):
        self.token = secrets.token_urlsafe(32)
        self.origins = set(origins)
        self.lock = Lock()
        self.requested = Event()
        self.value = dict(phase="starting", run_id=run_id, next_batch=None,
                          published_batches=0, skipped_batches=0, last_publication=None, error=None)
        self.server = None

    def snapshot(self):
        with self.lock:
            return dict(self.value)

    def update(self, **values):
        with self.lock:
            self.value.update(values)

    def request_next(self, delivery_id):
        with self.lock:
            upcoming = self.value["next_batch"]
            if (self.value["phase"] != "ready" or not upcoming
                    or delivery_id != upcoming["delivery_id"]):
                return False
            # The delivery ID guards double clicks, retries and stale browser tabs,
            # even when the first request has already finished publishing.
            self.value["phase"] = "processing"
            self.requested.set()
            return True

    def wait_for_click(self, batch, wait):
        self.requested.clear()
        self.update(phase="ready", next_batch={"delivery_id": batch.delivery_id,
                    "cultures": len(batch.bundles), "as_of": batch.as_of.isoformat()})
        while not self.requested.is_set():
            wait(0.1)  # Keep supervising child processes and handling Ctrl+C.

    def __enter__(self):
        control = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                pass

            def setup(self):
                super().setup()
                self.connection.settimeout(5)

            def reply(self, code, value):
                body = json.dumps(value).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def authorized(self):
                supplied = self.headers.get("Authorization", "")
                if not secrets.compare_digest(supplied, f"Bearer {control.token}"):
                    self.reply(403, {"error": "Demo control access denied"})
                    return False
                return True

            def do_GET(self):
                if self.authorized():
                    self.reply(200, control.snapshot()) if self.path == "/status" else self.reply(404, {})

            def do_POST(self):
                if not self.authorized():
                    return
                if self.headers.get("Origin") not in control.origins:
                    self.reply(403, {"error": "Use the local demo dashboard"})
                    return
                if self.path != "/next":
                    self.reply(404, {})
                    return
                try:
                    size = int(self.headers.get("Content-Length", "0"))
                    if not 0 < size <= 4096:
                        raise ValueError("Invalid body length")
                    data = json.loads(self.rfile.read(size))
                    if not isinstance(data, dict) or not isinstance(data.get("delivery_id"), str):
                        raise ValueError("Expected delivery_id")
                except (ValueError, OSError):
                    self.reply(400, {"error": "Expected a JSON delivery_id"})
                    return
                accepted = control.request_next(data["delivery_id"])
                self.reply(202 if accepted else 409, control.snapshot())

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        return self

    def __exit__(self, *_):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
