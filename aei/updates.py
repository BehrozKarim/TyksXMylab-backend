"""SSE delivery from a durable, transactionally committed notification log."""

import asyncio
import json
import time

from .publication_models import ServiceError


class UpdateStream:
    def __init__(self, publication):
        self.publication = publication
        self.store = publication.store

    def begin(self, after=None):
        self.publication.state()  # Bootstrap a pre-notification run when needed.
        with self.store.connection() as db:
            latest = db.execute("SELECT COALESCE(MAX(notification_id), 0) FROM publication_notifications").fetchone()[0]
            if after is not None:
                if after < 0 or after > latest:
                    raise ServiceError(422, "invalid_event_cursor", "Event cursor must refer to this backend's notification history")
                return after, None
            row = db.execute("""SELECT n.notification_id, n.payload FROM publication_notifications n
                JOIN active_run a USING(run_id) JOIN replay_runs r USING(run_id)
                WHERE n.state_revision=(r.payload->>'state_revision')::bigint""").fetchone()
            return row[0], row

    def read_after(self, cursor):
        with self.store.connection() as db:
            return db.execute("""SELECT notification_id, payload FROM publication_notifications
                WHERE notification_id>%s ORDER BY notification_id LIMIT 100""", (cursor,)).fetchall()

    @staticmethod
    def encode(row):
        return f"id: {row[0]}\nevent: state\ndata: {json.dumps(row[1], separators=(',', ':'))}\n\n"

    async def stream(self, request, cursor, initial):
        yield "retry: 2000\n\n"
        if initial:
            yield self.encode(initial)
        last_message = time.monotonic()
        while not await request.is_disconnected():
            rows = await asyncio.to_thread(self.read_after, cursor)
            for row in rows:
                yield self.encode(row)
                cursor = row[0]
                last_message = time.monotonic()
            if not rows:
                if time.monotonic() - last_message >= 15:
                    yield ": keep-alive\n\n"
                    last_message = time.monotonic()
                await asyncio.sleep(0.5)
