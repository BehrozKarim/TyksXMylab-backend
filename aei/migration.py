"""Read existing SQLite history without changing or initializing the source file."""

import sqlite3
from contextlib import closing
from pathlib import Path

from .models import IsolateEvent


def read_sqlite_history(path: str | Path) -> list[IsolateEvent]:
    source = Path(path).resolve(strict=True)
    with closing(sqlite3.connect(source.as_uri() + "?mode=ro", uri=True)) as db:
        rows = db.execute("SELECT payload FROM isolate_events ORDER BY rowid").fetchall()
    return [IsolateEvent.model_validate_json(row[0]) for row in rows]
