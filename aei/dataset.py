"""Turn the supplied synthetic CSV archive into complete isolate events."""

import csv
import io
from collections import defaultdict
from datetime import date, datetime, time, timezone
from pathlib import Path
from zipfile import ZipFile

from .models import IsolateEvent, Susceptibility


DEFAULT_DATASET = Path(__file__).resolve().parents[2] / "synthetic_microbiology_dataset.zip"


def load_dataset(path: str | Path = DEFAULT_DATASET) -> list[IsolateEvent]:
    with ZipFile(path) as archive:
        def rows(filename):
            names = [name for name in archive.namelist() if Path(name).name == filename]
            if len(names) != 1:
                raise ValueError(f"Expected exactly one {filename} in the archive")
            with archive.open(names[0]) as raw:
                yield from csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))

        patients = {row["patient_id"] for row in rows("patients.csv")}
        cultures = {row["culture_id"]: row for row in rows("cultures.csv")}
        isolates = {row["isolate_id"]: row for row in rows("isolates.csv")}
        tests = defaultdict(list)
        for row in rows("susceptibility.csv"):
            isolate = isolates.get(row["isolate_id"])
            if (isolate is None or row["culture_id"] != isolate["culture_id"]
                    or row["organism"] != isolate["organism"]):
                raise ValueError("Susceptibility record has an inconsistent isolate reference")
            tests[row["isolate_id"]].append(Susceptibility(
                antibiotic_code=row["antibiotic_code"], result=row["result"], guideline=row["guideline"],
            ))

        events = []
        for isolate_id, isolate in isolates.items():
            culture = cultures.get(isolate["culture_id"])
            if culture is None or culture["patient_id"] not in patients:
                raise ValueError("Isolate has an inconsistent culture/patient reference")
            sample_date = date.fromisoformat(culture["sample_date"])
            events.append(IsolateEvent(
                event_id=f"synthetic:{isolate_id}:1", isolate_id=isolate_id, revision=1,
                patient_id=culture["patient_id"], culture_id=culture["culture_id"],
                sample_date=sample_date,
                # The source has NO result-arrival timestamp. This is a declared demo convention.
                received_at=datetime.combine(sample_date, time(12), timezone.utc),
                ward_code=culture["ward_code"], organism=isolate["organism"],
                specimen=culture["specimen"], tests=tuple(tests[isolate_id]),
            ))
    return sorted(events, key=lambda event: (event.sample_date, event.culture_id, event.isolate_id))
