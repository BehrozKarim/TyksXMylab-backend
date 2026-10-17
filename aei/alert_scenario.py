"""Write a separate, explicitly authored alert demonstration; never edit input data."""

import argparse
import csv
import json
from datetime import date
from pathlib import Path


SCENARIO = {
    "label": "Authored synthetic resistance-rise demo; not supplied laboratory data",
    "seed_through": "2026-09-26",
    "replay_through": "2026-10-01",
    "monitor": {"ward_code": "ED", "organism": "Escherichia coli", "specimen": "urine", "antibiotic_code": "CIP"},
    "expected": {"2026-09-26": "no alert (29 recent tested)", "2026-09-27": "new (20% to 30%, exactly 10 pp)",
                 "2026-09-28": "insufficient_evidence", "2026-09-29": "cleared",
                 "2026-09-30": "ongoing (recurrence, same ID)", "2026-10-01": "ongoing"},
}


def write_alert_scenario(output):
    output = Path(output)
    # Exclusive directory creation prevents overwriting any supplied CSVs.
    output.mkdir(parents=True, exist_ok=False)
    ward = dict(ward_code="ED", ward_fi="DEMO", ward_en="Authored demo emergency", ward_type="out", institution="hospital")
    drug = dict(antibiotic_code="CIP", antibiotic="ciprofloxacin", antibiotic_fi="siprofloksasiini",
                **{"class": "fluoroquinolones"}, reported_for="all")
    tables = {name: [] for name in ("patients", "cultures", "isolates", "susceptibility")}
    tables.update(wards=[ward], antibiotics=[drug])
    for day, count, resistant in (("2026-01-01", 30, 6), ("2026-08-31", 29, 9), ("2026-09-27", 1, 0),
                                  ("2026-09-28", 28, 9), ("2026-09-29", 1, 0), ("2026-09-30", 30, 30),
                                  ("2026-10-01", 1, None)):
        stamp = date.fromisoformat(day)
        for index in range(count):
            key = f"authored-alert-demo-{len(tables['patients']) + 1:04d}"
            tables["patients"].append(dict(patient_id=key, sex="F" if index % 2 else "M",
                                            birth_year=1980, home_area="Authored alert scenario"))
            tables["cultures"].append(dict(culture_id=key, patient_id=key, age_years=46, sample_date=day,
                iso_year=stamp.isocalendar().year, iso_week=stamp.isocalendar().week, **ward, admission_date="",
                specimen="urine", specimen_fi="virtsa", result="negative" if resistant is None else "positive",
                n_isolates=0 if resistant is None else 1))
            if resistant is None:
                continue
            tables["isolates"].append(dict(isolate_id=key, culture_id=key, isolate_no=1, organism="Escherichia coli",
                quantity="", esbl=0, mrsa=0, vre=0, cpe=0))
            tables["susceptibility"].append(dict(isolate_id=key, culture_id=key, organism="Escherichia coli",
                antibiotic=drug["antibiotic"], antibiotic_code="CIP", antibiotic_fi=drug["antibiotic_fi"],
                result="R" if index < resistant else "S", guideline="Authored demo S/R"))
    for name, rows in tables.items():
        with (output / f"{name}.csv").open("x", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    (output / "SCENARIO.json").write_text(json.dumps(SCENARIO, indent=2) + "\n", encoding="utf-8")
    return output


def main():
    parser = argparse.ArgumentParser(description=SCENARIO["label"])
    parser.add_argument("--output", type=Path, required=True, help="New directory; must not already exist")
    args = parser.parse_args()
    try:
        print(write_alert_scenario(args.output))
    except FileExistsError:
        parser.error("Output already exists; choose a new directory to preserve existing files")


if __name__ == "__main__":
    main()
