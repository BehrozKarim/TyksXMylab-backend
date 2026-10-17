"""Validate untouched source CSVs before constructing complete import bundles."""

import csv
import hashlib
import io
import json
import os
import re
from collections import Counter, defaultdict
from datetime import date, datetime, time, timezone
from pathlib import Path
from zipfile import BadZipFile, ZipFile

from pydantic import ValidationError

from .dataset_models import (
    ASTRecord, AntibioticRecord, CultureRecord, DatasetBundle, DatasetManifest,
    FileSummary, ImportIssue, IsolateEnrichment, IsolateRecord, ObservedPanel,
    PatientRecord, ValidationReport, WardRecord,
)
from .models import IsolateEvent, Susceptibility


def default_dataset() -> Path:
    """Explicit environment setting, then the historical ZIP, then a local folder."""
    if os.environ.get("AEI_DATASET_PATH"):
        return Path(os.environ["AEI_DATASET_PATH"])
    root = Path(__file__).resolve().parents[1]
    archive = root.parent / "synthetic_microbiology_dataset.zip"
    directory = root / "synthetic_microbiology_dataset"
    return archive if archive.is_file() or not directory.is_dir() else directory


DEFAULT_DATASET = default_dataset()
TABLES = {
    "patients.csv": (PatientRecord, "patient_id"),
    "wards.csv": (WardRecord, "ward_code"),
    "antibiotics.csv": (AntibioticRecord, "antibiotic_code"),
    "cultures.csv": (CultureRecord, "culture_id"),
    "isolates.csv": (IsolateRecord, "isolate_id"),
    "susceptibility.csv": (ASTRecord, "isolate_id"),
}


class DatasetValidationError(ValueError):
    def __init__(self, report: ValidationReport):
        self.report = report
        super().__init__(f"Dataset validation failed with {report.error_count} error(s)")


def parse_source_date(value: str) -> date:
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return date.fromisoformat(value)
    if re.fullmatch(r"\d{2}-\d{2}-\d{2}", value):
        day, month, year = map(int, value.split("-"))
        return date(2000 + year, month, day)
    raise ValueError("Expected YYYY-MM-DD or DD-MM-YY (short years mean 2000-2099)")


def load_bundle(path: str | Path | None = None) -> DatasetBundle:
    source = Path(path) if path is not None else default_dataset()
    kind = "directory" if source.is_dir() else "zip"
    errors, warnings, summaries = [], [], {}
    error_count = 0

    def error(code, message, filename=None, row=None, record_id=None):
        nonlocal error_count
        error_count += 1
        if len(errors) < 100:
            errors.append(ImportIssue(code=code, message=message, file=filename,
                                      row=row, record_id=record_id))

    def fail_if_errors():
        if error_count:
            raise DatasetValidationError(ValidationReport(
                valid=False, source_kind=kind, files=summaries, errors=tuple(errors),
                error_count=error_count, warnings=tuple(warnings)))

    contents = {}
    try:
        if kind == "directory":
            for filename in TABLES:
                try:
                    contents[filename] = (source / filename).read_bytes()
                except OSError:
                    error("missing_file", "Required CSV cannot be read", filename)
        else:
            with ZipFile(source) as archive:
                for filename in TABLES:
                    matches = [name for name in archive.namelist() if Path(name).name == filename]
                    if len(matches) != 1:
                        error("ambiguous_file", "Expected exactly one matching CSV in ZIP", filename)
                    else:
                        contents[filename] = archive.read(matches[0])
    except (OSError, BadZipFile):
        error("unreadable_source", "Source must be a readable ZIP archive or CSV directory")
    fail_if_errors()

    tables = {}
    row_numbers = {}
    for filename, (model, primary_key) in TABLES.items():
        raw = contents.pop(filename)
        digest = hashlib.sha256(raw).hexdigest()
        records, row_numbers[filename], seen = {}, {}, set()
        count = 0
        try:
            reader = csv.DictReader(io.StringIO(raw.decode("utf-8-sig"), newline=""), strict=True)
            expected = {field.alias or name for name, field in model.model_fields.items()}
            if (not reader.fieldnames or len(reader.fieldnames) != len(set(reader.fieldnames))
                    or set(reader.fieldnames) != expected):
                error("invalid_headers", "Headers must match: " + ", ".join(sorted(expected)), filename, 1)
            else:
                for row in reader:
                    count += 1
                    line = reader.line_num
                    record_id = row.get(primary_key)
                    if None in row or any(value is None for value in row.values()):
                        error("invalid_row", "Row has an incorrect number of fields", filename, line, record_id)
                        continue
                    key = ((record_id, row["antibiotic_code"]) if model is ASTRecord else record_id)
                    if key in seen:
                        error("duplicate_key", "Duplicate primary key or isolate/antibiotic pair", filename, line, record_id)
                        continue
                    seen.add(key)
                    try:
                        if model is CultureRecord:
                            row["sample_date"] = parse_source_date(row["sample_date"])
                            row["admission_date"] = parse_source_date(row["admission_date"]) if row["admission_date"] else None
                        if any(isinstance(value, str) and value and not value.strip() for value in row.values()):
                            raise ValueError("Whitespace-only fields are not allowed")
                        record = model.model_validate(row)
                    except (ValidationError, ValueError) as exc:
                        message = ("; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors())
                                   if isinstance(exc, ValidationError) else str(exc))
                        error("invalid_value", message, filename, line, record_id)
                        continue
                    records[key] = record
                    row_numbers[filename][key] = line
        except (UnicodeError, csv.Error):
            error("invalid_csv", "Expected valid comma-separated UTF-8 CSV", filename)
        summaries[filename] = FileSummary(sha256=digest, rows=count)
        tables[filename] = records
    # Do not produce cascading link errors for a structurally invalid source.
    fail_if_errors()

    patients, wards, drugs, cultures, isolates, ast = (tables[name] for name in TABLES)

    def link_error(filename, key, message):
        error("inconsistent_reference", message, filename, row_numbers[filename][key],
              key[0] if isinstance(key, tuple) else key)

    for key, culture in cultures.items():
        if culture.patient_id not in patients:
            link_error("cultures.csv", key, "Unknown patient_id")
        elif patients[culture.patient_id].birth_year > culture.sample_date.year:
            link_error("cultures.csv", key, "Patient birth_year is after sample_date")
        ward = wards.get(culture.ward_code)
        if ward is None:
            link_error("cultures.csv", key, "Unknown ward_code")
        elif any(getattr(culture, field) != getattr(ward, field)
                 for field in ("ward_fi", "ward_en", "ward_type", "institution")):
            link_error("cultures.csv", key, "Ward labels/type/institution disagree with dictionary")

    counts, slots = Counter(), set()
    for key, isolate in isolates.items():
        if isolate.culture_id not in cultures:
            link_error("isolates.csv", key, "Unknown culture_id")
        slot = (isolate.culture_id, isolate.isolate_no)
        if slot in slots:
            link_error("isolates.csv", key, "Duplicate isolate_no within culture")
        slots.add(slot)
        counts[isolate.culture_id] += 1
    for key, culture in cultures.items():
        if culture.n_isolates != counts[key]:
            link_error("cultures.csv", key, "n_isolates does not match linked isolates")
        if (culture.result == "positive") != (counts[key] > 0):
            link_error("cultures.csv", key, "Culture result contradicts linked isolate count")

    tests, panels = defaultdict(list), defaultdict(set)
    for key, result in ast.items():
        isolate, drug = isolates.get(result.isolate_id), drugs.get(result.antibiotic_code)
        if isolate is None or (result.culture_id, result.organism) != (isolate.culture_id, isolate.organism):
            link_error("susceptibility.csv", key, "Unknown or inconsistent isolate/culture/organism reference")
            continue
        if drug is None or (result.antibiotic, result.antibiotic_fi) != (drug.antibiotic, drug.antibiotic_fi):
            link_error("susceptibility.csv", key, "Unknown or inconsistent antibiotic dictionary reference")
            continue
        culture = cultures.get(isolate.culture_id)
        if culture:
            if drug.reported_for not in ("all", culture.specimen):
                link_error("susceptibility.csv", key, "Drug specimen restriction disagrees with culture")
            panels[(isolate.organism, culture.specimen)].add(result.antibiotic_code)
        tests[isolate.isolate_id].append(Susceptibility(
            antibiotic_code=result.antibiotic_code, result=result.result, guideline=result.guideline))
    fail_if_errors()

    warnings.append(ImportIssue(code="synthetic_arrival_time", message=
        "Received times use 12:00 UTC on sample_date; the source supplies no laboratory arrival timestamps."))
    missing = sum(not tests[isolate_id] for isolate_id in isolates)
    if missing:
        warnings.append(ImportIssue(code="missing_ast", file="isolates.csv", message=
            f"{missing} isolate(s) have no recorded antibiotic tests; no results were inferred."))
    # A discrepancy with the supplied README, not a clinical inference or exclusion.
    described_pairs = {("Proteus mirabilis", "NIT"), ("Klebsiella pneumoniae", "AMP")}
    present = sorted({(row.organism, row.antibiotic_code) for row in ast.values()
                      if (row.organism, row.antibiotic_code) in described_pairs})
    if present:
        warnings.append(ImportIssue(code="source_documentation_discrepancy", file="susceptibility.csv", message=
            "Pairs described as omitted in the supplied README are present and retained: "
            + ", ".join(f"{organism}/{code}" for organism, code in present)))

    events, enrichments = [], []
    for isolate_id, isolate in isolates.items():
        culture = cultures[isolate.culture_id]
        patient = patients[culture.patient_id]
        event = IsolateEvent(
            event_id=f"synthetic:{isolate_id}:1", isolate_id=isolate_id, revision=1,
            patient_id=culture.patient_id, culture_id=culture.culture_id,
            sample_date=culture.sample_date,
            received_at=datetime.combine(culture.sample_date, time(12), timezone.utc),
            ward_code=culture.ward_code, organism=isolate.organism, specimen=culture.specimen,
            tests=tuple(tests[isolate_id]))
        events.append(event)
        enrichments.append(IsolateEnrichment(
            event_id=event.event_id, culture_event_id=culture.event_id, sex=patient.sex,
            birth_year=patient.birth_year, home_area=patient.home_area, age_years=culture.age_years,
            admission_date=culture.admission_date, **isolate.model_dump(include={
                "isolate_no", "quantity", "esbl", "mrsa", "vre", "cpe"})))
    events.sort(key=lambda event: (event.sample_date, event.culture_id, event.isolate_id))
    dates = [culture.sample_date for culture in cultures.values()]
    identity = json.dumps({"importer_version": "dataset-v1",
                           "files": {name: item.sha256 for name, item in summaries.items()}}, sort_keys=True)
    manifest = DatasetManifest(
        manifest_id=hashlib.sha256(identity.encode()).hexdigest(), files=summaries,
        source_start=min(dates) if dates else None, source_end=max(dates) if dates else None,
        observed_panels=tuple(ObservedPanel(organism=organism, specimen=specimen,
            antibiotic_codes=tuple(sorted(codes))) for (organism, specimen), codes in sorted(panels.items())))
    report = ValidationReport(valid=True, source_kind=kind, files=summaries, manifest=manifest,
                              warnings=tuple(warnings))
    return DatasetBundle(report=report, patients=tuple(patients.values()), wards=tuple(wards.values()),
                         antibiotics=tuple(drugs.values()), cultures=tuple(cultures.values()),
                         events=tuple(events), enrichments=tuple(enrichments))


def load_dataset(path: str | Path | None = None) -> list[IsolateEvent]:
    """Compatibility entry point for existing calculations and replay callers."""
    return list(load_bundle(path).events)
