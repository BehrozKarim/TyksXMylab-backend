import copy
import csv
import hashlib
import json
import os
import subprocess
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch
from zipfile import ZipFile

from aei.dataset import DatasetValidationError, default_dataset, load_bundle, load_dataset, parse_source_date


def fixture_tables():
    wards = [dict(ward_code="ED", ward_fi="Paivystys", ward_en="Emergency", ward_type="out", institution="hospital"),
             dict(ward_code="MED", ward_fi="Sisataudit", ward_en="Medicine", ward_type="in", institution="hospital")]

    def culture(key, day, result, n, ward):
        sample = date.fromisoformat(day)
        return dict(culture_id=key, patient_id="0001", age_years="26", sample_date=day,
                    iso_year=str(sample.isocalendar().year), iso_week=str(sample.isocalendar().week),
                    **ward, admission_date="2026-09-20" if ward["ward_type"] == "in" else "",
                    specimen="urine", specimen_fi="virtsa", result=result, n_isolates=str(n))

    return {
        "patients.csv": [dict(patient_id="0001", sex="F", birth_year="2000", home_area="Centre")],
        "wards.csv": wards,
        "antibiotics.csv": [dict(antibiotic="ciprofloxacin", antibiotic_code="CIP", antibiotic_fi="sipro",
                                  **{"class": "fluoroquinolones"}, reported_for="all"),
                            dict(antibiotic="cefoxitin (screen)", antibiotic_code="FOX", antibiotic_fi="kefoksitiini",
                                 **{"class": "MRSA screen"}, reported_for="all")],
        "cultures.csv": [culture("c1", "2026-09-25", "positive", 1, wards[0]),
                         culture("c2", "2026-09-26", "negative", 0, wards[0]),
                         culture("c3", "2026-09-26", "mixed_growth", 0, wards[0]),
                         culture("c4", "2026-09-27", "positive", 1, wards[1])],
        "isolates.csv": [dict(isolate_id="i1", culture_id="c1", isolate_no="1", organism="Escherichia coli",
                              quantity=">=10^5", esbl="0", mrsa="0", vre="0", cpe="0"),
                         dict(isolate_id="i2", culture_id="c4", isolate_no="1", organism="Escherichia coli",
                              quantity="10^4", esbl="1", mrsa="0", vre="0", cpe="0")],
        "susceptibility.csv": [dict(isolate_id="i1", culture_id="c1", organism="Escherichia coli",
                                    antibiotic="ciprofloxacin", antibiotic_code="CIP", antibiotic_fi="sipro",
                                    result="S", guideline="EUCAST 2026"),
                               dict(isolate_id="i2", culture_id="c4", organism="Escherichia coli",
                                    antibiotic="ciprofloxacin", antibiotic_code="CIP", antibiotic_fi="sipro",
                                    result="R", guideline="EUCAST 2026")],
    }


def write_fixture(folder, tables=None, *, short_dates=False, bom=False):
    tables = copy.deepcopy(tables if tables is not None else fixture_tables())
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    for filename, rows in tables.items():
        headers = list(fixture_tables()[filename][0])
        if short_dates and filename == "cultures.csv":
            for row in rows:
                for field in ("sample_date", "admission_date"):
                    if row[field]:
                        row[field] = date.fromisoformat(row[field]).strftime("%d-%m-%y")
        with (folder / filename).open("w", encoding="utf-8-sig" if bom else "utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=headers)
            writer.writeheader()
            writer.writerows(rows)
    return folder


class ImportTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.folder = write_fixture(self.root / "source")

    def test_zip_directory_and_short_date_sources_preserve_event_identity(self):
        before = {path.name: path.read_bytes() for path in self.folder.iterdir()}
        archive = self.root / "data.zip"
        with ZipFile(archive, "w") as zipped:
            for name, raw in before.items():
                zipped.writestr("nested/" + name, raw)
        directory, zipped = load_bundle(self.folder), load_bundle(archive)
        short = load_bundle(write_fixture(self.root / "short", short_dates=True, bom=True))
        expected = [event.canonical_json() for event in directory.events]
        self.assertEqual(expected, [event.canonical_json() for event in zipped.events])
        self.assertEqual(expected, [event.canonical_json() for event in short.events])
        self.assertEqual(directory.report.manifest, zipped.report.manifest)
        self.assertEqual([event.canonical_json() for event in load_dataset(self.folder)], expected)
        self.assertEqual({path.name: path.read_bytes() for path in self.folder.iterdir()}, before)

    def test_bundle_keeps_negative_mixed_cultures_and_revision_metadata(self):
        bundle = load_bundle(self.folder)
        self.assertEqual(len(bundle.cultures), 4)
        self.assertEqual(len(bundle.events), 2)
        self.assertEqual(bundle.patients[0].patient_id, "0001")
        self.assertEqual({c.result for c in bundle.cultures}, {"positive", "negative", "mixed_growth"})
        enriched = {item.event_id: item for item in bundle.enrichments}
        self.assertEqual(enriched["synthetic:i2:1"].admission_date, date(2026, 9, 20))
        self.assertEqual(enriched["synthetic:i2:1"].esbl, 1)
        self.assertEqual(enriched["synthetic:i1:1"].sex, "F")
        self.assertTrue(next(drug for drug in bundle.antibiotics if drug.antibiotic_code == "FOX").is_screening)
        self.assertNotIn("sex", bundle.events[0].model_dump())

    def test_manifest_describes_source_bytes_counts_bounds_and_observed_panels(self):
        report = load_bundle(self.folder).report
        self.assertTrue(report.valid)
        self.assertEqual(report.files["cultures.csv"].rows, 4)
        self.assertEqual(report.files["cultures.csv"].sha256,
                         hashlib.sha256((self.folder / "cultures.csv").read_bytes()).hexdigest())
        self.assertEqual((report.manifest.source_start, report.manifest.source_end),
                         (date(2026, 9, 25), date(2026, 9, 27)))
        self.assertEqual(report.manifest.observed_panels[0].antibiotic_codes, ("CIP",))

    def test_invalid_values_and_relationships_produce_located_errors(self):
        cases = [
            ("patients.csv", "sex", "X"),
            ("cultures.csv", "patient_id", "missing"),
            ("cultures.csv", "ward_code", "missing"),
            ("cultures.csv", "ward_en", "different"),
            ("cultures.csv", "sample_date", "09/25/2026"),
            ("cultures.csv", "iso_week", "1"),
            ("cultures.csv", "age_years", "-1"),
            ("cultures.csv", "n_isolates", "2"),
            ("cultures.csv", "result", "negative"),
            ("isolates.csv", "culture_id", "missing"),
            ("isolates.csv", "esbl", "2"),
            ("susceptibility.csv", "isolate_id", "missing"),
            ("susceptibility.csv", "culture_id", "c4"),
            ("susceptibility.csv", "organism", "another organism"),
            ("susceptibility.csv", "antibiotic_code", "UNKNOWN"),
            ("susceptibility.csv", "antibiotic_fi", "different"),
            ("susceptibility.csv", "result", "I"),
        ]
        for filename, field, value in cases:
            with self.subTest(file=filename, field=field):
                tables = fixture_tables()
                tables[filename][0][field] = value
                write_fixture(self.folder, tables)
                with self.assertRaises(DatasetValidationError) as caught:
                    load_bundle(self.folder)
                issue = next(item for item in caught.exception.report.errors if item.file == filename)
                self.assertEqual(issue.row, 2)
                self.assertIsNotNone(issue.record_id)

    def test_admission_after_collection_is_rejected(self):
        tables = fixture_tables()
        tables["cultures.csv"][3]["admission_date"] = "2026-09-28"
        write_fixture(self.folder, tables)
        with self.assertRaises(DatasetValidationError) as caught:
            load_bundle(self.folder)
        self.assertIn("after sample_date", caught.exception.report.errors[0].message)

    def test_duplicate_ids_and_ast_pairs_are_never_silently_overwritten(self):
        for filename in fixture_tables():
            with self.subTest(filename=filename):
                tables = fixture_tables()
                tables[filename].append(tables[filename][0].copy())
                write_fixture(self.folder, tables)
                with self.assertRaises(DatasetValidationError) as caught:
                    load_bundle(self.folder)
                self.assertEqual(caught.exception.report.errors[0].code, "duplicate_key")

    def test_missing_tests_are_allowed_and_reported_without_invented_results(self):
        tables = fixture_tables()
        tables["susceptibility.csv"] = tables["susceptibility.csv"][:1]
        bundle = load_bundle(write_fixture(self.folder, tables))
        self.assertEqual(bundle.events[1].tests, ())
        self.assertIn("missing_ast", [issue.code for issue in bundle.report.warnings])

    def test_missing_headers_files_and_ambiguous_zip_entries_are_rejected(self):
        with ZipFile(self.root / "ambiguous.zip", "w") as archive:
            for name in fixture_tables():
                archive.writestr(name, (self.folder / name).read_bytes())
            archive.writestr("another/patients.csv", (self.folder / "patients.csv").read_bytes())
        with self.assertRaises(DatasetValidationError) as caught:
            load_bundle(self.root / "ambiguous.zip")
        self.assertEqual(caught.exception.report.errors[0].code, "ambiguous_file")
        (self.folder / "patients.csv").write_text("patient_id,sex\n1,F\n", encoding="utf-8")
        with self.assertRaises(DatasetValidationError) as caught:
            load_bundle(self.folder)
        self.assertEqual(caught.exception.report.errors[0].code, "invalid_headers")
        with self.assertRaises(DatasetValidationError):
            load_bundle(self.root / "absent.zip")

    def test_short_year_mapping_and_invalid_dates(self):
        self.assertEqual(parse_source_date("02-10-23"), date(2023, 10, 2))
        self.assertEqual(parse_source_date("01-01-99"), date(2099, 1, 1))
        for value in ("2026-02-29", "31-04-26", "2026-9-1", "01/02/26"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_source_date(value)

    def test_validation_cli_writes_json_without_a_database(self):
        report = self.root / "report.json"
        command = [sys.executable, "-m", "aei", "--database-url", "postgresql://invalid:1/unavailable",
                   "validate-dataset", "--dataset-dir", str(self.folder), "--report", str(report)]
        result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(json.loads(report.read_text(encoding="utf-8"))["valid"])
        tables = fixture_tables()
        tables["patients.csv"][0]["sex"] = "X"
        write_fixture(self.folder, tables)
        result = subprocess.run(command, capture_output=True, text=True, encoding="utf-8")
        self.assertEqual(result.returncode, 2)
        self.assertFalse(json.loads(report.read_text(encoding="utf-8"))["valid"])

    def test_source_options_are_exclusive_and_environment_is_respected(self):
        with patch.dict(os.environ, {"AEI_DATASET_PATH": str(self.folder)}):
            self.assertEqual(default_dataset(), self.folder)
            self.assertEqual(len(load_dataset()), 2)
        result = subprocess.run([sys.executable, "-m", "aei", "validate-dataset", "--zip", "a.zip",
                                 "--dataset-dir", str(self.folder)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
