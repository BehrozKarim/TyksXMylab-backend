"""Published comparison/history behavior, including visibility and cursor isolation."""

import copy
import csv
import io
import os
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from unittest.mock import patch
from uuid import uuid4

import psycopg
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.types.json import Jsonb

from aei.analysis_models import ComparisonRequest, HistoryRequest, PatientSearchRequest, SavedHistory
from aei.api import create_app
from aei.dataset import load_bundle
from aei.history import history_page
from aei.postgres import PostgresEventStore
from aei.publication import PublicationService, export_csv
from aei.publication_models import EvidenceRequest, ExpectedState, ReplayReset, ServiceError
from test_import import fixture_tables, write_fixture


PAIR = dict(organism="Escherichia coli", specimen="urine", antibiotic_a="CIP", antibiotic_b="TMP")


def view_tables():
    tables = fixture_tables()
    tmp = {**tables["antibiotics.csv"][0], "antibiotic": "trimethoprim", "antibiotic_code": "TMP",
           "antibiotic_fi": "trimetopriimi", "class": "antifolate", "reported_for": "urine"}
    tables["antibiotics.csv"].append(tmp)
    for row in list(tables["susceptibility.csv"]):
        tables["susceptibility.csv"].append({**row, "antibiotic": tmp["antibiotic"], "antibiotic_code": "TMP",
            "antibiotic_fi": tmp["antibiotic_fi"], "result": "R" if row["result"] == "S" else "S"})

    def add_culture(key, day, patient="0001", result="negative", count=0):
        stamp = date.fromisoformat(day)
        record = {**tables["cultures.csv"][0], "culture_id": key, "sample_date": day,
            "iso_year": str(stamp.isocalendar().year), "iso_week": str(stamp.isocalendar().week),
            "patient_id": patient, "result": result, "n_isolates": str(count)}
        tables["cultures.csv"].append(record)

    add_culture("boundary", "2025-09-27")
    add_culture("too_old", "2025-09-26")
    add_culture("repeat", "2026-09-25", result="positive", count=2)
    for key, organism in (("i3", "Escherichia coli"), ("i4", "Staphylococcus aureus")):
        tables["isolates.csv"].append({**tables["isolates.csv"][0], "isolate_id": key,
            "culture_id": "repeat", "organism": organism, "isolate_no": "1" if key == "i3" else "2"})
        tables["susceptibility.csv"].append({**tables["susceptibility.csv"][0], "isolate_id": key,
            "culture_id": "repeat", "organism": organism, "result": "R"})
    for key, day in (("0002", "2026-09-28"), ("0003", "2026-09-27"), ("0004", "2024-01-01")):
        tables["patients.csv"].append({**tables["patients.csv"][0], "patient_id": key})
        add_culture("patient_" + key, day, patient=key)
    return tables


def forty_pairs():
    tables = view_tables()
    patient, culture, isolate, ast = [copy.deepcopy(tables[name][0]) for name in
                                    ("patients.csv", "cultures.csv", "isolates.csv", "susceptibility.csv")]
    for name in ("patients.csv", "cultures.csv", "isolates.csv", "susceptibility.csv"):
        tables[name] = []
    pairs = [("S", "S")] * 25 + [("S", "R")] * 5 + [("R", "S")] * 8 + [("R", "R")] * 2
    for i, (a, b) in enumerate(pairs):
        key = f"{i:04d}"
        tables["patients.csv"].append({**patient, "patient_id": key})
        tables["cultures.csv"].append({**culture, "patient_id": key, "culture_id": key})
        tables["isolates.csv"].append({**isolate, "isolate_id": key, "culture_id": key})
        for code, result in (("CIP", a), ("TMP", b)):
            drug = next(item for item in tables["antibiotics.csv"] if item["antibiotic_code"] == code)
            tables["susceptibility.csv"].append({**ast, "isolate_id": key, "culture_id": key,
                "antibiotic_code": code, "antibiotic": drug["antibiotic"], "antibiotic_fi": drug["antibiotic_fi"], "result": result})
    return tables


@unittest.skipUnless(os.environ.get("AEI_TEST_DATABASE_URL"), "Set AEI_TEST_DATABASE_URL for PostgreSQL tests")
class PatientViewTests(unittest.TestCase):
    def setUp(self):
        self.url = os.environ["AEI_TEST_DATABASE_URL"]
        self.schema = "aei_test_" + uuid4().hex
        self.store = PostgresEventStore(self.url, self.schema)
        self.service = PublicationService(self.store)

    def tearDown(self):
        if not self.schema.startswith("aei_test_"):
            raise RuntimeError("Refusing cleanup outside disposable test schema")
        with psycopg.connect(self.url) as db:
            db.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(self.schema)))

    def load(self, tables=None):
        with tempfile.TemporaryDirectory() as folder:
            bundle = load_bundle(write_fixture(folder, tables or view_tables()))
        self.store.import_dataset(bundle)
        return bundle

    def history(self, patient="0001", **kwargs):
        return self.service.patient_history(patient, HistoryRequest(**{"as_of": "2026-09-27", **kwargs}))

    def test_history_includes_boundary_negatives_mixed_growth_and_every_repeat(self):
        self.load()
        report = self.history()
        ids = [culture.record.culture_id for culture in report.cultures]
        self.assertEqual(ids, ["c3", "c2", "repeat", "c1", "boundary"])
        self.assertEqual(report.context.start_date, date(2025, 9, 27))
        self.assertEqual(report.context.end_date_exclusive, date(2026, 9, 27))
        self.assertEqual([item.record.result for item in report.cultures[:2]], ["mixed_growth", "negative"])
        self.assertEqual([len(item.isolates) for item in report.cultures], [0, 0, 2, 1, 0])
        repeat = report.cultures[2]
        self.assertEqual({item.event.organism for item in repeat.isolates}, {"Escherichia coli", "Staphylococcus aureus"})
        self.assertEqual(repeat.isolates[0].event.tests[0].result, "R")
        self.assertEqual(repeat.isolates[0].enrichment.age_years, 26)
        self.assertNotIn("c4", ids)  # All specimens on cutoff date are excluded.

    def test_search_is_bounded_literal_and_independent_of_history_lookback(self):
        self.load()
        found = self.service.patients(PatientSearchRequest(search="000", as_of="2026-09-27"))
        self.assertEqual([item.patient_id for item in found.patients], ["0001", "0003", "0004"])
        limited = self.service.patients(PatientSearchRequest(search="000", limit=1, as_of="2026-09-27"))
        self.assertTrue(limited.has_more)
        self.assertEqual(len(limited.patients), 1)
        for literal in ("%", "_", "' OR true --"):
            self.assertEqual(self.service.patients(PatientSearchRequest(search=literal)).patients, ())
        # Current-only and older-than-lookback patients remain selectable.
        for patient in ("0003", "0004"):
            self.assertEqual(self.history(patient).cultures, ())

    def test_unknown_and_future_only_patients_have_identical_404_errors(self):
        self.load()
        self.service.reset(ReplayReset(seed_through="2026-09-27"))
        with TestClient(create_app(store=self.store)) as client:
            future = client.get("/api/v1/patients/0002/history")
            unknown = client.get("/api/v1/patients/does-not-exist/history")
            self.assertEqual((future.status_code, unknown.status_code), (404, 404))
            self.assertEqual(future.json()["error"], unknown.json()["error"])
            self.assertEqual(client.get("/api/v1/patients", params={"search": "0002"}).json()["patients"], [])
            self.assertEqual(client.get("/api/v1/patients/0003/history").status_code, 200)

    def test_history_pages_have_stable_tie_order_without_duplicates(self):
        self.load()
        page = self.history(limit=2)
        all_ids = []
        first = page
        while True:
            all_ids.extend(c.record.culture_id for c in page.cultures)
            if not page.has_more:
                self.assertIsNone(page.next_cursor)
                break
            page = self.history(limit=2, cursor=page.next_cursor)
        self.assertEqual(all_ids, ["c3", "c2", "repeat", "c1", "boundary"])
        self.assertEqual(self.history(limit=2), first)
        self.assertEqual(self.service.analysis(first.analysis_id), first)

    def test_cursor_rejects_other_patients_filters_kinds_and_terminal_pages(self):
        self.load()
        first = self.history(limit=2)
        evidence = self.service.evidence(EvidenceRequest(organism="Escherichia coli", specimen="urine", antibiotic_code="CIP"))
        for patient, changes in [("0003", {"limit": 2, "cursor": first.next_cursor}),
                                 ("0001", {"limit": 3, "cursor": first.next_cursor}),
                                 ("0001", {"limit": 2, "as_of": "2026-09-26", "cursor": first.next_cursor}),
                                 ("0001", {"cursor": evidence.analysis_id}),
                                 ("0001", {"cursor": str(uuid4())}),
                                 ("0001", {"cursor": self.history().analysis_id})]:
            with self.subTest(changes=changes), self.assertRaises(ServiceError) as caught:
                self.history(patient, **changes)
            self.assertEqual(caught.exception.code, "invalid_cursor")

    def test_replay_step_invalidates_cursor_but_retains_saved_history_and_comparison(self):
        self.load()
        state = self.service.reset(ReplayReset(seed_through="2026-09-27"))
        history = self.history(limit=2)
        comparison = self.service.comparison(ComparisonRequest(**PAIR))
        original_csv = export_csv(history)
        self.service.step(ExpectedState(expected_run_id=state.run_id, expected_revision=state.state_revision))
        with self.assertRaises(ServiceError) as caught:
            self.history(limit=2, cursor=history.next_cursor)
        self.assertEqual(caught.exception.code, "stale_state")
        self.assertEqual(self.service.analysis(history.analysis_id), history)
        self.assertEqual(export_csv(self.service.analysis(history.analysis_id)), original_csv)
        self.assertEqual(self.service.analysis(comparison.analysis_id), comparison)
        self.assertIsInstance(self.service.analysis(history.analysis_id), SavedHistory)

    def test_reset_rejects_same_revision_cursor_and_hides_prior_run_future(self):
        self.load()
        old = self.service.reset(ReplayReset(seed_through="2026-09-27"))
        report = self.history(limit=2)
        new = self.service.reset(ReplayReset(seed_through="2026-09-25"))
        self.assertEqual(old.state_revision, new.state_revision)
        with self.assertRaises(ServiceError) as caught:
            self.history(limit=2, cursor=report.next_cursor)
        self.assertEqual(caught.exception.status, 409)
        now = self.service.patient_history("0001", HistoryRequest())
        self.assertEqual({item.record.culture_id for item in now.cultures}, {"boundary", "too_old"})
        self.assertEqual(self.service.analysis(report.analysis_id), report)

    def test_history_exports_preserve_zero_isolate_cultures_and_recorded_ast(self):
        self.load()
        report = self.history()
        rows = list(csv.DictReader(io.StringIO(export_csv(report))))
        self.assertEqual(sum(row["record_type"] == "culture" for row in rows), 5)
        self.assertEqual(sum(row["record_type"] == "isolate" for row in rows), 3)
        self.assertEqual(sum(row["record_type"] == "ast" for row in rows), 4)
        self.assertEqual({row["test.result"] for row in rows if row["record_type"] == "ast"}, {"S", "R"})
        self.assertTrue(all(row["context.run_id"] == report.context.run_id for row in rows))
        empty = list(csv.DictReader(io.StringIO(export_csv(self.history("0003")))))
        self.assertEqual((empty[0]["record_type"], empty[0]["culture_count"]), ("history_summary", "0"))
        with TestClient(create_app(store=self.store)) as client:
            response = client.get("/api/v1/patients/0001/history", params={"as_of": "2026-09-27"})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json(), report.model_dump(mode="json"))
            url = f"/api/v1/analyses/{report.analysis_id}"
            self.assertEqual(client.get(url).json(), response.json())
            self.assertEqual(client.get(url + "/export").json(), response.json())
            self.assertEqual(client.get(url + "/export", params={"format": "csv"}).text, export_csv(report))

    def test_comparison_api_has_common_counts_five_measures_and_frozen_exports(self):
        self.load(forty_pairs())
        with TestClient(create_app(store=self.store)) as client:
            response = client.get("/api/v1/comparisons", params=PAIR)
            self.assertEqual(response.status_code, 200, response.text)
            saved = response.json()
            self.assertEqual(saved["context"]["method_version"], "comparison-v1")
            self.assertEqual(saved["comparison"]["cells"], dict(both_s=25, a_only_s=5, b_only_s=8, both_r=2))
            self.assertEqual(saved["comparison"]["net_difference_pp"], 7.5)
            self.assertEqual(saved["comparison"]["b_only_added_pct"], 20)
            again = client.get("/api/v1/comparisons", params={**PAIR, "window": "rolling_28_days", "as_of": "2026-09-25"})
            self.assertEqual(again.json(), saved)
            url = f'/api/v1/analyses/{saved["analysis_id"]}'
            self.assertEqual(client.get(url).json(), saved)
            self.assertEqual(client.get(url + "/export").json(), saved)
            rows = list(csv.DictReader(io.StringIO(client.get(url + "/export", params={"format": "csv"}).text)))
            self.assertEqual((rows[0]["n_paired"], rows[0]["net_difference_pp"]), ("40", "7.5"))
            swapped = client.get("/api/v1/comparisons", params={**PAIR, "antibiotic_a": "TMP", "antibiotic_b": "CIP"}).json()
            self.assertNotEqual(swapped["analysis_id"], saved["analysis_id"])
            self.assertEqual(swapped["comparison"]["net_difference_pp"], -7.5)
            meta = client.get("/api/v1/meta").json()
            self.assertNotIn("FOX", meta["comparison_antibiotics"])
            self.assertEqual(meta["analysis_methods"]["history"], "history-v1")
            client.post("/api/v1/replay/reset", json={"seed_through": "2026-09-24"}).raise_for_status()
            self.assertEqual(client.get(url + "/export").json(), saved)
            self.assertEqual(client.get("/api/v1/comparisons", params=PAIR).json()["comparison"]["n_eligible"], 0)

    def test_new_routes_validate_filters_dates_and_expected_state_consistently(self):
        self.load()
        with TestClient(create_app(store=self.store)) as client:
            for path, params in [
                ("/comparisons", {**PAIR, "antibiotic_b": "CIP"}),
                ("/comparisons", {**PAIR, "antibiotic_a": "FOX"}),
                ("/comparisons", {**PAIR, "antibiotic_a": "UNKNOWN"}),
                ("/comparisons", {**PAIR, "window": "annual"}),
                ("/comparisons", {**PAIR, "age_min": 30, "age_max": 20}),
                ("/comparisons", {**PAIR, "as_of": "2026-09-29"}),
                ("/patients", {"search": ""}), ("/patients", {"search": "000", "limit": 101}),
                ("/patients", {"search": "000", "as_of": "2026-09-29"}),
                ("/patients/0001/history", {"ward_code": "ED"}),
                ("/patients/0001/history", {"limit": 0}),
                ("/patients/0001/history", {"cursor": "invalid"}),
                ("/patients/0001/history", {"as_of": "0001-01-01"}),
                ("/patients/0001/history", {"as_of": "2026-09-29"}),
            ]:
                with self.subTest(path=path, params=params):
                    response = client.get("/api/v1" + path, params=params)
                    self.assertEqual(response.status_code, 422, response.text)
                    self.assertEqual(set(response.json()), {"error", "request_id"})
            state = client.get("/api/v1/state").json()
            stale = dict(expected_run_id="old-run", expected_revision=state["state_revision"])
            for path, params in [("/comparisons", PAIR), ("/patients", {"search": "000"}), ("/patients/0001/history", {})]:
                response = client.get("/api/v1" + path, params={**params, **stale})
                self.assertEqual(response.status_code, 409)
                self.assertEqual(response.json()["error"]["details"]["state"]["run_id"], state["run_id"])
            spec = client.get("/openapi.json").json()
            self.assertIn("/api/v1/patients/{patient_id}/history", spec["paths"])

    def test_isolate_corrections_do_not_reuse_old_ast_metadata_or_out_of_range_revisions(self):
        bundle = self.load()
        self.service.state()
        original = next(event for event in bundle.events if event.isolate_id == "i1")
        correction = original.model_copy(update={"revision": 2, "event_id": "corrected:i1:2",
            "tests": (original.tests[0].model_copy(update={"result": "R"}),)})
        self.publish_isolate(correction)
        report = self.history()
        changed = next(c for c in report.cultures if c.record.culture_id == "c1").isolates[0]
        self.assertEqual(changed.event.revision, 2)
        self.assertEqual(changed.event.tests[0].result, "R")
        self.assertIsNone(changed.enrichment)
        moved = correction.model_copy(update={"revision": 3, "event_id": "corrected:i1:3", "sample_date": date(2026, 9, 28)})
        self.publish_isolate(moved)
        self.assertEqual(next(c for c in self.history().cultures if c.record.culture_id == "c1").isolates, ())

    def publish_isolate(self, event):
        self.store.append_many([event])
        with self.service.locked() as db:
            state = self.service._active(db)
            revision = state.state_revision + 1
            db.execute("INSERT INTO run_isolates VALUES (%s, %s, %s)", (state.run_id, event.event_id, revision))
            self.service._save(db, state.model_copy(update={"state_revision": revision,
                "published_isolates": state.published_isolates + 1}))

    def test_culture_correction_is_resolved_before_date_and_patient_search(self):
        bundle = self.load()
        self.service.state()
        original = next(c for c in bundle.cultures if c.culture_id == "patient_0003")
        moved = original.model_copy(update={"sample_date": date(2026, 9, 28), "iso_week": 40})
        with self.service.locked() as db:
            state = self.service._active(db)
            db.execute("""INSERT INTO culture_events
                (event_id, culture_id, revision, patient_id, ward_code, sample_date, manifest_id, payload)
                VALUES (%s, %s, 2, %s, %s, %s, %s, %s)""", ("changed:current:2", moved.culture_id,
                moved.patient_id, moved.ward_code, moved.sample_date, state.source_manifest_id, Jsonb(moved.model_dump(mode="json"))))
            db.execute("INSERT INTO run_cultures VALUES (%s, %s, %s)", (state.run_id, "changed:current:2", state.state_revision + 1))
            self.service._save(db, state.model_copy(update={"state_revision": state.state_revision + 1,
                "published_cultures": state.published_cultures + 1}))
        self.assertEqual(self.service.patients(PatientSearchRequest(search="0003", as_of="2026-09-27")).patients, ())
        with self.assertRaises(ServiceError) as caught:
            self.history("0003")
        self.assertEqual(caught.exception.status, 404)

    def test_history_is_frozen_before_concurrent_reset_releases_new_run(self):
        self.load()
        initial = self.service.state()
        entered, release = threading.Event(), threading.Event()

        def delayed(*args):
            result = history_page(*args)
            entered.set()
            if not release.wait(timeout=5):
                raise TimeoutError("Test did not release history calculation")
            return result

        with ThreadPoolExecutor(max_workers=2) as pool:
            with patch("aei.publication.history_page", delayed):
                getting = pool.submit(self.history)
                self.assertTrue(entered.wait(timeout=5))
                resetting = pool.submit(self.service.reset, ReplayReset(seed_through="2026-09-25"))
                release.set()
                report, state = getting.result(timeout=5), resetting.result(timeout=5)
        self.assertEqual(report.context.run_id, initial.run_id)
        self.assertNotEqual(state.run_id, initial.run_id)
        self.assertEqual(len(report.cultures), 5)
        self.assertEqual(self.service.analysis(report.analysis_id), report)

    def test_history_and_comparison_survive_new_service_and_schema_initialization(self):
        self.load()
        history = self.history()
        comparison = self.service.comparison(ComparisonRequest(**PAIR))
        restarted = PublicationService(PostgresEventStore(self.url, self.schema))
        self.assertEqual(restarted.patient_history("0001", HistoryRequest(as_of="2026-09-27")), history)
        self.assertEqual(restarted.comparison(ComparisonRequest(**PAIR)), comparison)
