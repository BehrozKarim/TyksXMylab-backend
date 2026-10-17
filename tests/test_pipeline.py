import sqlite3
import tempfile
import unittest
from datetime import date, datetime, time, timezone
from pathlib import Path

from fastapi.testclient import TestClient
from pydantic import ValidationError

from aei.api import create_app
from aei.dataset import DEFAULT_DATASET, load_dataset
from aei.models import AnalysisQuery, IsolateEvent, Susceptibility
from aei.processor import ReferenceProcessor, compare_periods
from aei.store import EventConflict, SQLiteEventStore


def event(isolate="i1", patient=None, day="2026-09-10", result="S", revision=1,
          ward="ED", organism="Escherichia coli", specimen="urine", tests=None, received=None):
    sample_date = date.fromisoformat(day)
    return IsolateEvent(
        event_id=f"{isolate}:{revision}", isolate_id=isolate, revision=revision,
        patient_id=patient or isolate, culture_id=f"culture-{isolate}",
        sample_date=sample_date,
        received_at=received or datetime.combine(sample_date, time(12), timezone.utc),
        ward_code=ward, organism=organism, specimen=specimen,
        tests=tuple(tests) if tests is not None else (
            Susceptibility(antibiotic_code="CIP", result=result, guideline="EUCAST 2026"),
        ),
    )


def query(**changes):
    return AnalysisQuery(as_of=date(2026, 9, 27), **changes)


class CalculationTests(unittest.TestCase):
    def setUp(self):
        self.processor = ReferenceProcessor()

    def calculate(self, events, request=None):
        return self.processor.calculate(events, request or query())

    def test_30_tested_isolates_show_correct_percentages(self):
        result = self.calculate([event(str(i), result="S" if i < 24 else "R") for i in range(30)])
        row = result.rows[0]
        self.assertEqual((row.tested, row.susceptible, row.resistant), (30, 24, 6))
        self.assertEqual((row.susceptibility_percent, row.resistance_percent), (80, 20))
        self.assertEqual(row.status, "available")

    def test_29_tested_isolates_suppress_percentages(self):
        row = self.calculate([event(str(i)) for i in range(29)]).rows[0]
        self.assertEqual(row.tested, 29)
        self.assertEqual(row.status, "insufficient_data")
        self.assertIsNone(row.susceptibility_percent)
        self.assertIsNone(row.resistance_percent)

    def test_antibiotic_denominator_excludes_missing_tests(self):
        events = [event(str(i)) for i in range(29)] + [event("untested", tests=[])]
        row = self.calculate(events, query(antibiotic_code="CIP")).rows[0]
        self.assertEqual((row.eligible_isolates, row.tested, row.untested), (30, 29, 1))
        self.assertEqual(row.status, "insufficient_data")

    def test_first_isolate_is_selected_before_antibiotic_filter(self):
        events = [event("first", patient="p", day="2026-09-01", tests=[]),
                  event("later", patient="p", result="R")]
        row = self.calculate(events, query(antibiotic_code="CIP")).rows[0]
        self.assertEqual((row.eligible_isolates, row.tested, row.untested), (1, 0, 1))

    def test_species_and_specimen_have_independent_deduplication(self):
        events = [event("a", patient="p"), event("b", patient="p"),
                  event("c", patient="p", specimen="blood"),
                  event("d", patient="p", organism="Staphylococcus aureus")]
        self.assertEqual(sum(row.tested for row in self.calculate(events).rows), 3)

    def test_moving_window_promotes_the_next_isolate_without_new_events(self):
        events = [event("first", patient="p", day="2026-08-31"),
                  event("later", patient="p", result="R")]
        initial = self.calculate(events)
        later = self.calculate(events, AnalysisQuery(as_of=date(2026, 9, 28)))
        self.assertEqual(initial.start_date, date(2026, 8, 31))
        self.assertEqual(initial.rows[0].susceptible, 1)
        self.assertEqual(later.rows[0].resistant, 1)

    def test_window_edges_are_inclusive_and_future_samples_excluded(self):
        events = [event("before", day="2026-08-30"), event("start", day="2026-08-31"),
                  event("end", day="2026-09-27"), event("future", day="2026-09-28")]
        self.assertEqual(self.calculate(events).rows[0].tested, 2)

    def test_latest_revision_replaces_instead_of_double_counting(self):
        first = event()
        correction = event(revision=2, result="R")
        result = self.calculate([correction, first, correction])
        self.assertEqual((result.rows[0].tested, result.rows[0].resistant), (1, 1))

    def test_correction_moving_sample_outside_window_does_not_restore_old_revision(self):
        result = self.calculate([event(), event(revision=2, day="2026-08-01")])
        self.assertEqual(result.rows, ())

    def test_known_at_excludes_corrections_received_later(self):
        correction = event(revision=2, result="R", received=datetime(2026, 9, 30, tzinfo=timezone.utc))
        result = self.calculate([event(), correction], query(
            known_at=datetime(2026, 9, 27, 23, 59, tzinfo=timezone.utc)))
        self.assertEqual(result.rows[0].susceptible, 1)

    def test_ward_deduplication_choice_is_explicit(self):
        events = [event("first", patient="p", day="2026-09-01", ward="ICU"),
                  event("later", patient="p", ward="ED")]
        self.assertEqual(self.calculate(events, query(ward_code="ED")).rows[0].tested, 1)
        self.assertEqual(self.calculate(events, query(ward_code="ED", dedup_scope="all_wards")).rows, ())

    def test_each_reporting_period_selects_its_own_first_isolate(self):
        events = [event("old", patient="p", day="2026-01-01", result="R"),
                  event("recent", patient="p")]
        self.assertEqual(self.calculate(events).rows[0].susceptible, 1)
        self.assertEqual(self.calculate(events, query(start_date=date(2026, 1, 1))).rows[0].resistant, 1)

    def test_no_data_is_not_a_zero_percent_measurement(self):
        self.assertEqual(self.calculate([]).rows, ())

    def test_invalid_period_and_duplicate_antibiotic_are_rejected(self):
        with self.assertRaises(ValidationError):
            query(start_date=date(2026, 10, 1))
        with self.assertRaises(ValidationError):
            event(tests=[event().tests[0], event().tests[0]])

    def test_rolling_year_uses_calendar_anniversary(self):
        result = self.calculate([], query(period="rolling_12_months"))
        self.assertEqual((result.start_date, result.end_date), (date(2025, 9, 28), date(2026, 9, 27)))
        self.assertEqual(result.period, "rolling_12_months")

    def test_rolling_year_includes_leap_day(self):
        request = AnalysisQuery(as_of=date(2024, 3, 1), period="rolling_12_months")
        result = self.calculate([], request)
        self.assertEqual(result.start_date, date(2023, 3, 2))
        self.assertEqual((result.end_date - result.start_date).days + 1, 366)

    def test_leap_day_anniversary_is_clamped(self):
        request = AnalysisQuery(as_of=date(2024, 2, 29), period="rolling_12_months")
        self.assertEqual(request.period_start, date(2023, 3, 1))

    def test_rolling_year_boundary_is_exclusive_at_prior_anniversary(self):
        events = [event("before", day="2025-09-27"), event("start", day="2025-09-28"),
                  event("end", day="2026-09-27"), event("future", day="2026-09-28")]
        self.assertEqual(self.calculate(events, query(period="rolling_12_months")).rows[0].tested, 2)

    def test_comparison_deduplicates_the_two_periods_independently(self):
        events = [event("old", patient="p", day="2026-01-01", result="R"), event("new", patient="p")]
        result = compare_periods(self.processor, iter(events), query())
        self.assertEqual(result.rolling_28_days.rows[0].susceptible, 1)
        self.assertEqual(result.rolling_12_months.rows[0].resistant, 1)

    def test_named_and_custom_periods_cannot_be_mixed(self):
        with self.assertRaises(ValidationError):
            query(period="rolling_12_months", start_date=date(2026, 1, 1))
        with self.assertRaises(ValidationError):
            query(period="rolling_12_months", window_days=365)
        with self.assertRaises(ValueError):
            compare_periods(self.processor, [], query(period="rolling_12_months"))


class StoreAndAPITests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "events.sqlite3"
        self.store = SQLiteEventStore(self.path)

    def tearDown(self):
        self.directory.cleanup()

    def test_repeated_delivery_is_idempotent_and_history_retains_revisions(self):
        self.assertEqual(self.store.append_many([event()]), (1, 0))
        self.assertEqual(self.store.append_many([event()]), (0, 1))
        self.store.append_many([event(revision=2, result="R")])
        self.assertEqual([item.revision for item in self.store.events("i1")], [1, 2])
        self.assertEqual(self.store.count(), 2)

    def test_conflicting_batch_rolls_back_without_partial_ingestion(self):
        self.store.append_many([event()])
        with self.assertRaises(EventConflict):
            self.store.append_many([event("new"), event(result="R")])
        self.assertEqual(self.store.count(), 1)

    def test_database_rejects_changes_to_existing_history(self):
        self.store.append_many([event()])
        for statement in ["DELETE FROM isolate_events", "UPDATE isolate_events SET revision = 2"]:
            with self.assertRaises(sqlite3.IntegrityError):
                with self.store.connection() as db:
                    db.execute(statement)
        self.assertEqual(self.store.count(), 1)

    def test_api_ingestion_statistics_and_history(self):
        with TestClient(create_app(store=self.store)) as client:
            batch = [event(str(i), result="S" if i < 24 else "R").model_dump(mode="json") for i in range(30)]
            response = client.post("/events/batch", json=batch)
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["inserted"], 30)
            self.assertEqual(client.post("/events", json=batch[0]).json()["duplicates"], 1)
            response = client.get("/statistics", params={"as_of": "2026-09-27", "antibiotic_code": "CIP"})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["rows"][0]["resistance_percent"], 20)
            self.assertEqual(len(client.get("/isolates/0/history").json()), 1)
            self.assertEqual(client.get("/statistics").status_code, 422)
            self.assertEqual(client.get("/statistics", params={"as_of": "2026-09-27", "start_date": "2026-10-01"}).status_code, 422)
            self.assertEqual(client.get("/isolates/missing/history").status_code, 404)
            self.assertEqual(client.post("/events", json={"invalid": True}).status_code, 422)
            response = client.get("/statistics/comparison", params={"as_of": "2026-09-27"})
            self.assertEqual(response.status_code, 200, response.text)
            self.assertEqual(response.json()["rolling_12_months"]["start_date"], "2025-09-28")
            self.assertEqual(client.get("/statistics/comparison", params={
                "as_of": "2026-09-27", "window_days": 365}).status_code, 422)

    def test_api_returns_conflict_for_a_changed_existing_event(self):
        with TestClient(create_app(store=self.store)) as client:
            client.post("/events", json=event().model_dump(mode="json"))
            self.assertEqual(client.post("/events", json=event(result="R").model_dump(mode="json")).status_code, 409)


class SuppliedDatasetTests(unittest.TestCase):
    @unittest.skipUnless(DEFAULT_DATASET.exists(), "Set AEI_DATASET_PATH to the supplied synthetic ZIP or folder")
    def test_supplied_archive_loads_into_complete_events(self):
        events = load_dataset()
        self.assertEqual(len(events), 24571)
        self.assertEqual(sum(len(item.tests) for item in events), 172381)
        self.assertEqual(len({item.event_id for item in events}), len(events))
        report = ReferenceProcessor().calculate(events, query())
        self.assertGreater(len(report.rows), 0)
        self.assertTrue(any(row.tested >= 30 for row in report.rows))
        for row in report.rows:
            self.assertEqual(row.tested, row.susceptible + row.resistant)
            self.assertEqual(row.eligible_isolates, row.tested + row.untested)
            self.assertEqual(row.resistance_percent is None, row.tested < 30)


if __name__ == "__main__":
    unittest.main()
