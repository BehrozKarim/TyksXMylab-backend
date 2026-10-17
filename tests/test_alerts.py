import os
import tempfile
import unittest
from dataclasses import replace
from datetime import date
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError

from aei.alert_models import AlertConfiguration, AlertMonitor, load_alert_configuration
from aei.alerts import alert_bounds, calculate_alert
from test_evidence import enrichment, snapshot
from test_pipeline import event


MONITOR = AlertMonitor(ward_code="ED")
CONFIG = AlertConfiguration(monitors=(MONITOR,))
AS_OF = date(2026, 9, 27)


def populations(baseline_n=30, baseline_r=6, recent_n=30, recent_r=9):
    return ([event(f"baseline-{i}", day="2026-01-01", result="R" if i < baseline_r else "S") for i in range(baseline_n)]
            + [event(f"recent-{i}", day="2026-09-10", result="R" if i < recent_r else "S") for i in range(recent_n)])


class AlertCalculationTests(unittest.TestCase):
    def calculate(self, events, monitor=MONITOR, configuration=CONFIG, metadata=None):
        return calculate_alert(snapshot(events, metadata), monitor, configuration, AS_OF)

    def test_exact_threshold_triggers_without_float_subtraction_rounding(self):
        result = self.calculate(populations())
        self.assertEqual(result.status, "triggered")
        self.assertEqual(result.resistance_change_pp, 10)
        self.assertEqual((result.baseline.resistance_pct, result.recent.resistance_pct), (20, 30))
        self.assertEqual((result.baseline.n_tested, result.recent.n_tested), (30, 30))

    def test_below_threshold_negative_change_and_custom_threshold(self):
        for recent_r in (0, 6, 8):
            with self.subTest(recent_r=recent_r):
                self.assertEqual(self.calculate(populations(recent_r=recent_r)).status, "below_threshold")
        self.assertEqual(self.calculate(populations(), configuration=AlertConfiguration(
            threshold_pp=10.000001, monitors=(MONITOR,))).status, "below_threshold")

    def test_29_in_either_period_suppresses_change_and_its_percentage(self):
        for baseline_n, recent_n in ((29, 30), (30, 29), (29, 29), (0, 30)):
            with self.subTest(baseline_n=baseline_n, recent_n=recent_n):
                row = self.calculate(populations(baseline_n=baseline_n, recent_n=recent_n))
                self.assertEqual(row.status, "insufficient_evidence")
                self.assertIsNone(row.resistance_change_pp)
                for window in (row.baseline, row.recent):
                    if window.n_tested < 30:
                        self.assertIsNone(window.resistance_pct)

    def test_unrounded_change_is_retained(self):
        row = self.calculate(populations(recent_n=31, recent_r=10))
        self.assertAlmostEqual(row.resistance_change_pp, 1000 / 31 - 20)
        self.assertNotEqual(row.resistance_change_pp, round(row.resistance_change_pp, 2))

    def test_baseline_and_recent_boundaries_are_disjoint_and_end_is_exclusive(self):
        events = [event(str(i), day=day) for i, day in enumerate(
            ("2025-08-30", "2025-08-31", "2026-08-30", "2026-08-31", "2026-09-27", "2026-09-28"))]
        row = self.calculate(events)
        self.assertEqual((row.baseline.n_tested, row.recent.n_tested), (2, 2))
        self.assertEqual(row.baseline.start_date, date(2025, 8, 31))
        self.assertEqual(row.baseline.end_date_exclusive, row.recent.start_date)
        self.assertEqual(row.recent.end_date_exclusive, date(2026, 9, 28))

    def test_leap_anniversary_clamps_recent_start_without_legacy_plus_one(self):
        self.assertEqual(alert_bounds(date(2024, 3, 27)),
                         (date(2023, 2, 28), date(2024, 2, 29), date(2024, 3, 28)))
        self.assertEqual(alert_bounds(date(2025, 3, 28)),
                         (date(2024, 3, 1), date(2025, 3, 1), date(2025, 3, 29)))
        for unsupported in (date.min, date.max, date(2, 1, 1)):
            row = calculate_alert(snapshot(()), MONITOR, CONFIG, unsupported)
            self.assertEqual((row.status, row.reason), ("unavailable", "unsupported_date_range"))

    def test_dedup_is_independent_per_period_and_shared_patients_are_disclosed(self):
        events = [event("b1", patient="same", day="2026-01-01"),
                  event("b2", patient="same", day="2026-02-01", result="R"),
                  event("r1", patient="same", result="R"), event("r2", patient="same", day="2026-09-20")]
        row = self.calculate(events)
        self.assertEqual((row.baseline.n_tested, row.baseline.n_r, row.recent.n_tested, row.recent.n_r), (1, 0, 1, 1))
        self.assertEqual(row.shared_patients_between_windows, 1)
        self.assertEqual(row.recent.measurements.raw_isolates_before_dedup_and_demographics, 2)

    def test_empty_day_expires_recent_isolates_and_moves_them_into_baseline(self):
        items = [item.model_copy(update={"sample_date": date(2026, 8, 31)}) if item.isolate_id.startswith("recent") else item
                 for item in populations()]
        source = snapshot(items)
        before = calculate_alert(source, MONITOR, CONFIG, AS_OF)
        after = calculate_alert(source, MONITOR, CONFIG, date(2026, 9, 28))
        self.assertEqual(before.status, "triggered")
        self.assertEqual((after.status, after.recent.n_tested, after.baseline.n_tested), ("insufficient_evidence", 0, 60))
        self.assertIsNone(after.resistance_change_pp)

    def test_first_untested_is_never_replaced_with_later_test(self):
        events = populations(recent_n=29) + [event("first", patient="extra", day="2026-09-01", tests=[]),
                                           event("later", patient="extra", result="R")]
        row = self.calculate(events)
        self.assertEqual((row.recent.n_eligible, row.recent.n_tested, row.recent.n_not_tested), (30, 29, 1))
        self.assertEqual(row.status, "insufficient_evidence")

    def test_same_day_tie_and_corrections_resolve_before_filters(self):
        first = event("a", patient="same", result="R")
        later = event("z", patient="same")
        self.assertEqual(self.calculate([later, first]).recent.n_r, 1)
        outside = event("a", patient="same", revision=2, day="2026-09-28")
        row = self.calculate([first, outside, later])
        self.assertEqual((row.recent.n_tested, row.recent.n_r), (1, 0))

    def test_ward_selection_policy_is_shared_with_evidence(self):
        events = [event("early", patient="same", ward="ICU", day="2026-09-01"),
                  event("later", patient="same", result="R")]
        self.assertEqual(self.calculate(events).recent.n_r, 1)
        row = self.calculate(events, monitor=MONITOR.model_copy(update={"dedup_scope": "all_wards"}))
        self.assertEqual(row.recent.n_tested, 0)
        self.assertEqual(self.calculate(events, monitor=MONITOR.model_copy(update={"ward_code": None})).recent.n_r, 0)

    def test_demographics_follow_selection_and_correction_metadata_is_not_borrowed(self):
        first, later = event("a", patient="same", day="2026-09-01"), event("b", patient="same")
        metadata = {first.event_id: enrichment(first, sex="M"), later.event_id: enrichment(later, sex="F")}
        filtered = MONITOR.model_copy(update={"sex": "F"})
        self.assertEqual(self.calculate([first, later], filtered, metadata=metadata).recent.n_eligible, 0)
        correction = event("a", patient="same", day="2026-09-01", revision=2)
        row = self.calculate([first, later, correction], filtered, metadata=metadata)
        self.assertEqual(row.recent.metadata.excluded_unknown, 1)
        self.assertEqual(row.recent.n_tested, 0)

    def test_context_populations_timing_proxy_and_count_only_organism_mix(self):
        items = [event("day3", ward="ICU"), event("day2", ward="ICU"), event("out"),
                 event("other-species", organism="Other species"), event("unknown", ward="ICU")]
        metadata = {item.event_id: enrichment(item, age=17 if item.isolate_id == "day3" else 65) for item in items[:-1]}
        for item, admission in ((items[0], date(2026, 9, 8)), (items[1], date(2026, 9, 9))):
            metadata[item.event_id] = metadata[item.event_id].model_copy(update={"admission_date": admission})
        source = replace(snapshot(items, metadata), ward_types={"ED": "out", "ICU": "in"})
        row = calculate_alert(source, MONITOR.model_copy(update={"ward_code": None}), CONFIG, AS_OF).recent
        detail = row.measurements
        self.assertEqual(row.n_eligible, 4)
        self.assertEqual(detail.organism_mix_counts, {"Escherichia coli": 4, "Other species": 1})
        self.assertEqual((detail.hospital_day_3_or_later, detail.hospital_day_1_or_2,
                          detail.timing_unknown, detail.timing_not_applicable), (1, 1, 1, 1))
        self.assertEqual(detail.age_bin_counts, {"0-17": 1, "18-64": 0, "65+": 2, "unknown": 1})
        self.assertEqual(sum(detail.sex_counts.values()), row.n_eligible)
        self.assertEqual(sum(detail.collection_setting_counts.values()), row.n_eligible)

    def test_missing_and_screening_configuration_is_unavailable_not_a_false_alert(self):
        for change, reason in (({"ward_code": "missing"}, "unknown_ward"),
                               ({"organism": "missing"}, "unknown_organism"),
                               ({"antibiotic_code": "missing"}, "unknown_antibiotic"),
                               ({"antibiotic_code": "FOX"}, "screening_antibiotic"),
                               ({"antibiotic_code": "TMP", "specimen": "blood"}, "antibiotic_not_applicable")):
            row = self.calculate(populations(), MONITOR.model_copy(update=change))
            self.assertEqual((row.status, row.reason), ("unavailable", reason))
            self.assertIsNone(row.recent)
            self.assertIsNone(row.resistance_change_pp)

    def test_configuration_validation_hashes_and_explicit_file_errors(self):
        for values in ({"threshold_pp": 0}, {"threshold_pp": float("nan")}, {"minimum_tested": 29},
                       {"rule_version": "unknown"}, {"monitors": [MONITOR, MONITOR]}):
            with self.assertRaises(ValidationError):
                AlertConfiguration(**values)
        with self.assertRaises(ValidationError):
            AlertMonitor(age_min=65, age_max=30)
        self.assertEqual(MONITOR.cohort_hash, MONITOR.model_copy(update={"antibiotic_code": "TMP"}).cohort_hash)
        self.assertNotEqual(MONITOR.monitor_id, MONITOR.model_copy(update={"antibiotic_code": "TMP"}).monitor_id)
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "alerts.json"
            with patch.dict(os.environ, {"AEI_ALERT_CONFIG": str(path)}):
                with self.assertRaises(FileNotFoundError):
                    load_alert_configuration()
                path.write_text(CONFIG.model_dump_json(), encoding="utf-8")
                self.assertEqual(load_alert_configuration(), CONFIG)
                path.write_text('{"unexpected": true}', encoding="utf-8")
                with self.assertRaises(ValidationError):
                    load_alert_configuration()
