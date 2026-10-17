import unittest
from dataclasses import replace
from datetime import date, datetime, timezone

from pydantic import ValidationError

from aei.dataset_models import AntibioticRecord, DatasetManifest, FileSummary, IsolateEnrichment, ObservedPanel
from aei.evidence import EvidenceQueryError, calculate_evidence, wilson_interval
from aei.evidence_models import EvidenceQuery
from aei.evidence_source import EvidenceSnapshot
from aei.models import AnalysisQuery
from aei.processor import ReferenceProcessor
from test_pipeline import event


def request(**changes):
    values = dict(as_of=date(2026, 9, 27), organism="Escherichia coli", specimen="urine", antibiotic_code="CIP")
    values.update(changes)
    return EvidenceQuery(**values)


def enrichment(item, sex="F", age=40):
    return IsolateEnrichment(event_id=item.event_id, culture_event_id=f"culture:{item.culture_id}:1",
        sex=sex, birth_year=1986, home_area="Synthetic", age_years=age, admission_date=None,
        isolate_no=1, quantity="", esbl=0, mrsa=0, vre=0, cpe=0)


def snapshot(events, metadata=None):
    events = tuple(events)
    codes = {"CIP": "all", "TMP": "urine", "FOX": "all"}
    drugs = {code: AntibioticRecord(antibiotic_code=code,
        antibiotic="cefoxitin (screen)" if code == "FOX" else code,
        antibiotic_fi=code, **{"class": "MRSA screen" if code == "FOX" else "example"}, reported_for=restriction)
        for code, restriction in codes.items()}
    manifest = DatasetManifest(manifest_id="authored-fixture", files={
        "cultures.csv": FileSummary(sha256="fixture", rows=len(events)),
        "isolates.csv": FileSummary(sha256="fixture", rows=len(events))},
        source_start=date(2023, 10, 2), source_end=date(2026, 9, 27),
        observed_panels=(ObservedPanel(organism="Escherichia coli", specimen="urine", antibiotic_codes=("CIP",)),))
    return EvidenceSnapshot(events=events,
        enrichments={item.event_id: enrichment(item) for item in events} if metadata is None else metadata,
        antibiotics=drugs, ward_codes=frozenset({"ED", "ICU"}), manifest=manifest,
        imported_cultures=len(events), imported_culture_start=manifest.source_start,
        imported_culture_end=manifest.source_end)


class EvidenceTests(unittest.TestCase):
    def recent(self, events, query=None, metadata=None):
        return calculate_evidence(snapshot(events, metadata), query or request()).windows.rolling_28_days

    def test_distinct_empty_missing_test_and_low_count_statuses(self):
        for events, expected in (([], "empty_cohort"), ([event(tests=[])], "no_tests"),
                                 ([event(str(i)) for i in range(29)], "insufficient_data")):
            with self.subTest(status=expected):
                row = self.recent(events)
                self.assertEqual(row.status, expected)
                self.assertIsNone(row.susceptibility_pct)
                self.assertIsNone(row.resistance_pct)
                self.assertEqual(row.ci95.model_dump(), {"susceptibility": {"lower": None, "upper": None},
                                                        "resistance": {"lower": None, "upper": None}})

    def test_thirty_tested_displays_unrounded_percentages_and_intervals(self):
        row = self.recent([event(str(i), result="S" if i < 23 else "R") for i in range(30)])
        self.assertEqual(row.status, "available")
        self.assertEqual((row.n_eligible, row.n_tested, row.n_s, row.n_r, row.n_not_tested), (30, 30, 23, 7, 0))
        self.assertEqual(row.susceptibility_pct, 100 * 23 / 30)
        self.assertNotEqual(row.susceptibility_pct, round(row.susceptibility_pct, 2))
        self.assertLess(row.ci95.susceptibility.lower, row.susceptibility_pct)
        self.assertGreater(row.ci95.susceptibility.upper, row.susceptibility_pct)
        self.assertAlmostEqual(row.ci95.resistance.lower, 100 - row.ci95.susceptibility.upper)

    def test_wilson_reference_values_and_extreme_outcomes(self):
        # The implementation plan's 32/40 example, published to three decimals.
        interval = wilson_interval(32, 40)
        self.assertAlmostEqual(interval.lower, 65.243, places=3)
        self.assertAlmostEqual(interval.upper, 89.500, places=3)
        zero, all_s = wilson_interval(0, 30), wilson_interval(30, 30)
        self.assertEqual(zero.lower, 0)
        self.assertEqual(all_s.upper, 100)
        self.assertGreater(zero.upper, 0)
        self.assertLess(all_s.lower, 100)
        self.assertEqual(wilson_interval(0, 0).model_dump(), {"lower": None, "upper": None})

    def test_dictionary_restriction_has_priority_and_screen_is_labeled(self):
        report = calculate_evidence(snapshot([]), request(specimen="blood", antibiotic_code="TMP"))
        self.assertEqual(report.windows.rolling_28_days.status, "not_applicable")
        self.assertIsNone(report.windows.rolling_28_days.resistance_pct)
        self.assertTrue(calculate_evidence(snapshot([]), request(antibiotic_code="FOX")).context.antibiotic_is_screening)

    def test_antibiotic_selection_does_not_promote_a_later_tested_isolate(self):
        row = self.recent([event("first", patient="p", day="2026-09-01", tests=[]),
                           event("later", patient="p", result="R")])
        self.assertEqual((row.n_eligible, row.n_tested, row.n_not_tested), (1, 0, 1))
        self.assertEqual(row.status, "no_tests")

    def test_demographic_filters_are_inclusive_and_apply_after_selection(self):
        first, later = event("first", patient="p", day="2026-09-01"), event("later", patient="p")
        third = event("another", patient="other")
        metadata = {first.event_id: enrichment(first, age=39), later.event_id: enrichment(later, age=40),
                    third.event_id: enrichment(third, age=40)}
        row = self.recent([first, later, third], request(sex="F", age_min=40, age_max=40), metadata)
        self.assertEqual(row.n_eligible, 1)
        self.assertEqual(row.metadata.retained_before_demographic_filters, 2)
        self.assertEqual(row.metadata.excluded_nonmatching, 1)

    def test_missing_metadata_is_reported_and_only_excluded_when_required(self):
        items = [event("known"), event("unknown")]
        metadata = {items[0].event_id: enrichment(items[0])}
        unfiltered = self.recent(items, metadata=metadata)
        filtered = self.recent(items, request(sex="F"), metadata)
        self.assertEqual(unfiltered.n_eligible, 2)
        self.assertEqual(unfiltered.metadata.unknown_age, 1)
        self.assertEqual(unfiltered.metadata.excluded_unknown, 0)
        self.assertEqual(filtered.n_eligible, 1)
        self.assertEqual(filtered.metadata.excluded_unknown, 1)

    def test_correction_does_not_reuse_old_revision_metadata(self):
        original = event()
        correction = event(revision=2, result="R", received=datetime(2026, 9, 29, tzinfo=timezone.utc))
        data = snapshot([original, correction], {original.event_id: enrichment(original)})
        current = calculate_evidence(data, request(sex="F")).windows.rolling_28_days
        self.assertEqual(current.status, "empty_cohort")
        self.assertEqual(current.metadata.excluded_unknown, 1)
        earlier = calculate_evidence(data, request(sex="F", known_at=datetime(2026, 9, 27, tzinfo=timezone.utc)))
        self.assertEqual(earlier.windows.rolling_28_days.n_s, 1)

    def test_correction_outside_cohort_never_restores_old_record(self):
        original, correction = event(), event(revision=2, day="2026-08-01")
        report = calculate_evidence(snapshot([original, correction]), request())
        self.assertEqual(report.windows.rolling_28_days.n_eligible, 0)
        self.assertEqual(report.windows.rolling_12_months.n_eligible, 1)

    def test_periods_select_first_isolates_independently_and_expire_without_arrivals(self):
        items = [event("old", patient="p", day="2026-08-31", result="R"), event("new", patient="p")]
        report = calculate_evidence(snapshot(items), request(as_of=date(2026, 9, 28)))
        self.assertEqual(report.windows.rolling_28_days.n_s, 1)
        self.assertEqual(report.windows.rolling_12_months.n_r, 1)

    def test_ward_policy_remains_explicit_and_all_wards_reselects_patients(self):
        items = [event("first", patient="p", day="2026-09-01", ward="ICU"),
                 event("later", patient="p", ward="ED", result="R")]
        ed = calculate_evidence(snapshot(items), request(ward_code="ED"))
        global_ed = calculate_evidence(snapshot(items), request(ward_code="ED", dedup_scope="all_wards"))
        all_wards = calculate_evidence(snapshot(items), request())
        self.assertEqual(ed.windows.rolling_28_days.n_r, 1)
        self.assertEqual(global_ed.windows.rolling_28_days.n_eligible, 0)
        self.assertEqual(all_wards.windows.rolling_28_days.n_eligible, 1)
        self.assertEqual(all_wards.windows.rolling_28_days.n_s, 1)
        self.assertEqual(all_wards.context.ward_scope, "all_wards")
        self.assertEqual(ed.context.filters.dedup_scope, "selected_cohort")

    def test_combined_wards_meet_threshold_without_averaging_suppressed_percentages(self):
        items = [event(str(i), ward="ED" if i < 15 else "ICU", result="S" if i < 18 else "R") for i in range(30)]
        legacy = ReferenceProcessor().calculate(items, AnalysisQuery(as_of=date(2026, 9, 27)))
        self.assertTrue(all(row.susceptibility_percent is None for row in legacy.rows))
        report = calculate_evidence(snapshot(items), request())
        self.assertEqual(report.windows.rolling_28_days.susceptibility_pct, 60)

    def test_coverage_and_import_completion_are_independent_of_threshold(self):
        items = [event(str(i), day="2023-10-15") for i in range(30)]
        data = snapshot(items)
        report = calculate_evidence(data, request(as_of=date(2023, 10, 20)))
        row = report.windows.rolling_12_months
        self.assertEqual(row.status, "available")
        self.assertFalse(row.coverage.source_date_span_covers_window)
        self.assertTrue(row.coverage.source_fully_imported)
        self.assertEqual(row.coverage.overlap_start, date(2023, 10, 2))
        partial = replace(data, imported_cultures=5)
        self.assertFalse(calculate_evidence(partial, request()).windows.rolling_28_days.coverage.source_fully_imported)
        future = calculate_evidence(data, request(as_of=date(2028, 1, 1))).windows.rolling_12_months
        self.assertIsNone(future.coverage.overlap_start)
        self.assertIsNone(future.coverage.overlap_end)

    def test_legacy_calendar_boundaries_are_used_in_both_evidence_windows(self):
        for as_of in (date(2024, 2, 28), date(2024, 2, 29), date(2025, 2, 28)):
            with self.subTest(as_of=as_of):
                report = calculate_evidence(snapshot([]), request(as_of=as_of))
                self.assertEqual(report.windows.rolling_12_months.start_date,
                                 AnalysisQuery(as_of=as_of, period="rolling_12_months").period_start)

    def test_invalid_filters_are_rejected(self):
        for changes in (dict(age_min=-1), dict(age_min=50, age_max=20), dict(sex="unknown"), dict(as_of=date.min)):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                request(**changes)
        for field in ("organism", "antibiotic_code", "ward_code"):
            with self.subTest(field=field), self.assertRaises(EvidenceQueryError) as caught:
                calculate_evidence(snapshot([]), request(**{field: "unknown"}))
            self.assertEqual(caught.exception.field, field)
