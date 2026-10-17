import unittest
from datetime import date

from pydantic import ValidationError

from aei.analysis_models import ComparisonQuery
from aei.comparisons import calculate_comparison
from aei.evidence import EvidenceQueryError
from aei.history import history_start
from aei.models import Susceptibility
from test_evidence import enrichment, snapshot
from test_pipeline import event


def paired(key, a="S", b="S", **kwargs):
    tests = [Susceptibility(antibiotic_code=code, result=value, guideline="Authored fixture")
             for code, value in (("CIP", a), ("TMP", b)) if value is not None]
    return event(key, tests=tests, **kwargs)


def query(**kwargs):
    return ComparisonQuery(**{**dict(as_of="2026-09-27", organism="Escherichia coli",
        specimen="urine", antibiotic_a="CIP", antibiotic_b="TMP"), **kwargs})


class ComparisonTests(unittest.TestCase):
    def compare(self, events, *, metadata=None, **kwargs):
        return calculate_comparison(snapshot(events, metadata), query(**kwargs))[0]

    def test_authored_four_cell_example_distinguishes_net_from_added_activity(self):
        values = [("S", "S")] * 25 + [("S", "R")] * 5 + [("R", "S")] * 8 + [("R", "R")] * 2
        row = self.compare([paired(str(i), a, b) for i, (a, b) in enumerate(values)])
        self.assertEqual(row.cells.model_dump(), dict(both_s=25, a_only_s=5, b_only_s=8, both_r=2))
        self.assertEqual((row.n_eligible, row.n_paired), (40, 40))
        self.assertEqual((row.a_susceptibility_pct, row.b_susceptibility_pct, row.net_difference_pp,
                          row.b_only_added_pct, row.at_least_one_s_pct), (75, 82.5, 7.5, 20, 95))

    def test_swapping_drugs_changes_direction_not_common_denominator(self):
        events = [paired(str(i), "R" if i < 20 else "S", "S") for i in range(30)]
        ab = self.compare(events)
        ba = self.compare(events, antibiotic_a="TMP", antibiotic_b="CIP")
        self.assertEqual(ab.n_paired, ba.n_paired)
        self.assertEqual(ab.net_difference_pp, -ba.net_difference_pp)
        self.assertEqual(ab.cells.b_only_s, ba.cells.a_only_s)
        self.assertEqual(ab.at_least_one_s_pct, ba.at_least_one_s_pct)
        self.assertEqual(ba.b_only_added_pct, 0)

    def test_marginals_reaching_30_do_not_override_29_paired_suppression(self):
        events = [paired(str(i)) for i in range(29)]
        events += [paired("a", b=None), paired("b", a=None), paired("neither", a=None, b=None)]
        row = self.compare(events)
        self.assertEqual((row.n_tested_a, row.n_tested_b, row.n_paired), (30, 30, 29))
        self.assertEqual((row.n_eligible, row.n_missing_a, row.n_missing_b, row.n_missing_either), (32, 2, 2, 3))
        self.assertEqual(row.n_eligible, row.n_paired + row.n_missing_either)
        self.assertEqual(sum(row.cells.model_dump().values()), row.n_paired)
        self.assertEqual(row.status, "insufficient_data")
        for field in ("a_susceptibility_pct", "b_susceptibility_pct", "net_difference_pp", "b_only_added_pct", "at_least_one_s_pct"):
            self.assertIsNone(getattr(row, field))
        available = self.compare(events + [paired("thirtieth", "R", "S")])
        self.assertEqual(available.status, "available")
        self.assertEqual(available.b_only_added_pct, 100 / 30)

    def test_all_resistant_has_zero_activity_and_no_fabricated_difference(self):
        row = self.compare([paired(str(i), "R", "R") for i in range(30)])
        self.assertEqual(row.cells.both_r, 30)
        self.assertEqual((row.a_susceptibility_pct, row.b_susceptibility_pct,
                          row.net_difference_pp, row.at_least_one_s_pct), (0, 0, 0, 0))

    def test_statuses_distinguish_missing_pairs_empty_and_dictionary_restriction(self):
        self.assertEqual(self.compare([]).status, "empty_cohort")
        self.assertEqual(self.compare([paired("a", b=None), paired("b", a=None)]).status, "no_paired_tests")
        self.assertEqual(self.compare([], specimen="blood").status, "not_applicable")
        blood = [paired(str(i)).model_copy(update={"specimen": "blood"}) for i in range(30)]
        row = self.compare(blood, specimen="blood")
        self.assertEqual((row.status, row.n_paired), ("not_applicable", 30))
        self.assertIsNone(row.at_least_one_s_pct)

    def test_unknown_identical_and_screening_drugs_are_rejected(self):
        for changes in ({"antibiotic_a": "unknown"}, {"antibiotic_b": "FOX"}, {"antibiotic_a": "FOX"},
                        {"ward_code": "unknown"}, {"organism": "unknown"}):
            with self.subTest(changes=changes), self.assertRaises(EvidenceQueryError):
                self.compare([], **changes)
        for changes in ({"antibiotic_b": "CIP"}, {"window": "annual"}, {"age_min": 80, "age_max": 20}):
            with self.subTest(changes=changes), self.assertRaises(ValidationError):
                query(**changes)

    def test_first_untested_isolate_is_not_replaced_to_make_pair(self):
        row = self.compare([paired("early", b=None, patient="same", day="2026-09-01"),
                            paired("late", patient="same")])
        self.assertEqual((row.n_eligible, row.n_paired, row.n_missing_b), (1, 0, 1))

    def test_demographic_filters_follow_first_isolate_selection(self):
        first = paired("early", patient="same", day="2026-09-01")
        later = paired("late", patient="same")
        row = self.compare([first, later], metadata={first.event_id: enrichment(first, age=20),
            later.event_id: enrichment(later, age=40)}, age_min=30)
        self.assertEqual(row.n_eligible, 0)
        self.assertEqual(row.metadata.excluded_nonmatching, 1)

    def test_unknown_enrichment_on_correction_is_not_borrowed(self):
        first = paired("one")
        corrected = paired("one", revision=2, a="R")
        row = self.compare([first, corrected], metadata={first.event_id: enrichment(first)}, sex="F")
        self.assertEqual(row.n_eligible, 0)
        self.assertEqual(row.metadata.excluded_unknown, 1)

    def test_latest_revision_is_resolved_before_window_or_ward_filters(self):
        first = paired("one", ward="ED")
        correction = paired("one", revision=2, ward="ICU")
        self.assertEqual(self.compare([first, correction], ward_code="ED").n_eligible, 0)
        outside = correction.model_copy(update={"sample_date": date(2025, 1, 1)})
        self.assertEqual(self.compare([first, outside]).n_eligible, 0)

    def test_knowledge_cutoff_and_both_reporting_periods_keep_existing_rules(self):
        old = paired("old", patient="same", day="2026-02-01", a="R", b="R")
        recent = paired("recent", patient="same", a="S", b="S")
        self.assertEqual(self.compare([old, recent]).cells.both_s, 1)
        self.assertEqual(self.compare([old, recent], window="rolling_12_months").cells.both_r, 1)
        corrected = paired("recent", patient="same", revision=2, a="R", b="S").model_copy(
            update={"received_at": recent.received_at.replace(day=28)})
        self.assertEqual(self.compare([recent, corrected], known_at=recent.received_at).cells.both_s, 1)
        self.assertEqual(self.compare([recent, corrected]).cells.b_only_s, 1)

    def test_combined_ward_cohort_is_reselected_and_ward_policy_remains_explicit(self):
        early = paired("early", patient="same", ward="ICU", day="2026-09-01")
        later = paired("late", patient="same", ward="ED")
        self.assertEqual(self.compare([early, later]).n_eligible, 1)
        self.assertEqual(self.compare([early, later], ward_code="ED").n_eligible, 1)
        self.assertEqual(self.compare([early, later], ward_code="ED", dedup_scope="all_wards").n_eligible, 0)

    def test_history_anniversary_is_inclusive_and_distinct_from_surveillance_start(self):
        self.assertEqual(history_start(date(2026, 9, 27)), date(2025, 9, 27))
        self.assertEqual(history_start(date(2024, 2, 29)), date(2023, 2, 28))
        self.assertEqual(history_start(date(2024, 3, 1)), date(2023, 3, 1))
