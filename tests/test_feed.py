import copy
import json
import tempfile
import unittest
from datetime import date
from unittest.mock import patch

import httpx
from pydantic import ValidationError

from aei.dataset import load_bundle
from aei.feed_models import CultureBundle, FeedBatch, FeedStart
from aei.feed_sender import batches, culture_bundles, send_batch
from test_import import write_fixture


def fixture_dataset():
    with tempfile.TemporaryDirectory() as folder:
        return load_bundle(write_fixture(folder))


def changed_bundle(bundle, *, identity="live", day="2026-09-27", patient="live-patient", revision=1, result="R"):
    value = bundle.model_dump(mode="json", by_alias=True)
    stamp = date.fromisoformat(day)
    culture_id = f"{identity}-culture"
    culture_event_id = f"lab:{culture_id}:{revision}"
    value.update(culture_event_id=culture_event_id, culture_revision=revision)
    value["patient"]["patient_id"] = patient
    value["culture"].update(culture_id=culture_id, patient_id=patient, sample_date=day,
        iso_year=stamp.isocalendar().year, iso_week=stamp.isocalendar().week,
        age_years=stamp.year - value["patient"]["birth_year"])
    for index, item in enumerate(value["isolates"]):
        event_id = f"lab:{identity}-isolate-{index}:{revision}"
        item["event"].update(event_id=event_id, isolate_id=f"{identity}-isolate-{index}", revision=revision,
            culture_id=culture_id, patient_id=patient, sample_date=day, received_at=f"{day}T12:00:00Z")
        item["event"]["tests"][0]["result"] = result
        item["enrichment"].update(event_id=event_id, culture_event_id=culture_event_id,
                                   age_years=value["culture"]["age_years"])
    return CultureBundle.model_validate(value)


class FeedContractTests(unittest.TestCase):
    def setUp(self):
        self.dataset = fixture_dataset()
        self.bundles = tuple(culture_bundles(self.dataset))

    def test_sender_preserves_every_culture_and_exact_original_event_metadata(self):
        self.assertEqual(len(self.bundles), 4)
        self.assertEqual(sum(len(item.isolates) for item in self.bundles), 2)
        self.assertEqual([item.culture for item in self.bundles], list(self.dataset.cultures))
        originals = {item.event_id: item for item in self.dataset.events}
        for bundle in self.bundles:
            for item in bundle.isolates:
                self.assertEqual(item.event.canonical_json(), originals[item.event.event_id].canonical_json())
                self.assertIn(item.enrichment, self.dataset.enrichments)

    def test_validation_rejects_incomplete_mislinked_and_inconsistent_bundles(self):
        original = self.bundles[-1].model_dump(mode="json", by_alias=True)
        changes = [lambda v: v.update(isolates=[]), lambda v: v["patient"].update(patient_id="other"),
                   lambda v: v["ward"].update(ward_code="OTHER"), lambda v: v["culture"].update(age_years=99),
                   lambda v: v["isolates"][0]["event"].update(patient_id="other"),
                   lambda v: v["isolates"][0]["event"].update(revision=2),
                   lambda v: v["isolates"][0]["enrichment"].update(event_id="old-version"),
                   lambda v: v["isolates"][0]["enrichment"].update(sex="M"),
                   lambda v: v["isolates"][0]["enrichment"].update(esbl=2),
                   lambda v: v.update(antibiotics=[]),
                   lambda v: v["antibiotics"][0].update(reported_for="blood"),
                   lambda v: v["culture"].update(result="negative"),
                   lambda v: v["isolates"][0]["event"].update(received_at="2026-09-26T12:00:00Z")]
        for change in changes:
            value = copy.deepcopy(original)
            change(value)
            with self.subTest(change=change), self.assertRaises(ValidationError):
                CultureBundle.model_validate(value)

    def test_batch_limits_duplicate_cultures_and_simulated_clock_validation(self):
        args = dict(run_id="run", source_id="source", delivery_id="delivery")
        for kwargs in ({"bundles": (self.bundles[0], self.bundles[0])}, {}, {"bundles": self.bundles * 26}):
            with self.assertRaises(ValidationError):
                FeedBatch(**args, **kwargs)
        with self.assertRaises(ValidationError):
            FeedStart(source_id="s", source_label="Simulator", clock_mode="simulated")
        self.assertEqual(FeedBatch(**args, as_of="2026-09-28").bundles, ())

    def test_canonical_hash_ignores_order_delivery_id_and_equivalent_timestamp_offsets(self):
        batch = FeedBatch(run_id="run", source_id="source", delivery_id="delivery", as_of="2026-09-27", bundles=self.bundles)
        payload = batch.model_dump(mode="json", by_alias=True)
        payload["bundles"].reverse()
        payload["delivery_id"] = "retry-under-another-id"
        payload["bundles"][0]["isolates"][0]["event"]["received_at"] = "2026-09-27T15:00:00+03:00"
        self.assertEqual(batch.payload_hash, FeedBatch.model_validate(payload).payload_hash)
        payload["bundles"][0]["isolates"][0]["event"]["tests"][0]["result"] = "S"
        self.assertNotEqual(batch.payload_hash, FeedBatch.model_validate(payload).payload_hash)

    def test_sender_includes_empty_dates_and_uses_stable_batch_ids(self):
        args = (self.dataset, "run", "source", date(2026, 9, 25), date(2026, 9, 28), 1)
        first, repeated = list(batches(*args)), list(batches(*args))
        self.assertEqual(first, repeated)
        self.assertEqual([len(item.bundles) for item in first], [1, 1, 1, 1, 0])
        self.assertEqual(first[-1].as_of, date(2026, 9, 28))
        self.assertEqual(len({item.delivery_id for item in first}), 5)

    def test_sender_retries_same_body_and_waits_until_published(self):
        bodies, reads = [], []

        def handle(request):
            if request.method == "POST":
                bodies.append(json.loads(request.content))
                if len(bodies) == 1:
                    raise httpx.ReadTimeout("simulated lost acknowledgment", request=request)
                return httpx.Response(202, json={"status": "pending", "receipt_url": "/receipt"})
            reads.append(request.url.path)
            return httpx.Response(200, json={"status": "published", "receipt_url": "/receipt"})

        batch = next(batches(self.dataset, "run", "source", date(2026, 9, 25), date(2026, 9, 25)))
        with httpx.Client(base_url="http://sender.test", transport=httpx.MockTransport(handle)) as client:
            with patch("aei.feed_sender.time.sleep"):
                self.assertEqual(send_batch(client, batch)["status"], "published")
        self.assertEqual(bodies[0], bodies[1])
        self.assertEqual(reads, ["/receipt"])
