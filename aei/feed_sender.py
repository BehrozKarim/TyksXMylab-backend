"""A standalone HTTP producer that sends unchanged source culture bundles."""

import argparse
import json
import os
import time
from collections import defaultdict
from datetime import date, timedelta

import httpx

from .dataset import load_bundle
from .feed_models import CultureBundle, FeedBatch, FeedIsolate


def culture_bundles(dataset):
    patients = {item.patient_id: item for item in dataset.patients}
    wards = {item.ward_code: item for item in dataset.wards}
    drugs = {item.antibiotic_code: item for item in dataset.antibiotics}
    metadata = {item.event_id: item for item in dataset.enrichments}
    events = defaultdict(list)
    for item in dataset.events:
        events[item.culture_id].append(item)
    for culture in sorted(dataset.cultures, key=lambda item: (item.sample_date, item.culture_id)):
        isolates = events[culture.culture_id]
        codes = sorted({test.antibiotic_code for item in isolates for test in item.tests})
        yield CultureBundle(culture_event_id=culture.event_id, culture=culture,
            patient=patients[culture.patient_id], ward=wards[culture.ward_code],
            antibiotics=tuple(drugs[code] for code in codes),
            isolates=tuple(FeedIsolate(event=item, enrichment=metadata[item.event_id]) for item in isolates))


def batches(dataset, run_id, source_id, start, through, batch_size=100):
    if not 1 <= batch_size <= 100 or start > through:
        raise ValueError("Use a batch size of 1-100 and an ordered date range")
    by_day = defaultdict(list)
    for bundle in culture_bundles(dataset):
        if start <= bundle.culture.sample_date <= through:
            by_day[bundle.culture.sample_date].append(bundle)
    day = start
    while day <= through:
        items = by_day[day]
        for offset in range(0, max(1, len(items)), batch_size):
            # Identity does not depend on network retry timing. Changing batch size
            # changes delivery IDs, while publication still deduplicates event IDs.
            yield FeedBatch(run_id=run_id, source_id=source_id,
                delivery_id=f"dataset-{dataset.report.manifest.manifest_id}-{day}-size{batch_size}-{offset}",
                as_of=day, bundles=tuple(items[offset:offset + batch_size]))
        day += timedelta(days=1)


def send_batch(client, batch, *, wait_seconds=120):
    response = None
    for attempt in range(3):
        try:
            response = client.post("/api/v1/feed/batches", json=batch.model_dump(mode="json", by_alias=True))
            if response.status_code >= 500 and attempt < 2:
                time.sleep(0.5 * (attempt + 1))
                continue
            response.raise_for_status()
            break
        except httpx.TransportError:
            if attempt == 2:
                raise
            time.sleep(0.5 * (attempt + 1))
    receipt = response.json()
    deadline = time.monotonic() + wait_seconds
    while receipt["status"] == "pending":
        if time.monotonic() >= deadline:
            raise RuntimeError(f"Delivery remains queued; inspect {receipt['receipt_url']} or retry the same delivery")
        time.sleep(0.2)
        response = client.get(receipt["receipt_url"])
        response.raise_for_status()
        receipt = response.json()
    if receipt["status"] != "published":
        raise RuntimeError(f"Feed delivery {receipt['status']}: {receipt['error_code']}: {receipt['error_message']}")
    return receipt


def main():
    parser = argparse.ArgumentParser(description="Send the supplied dataset through the external feed HTTP contract")
    parser.add_argument("--source", required=True, help="Validated dataset ZIP or CSV directory")
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--run-id", required=True, help="Receiving run returned by POST /api/v1/feed/start")
    parser.add_argument("--source-id", default="dataset-simulator")
    parser.add_argument("--from-date", type=date.fromisoformat, required=True)
    parser.add_argument("--through", type=date.fromisoformat, required=True)
    parser.add_argument("--batch-size", type=int, default=100, choices=range(1, 101), metavar="1-100")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay after each published batch")
    args = parser.parse_args()
    if args.delay < 0 or not args.delay < float("inf") or args.from_date > args.through:
        parser.error("Use a finite nonnegative delay and an ordered date range")
    dataset = load_bundle(args.source)
    headers = {"Authorization": f"Bearer {os.environ['AEI_FEED_TOKEN']}"} if os.environ.get("AEI_FEED_TOKEN") else {}
    with httpx.Client(base_url=args.base_url.rstrip("/"), headers=headers, timeout=30, follow_redirects=False) as client:
        for batch in batches(dataset, args.run_id, args.source_id, args.from_date, args.through, args.batch_size):
            receipt = send_batch(client, batch)
            print(json.dumps({"receipt_id": receipt["receipt_id"], "status": receipt["status"],
                              "as_of": batch.as_of.isoformat(), "revision": receipt["published_revision"],
                              "cultures": len(batch.bundles)}), flush=True)
            time.sleep(args.delay)


if __name__ == "__main__":
    main()
