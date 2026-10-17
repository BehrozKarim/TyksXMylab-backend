import argparse
import json
import sys
import time
from datetime import date, timedelta
from itertools import groupby

from .api import DEFAULT_DB
from .dataset import DEFAULT_DATASET, load_dataset
from .models import AnalysisQuery, IsolateEvent
from .processor import ReferenceProcessor, compare_periods
from .store import SQLiteEventStore


def main():
    parser = argparse.ArgumentParser(description="Run the AEI reference pipeline before adding Flink")
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Local SQLite file")
    commands = parser.add_subparsers(dest="command", required=True)

    importer = commands.add_parser("import-dataset", help="Import the synthetic archive without extracting it")
    importer.add_argument("--zip", default=str(DEFAULT_DATASET))
    importer.add_argument("--through", type=date.fromisoformat)

    def add_filters(command):
        command.add_argument("--as-of", type=date.fromisoformat, required=True)
        command.add_argument("--ward", dest="ward_code")
        command.add_argument("--organism")
        command.add_argument("--specimen", choices=["urine", "blood", "wound"])
        command.add_argument("--antibiotic", dest="antibiotic_code")
        command.add_argument("--dedup-scope", choices=["selected_cohort", "all_wards"], default="selected_cohort")

    stats = commands.add_parser("stats", help="Compute a rolling or custom reporting period")
    add_filters(stats)
    stats.add_argument("--period", choices=["rolling_28_days", "rolling_12_months"])
    stats.add_argument("--start-date", type=date.fromisoformat)
    add_filters(commands.add_parser("compare", help="Return the rolling 28-day and 12-month reports together"))

    replay = commands.add_parser("replay", help="Replay daily batches and recalculate, including empty days")
    replay.add_argument("--zip", default=str(DEFAULT_DATASET))
    replay.add_argument("--from-date", type=date.fromisoformat, required=True)
    replay.add_argument("--through", type=date.fromisoformat, required=True)
    replay.add_argument("--delay", type=float, default=0.2, help="Seconds between simulated days")
    commands.add_parser("schema", help="Print the versioned input contract for future processors")
    args = parser.parse_args()

    if args.command == "schema":
        print(json.dumps(IsolateEvent.model_json_schema(), indent=2))
        return

    store = SQLiteEventStore(args.db)
    processor = ReferenceProcessor()
    if args.command == "import-dataset":
        events = load_dataset(args.zip)
        if args.through:
            events = [event for event in events if event.sample_date <= args.through]
        inserted, duplicates = store.append_many(events)
        print(json.dumps({"inserted": inserted, "duplicates": duplicates, "total_events": store.count()}))
    elif args.command in ("stats", "compare"):
        query = AnalysisQuery(**{key: value for key, value in vars(args).items()
                                 if key in AnalysisQuery.model_fields and value is not None})
        result = (compare_periods(processor, store.events(), query) if args.command == "compare"
                  else processor.calculate(store.events(), query))
        print(result.model_dump_json(indent=2))
    elif args.command == "replay":
        if args.from_date > args.through or args.delay < 0:
            parser.error("Replay needs from-date <= through and a nonnegative delay")
        if store.count():
            parser.error("Replay requires an empty database; use --db with a new file")
        events = load_dataset(args.zip)
        store.append_many(event for event in events if event.sample_date < args.from_date)
        by_day = {day: list(batch) for day, batch in groupby(events, key=lambda event: event.sample_date)}
        day = args.from_date
        while day <= args.through:
            inserted, _ = store.append_many(by_day.get(day, []))
            result = processor.calculate(store.events(), AnalysisQuery(as_of=day))
            print(json.dumps({"as_of": day.isoformat(), "new_isolates": inserted,
                              "history_events": store.count(), "groups": len(result.rows),
                              "groups_with_percentages": sum(row.status == "available" for row in result.rows)}),
                  flush=True)
            if day < args.through and args.delay:
                time.sleep(args.delay)
            day += timedelta(days=1)


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
