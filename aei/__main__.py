import argparse
import json
import sys
import time
from datetime import date, timedelta
from pathlib import Path

from .config import database_url
from .dataset import DatasetValidationError, default_dataset, load_bundle
from .evidence import EvidenceQueryError
from .evidence_source import EvidenceUnavailable
from .migration import read_sqlite_history
from .models import AnalysisQuery, IsolateEvent
from .postgres import PostgresEventStore
from .processor import ReferenceProcessor, compare_periods
from .publication import PublicationService
from .publication_models import EvidenceRequest, ExpectedState, ReplayReset, ServiceError
from .store import EventConflict


def main():
    parser = argparse.ArgumentParser(description="Run the AEI reference pipeline before adding Flink")
    parser.add_argument("--database-url", default=database_url(), help="PostgreSQL URL (or AEI_DATABASE_URL)")
    parser.add_argument("--schema", default="aei", help="PostgreSQL schema; use a separate schema for replay")
    commands = parser.add_subparsers(dest="command", required=True)

    def add_source(command):
        sources = command.add_mutually_exclusive_group()
        sources.add_argument("--zip", type=Path, help="Synthetic ZIP archive")
        sources.add_argument("--dataset-dir", type=Path, help="Directory containing the six source CSVs")

    importer = commands.add_parser("import-dataset", help="Validate and atomically import source cultures and metadata")
    add_source(importer)
    importer.add_argument("--through", type=date.fromisoformat)
    importer.add_argument("--report", type=Path, help="Write the validation and import result as JSON")
    validator = commands.add_parser("validate-dataset", help="Validate source files without connecting to a database")
    add_source(validator)
    validator.add_argument("--report", type=Path, help="Write the validation report as JSON")

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

    evidence = commands.add_parser("evidence", help="Enriched evidence for one organism, specimen and antibiotic")
    evidence.add_argument("--as-of", type=date.fromisoformat, help="Defaults to the published demo date")
    evidence.add_argument("--organism", required=True)
    evidence.add_argument("--specimen", choices=["urine", "blood", "wound"], required=True)
    evidence.add_argument("--antibiotic", dest="antibiotic_code", required=True)
    evidence.add_argument("--ward", dest="ward_code")
    evidence.add_argument("--sex", choices=["F", "M"])
    evidence.add_argument("--age-min", type=int)
    evidence.add_argument("--age-max", type=int)
    evidence.add_argument("--known-at", help="ISO timestamp limiting which event revisions are known")
    evidence.add_argument("--dedup-scope", choices=["selected_cohort", "all_wards"], default="selected_cohort")

    commands.add_parser("init-db", help="Initialize PostgreSQL event history")
    commands.add_parser("state", help="Inspect the active publication run and demo clock")
    migration = commands.add_parser("migrate-sqlite", help="Copy legacy SQLite history into PostgreSQL")
    migration.add_argument("--sqlite", type=Path, required=True)

    replay = commands.add_parser("replay", help="Stage complete source bundles and publish daily, including empty days")
    add_source(replay)
    replay.add_argument("--from-date", type=date.fromisoformat, required=True)
    replay.add_argument("--through", type=date.fromisoformat, required=True)
    replay.add_argument("--delay", type=float, default=0.2, help="Seconds between simulated days")
    commands.add_parser("schema", help="Print the versioned input contract for future processors")
    args = parser.parse_args()

    if args.command == "schema":
        print(json.dumps(IsolateEvent.model_json_schema(), indent=2))
        return

    if args.command in ("import-dataset", "validate-dataset", "replay"):
        if args.dataset_dir is not None and not args.dataset_dir.is_dir():
            parser.error("--dataset-dir must name an existing directory")
        if args.zip is not None and not args.zip.is_file():
            parser.error("--zip must name an existing archive")
        source = args.zip or args.dataset_dir or default_dataset()

    if args.command in ("import-dataset", "validate-dataset"):
        def emit_report(value):
            output = json.dumps(value, indent=2, ensure_ascii=False)
            if args.report:
                args.report.parent.mkdir(parents=True, exist_ok=True)
                args.report.write_text(output + "\n", encoding="utf-8")
            print(output)

        try:
            bundle = load_bundle(source)
        except DatasetValidationError as error:
            emit_report(error.report.model_dump(mode="json"))
            raise SystemExit(2) from error
        if args.command == "validate-dataset":
            emit_report(bundle.report.model_dump(mode="json"))
        else:
            store = PostgresEventStore(args.database_url, schema=args.schema)
            try:
                result = store.import_dataset(bundle, args.through)
            except EventConflict as error:
                emit_report({"validation": bundle.report.model_dump(mode="json"),
                             "error": {"code": "import_conflict", "message": str(error)}})
                raise SystemExit(2) from error
            emit_report({**result, "validation": bundle.report.model_dump(mode="json")})
        return

    store = PostgresEventStore(args.database_url, schema=args.schema)
    processor = ReferenceProcessor()
    if args.command == "evidence":
        try:
            query = EvidenceRequest(**{key: value for key, value in vars(args).items()
                                       if key in EvidenceRequest.model_fields and value is not None})
            report = PublicationService(store).evidence(query)
        except (EvidenceQueryError, EvidenceUnavailable, ServiceError, ValueError) as error:
            parser.error(str(error))
        print(report.model_dump_json(indent=2))
    elif args.command == "state":
        try:
            print(PublicationService(store).state().model_dump_json(indent=2))
        except EvidenceUnavailable as error:
            parser.error(str(error))
    elif args.command == "init-db":
        print(json.dumps({"storage": store.backend, "schema": store.schema, "total_events": store.count()}))
    elif args.command == "migrate-sqlite":
        events = read_sqlite_history(args.sqlite)
        inserted, duplicates = store.append_many(events)
        print(json.dumps({"source_events": len(events), "inserted": inserted,
                          "duplicates": duplicates, "total_events": store.count()}))
    elif args.command in ("stats", "compare"):
        query = AnalysisQuery(**{key: value for key, value in vars(args).items()
                                 if key in AnalysisQuery.model_fields and value is not None})
        result = (compare_periods(processor, store.events(), query) if args.command == "compare"
                  else processor.calculate(store.events(), query))
        print(result.model_dump_json(indent=2))
    elif args.command == "replay":
        if args.from_date > args.through or args.delay < 0:
            parser.error("Replay needs from-date <= through and a nonnegative delay")
        try:
            bundle = load_bundle(source)
            manifest = bundle.report.manifest
            if (args.from_date < manifest.source_start or args.through > manifest.source_end):
                parser.error("Replay dates must be within the source's observed range")
            store.import_dataset(bundle)
            publication = PublicationService(store)
            state = publication.reset(ReplayReset(seed_through=args.from_date - timedelta(days=1),
                                                   replay_through=args.through))
            while state.status == "paused":
                previous = state
                state = publication.step(ExpectedState(expected_run_id=state.run_id,
                                                        expected_revision=state.state_revision))
                print(json.dumps({"state": state.model_dump(mode="json"),
                    "new_cultures": state.published_cultures - previous.published_cultures,
                    "new_isolates": state.published_isolates - previous.published_isolates}), flush=True)
                if state.status != "completed" and args.delay:
                    time.sleep(args.delay)
        except (DatasetValidationError, EventConflict, EvidenceUnavailable, ServiceError, ValueError) as error:
            parser.error(str(error))


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
