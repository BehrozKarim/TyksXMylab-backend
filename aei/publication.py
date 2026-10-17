"""Atomic replay publication and reproducible evidence; no source history is deleted."""

import csv
import hashlib
import io
import json
import logging
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

from psycopg.types.json import Jsonb

from .analysis_models import (
    ComparisonContext, ComparisonRequest, HistoryContext, HistoryRequest, PatientSearchRequest,
    PatientSearchResult, SavedAnalysis, SavedComparison, SavedHistory, SourceContext,
)
from .comparisons import calculate_comparison
from .evidence import calculate_evidence
from .evidence_source import EvidenceUnavailable, snapshot_from_connection
from .history import history_page, history_start, patient_visible, search_patients
from .models import AnalysisQuery
from .publication_models import (
    EvidenceRequest, ExpectedState, PublishedContext, ReplayReset, ReplayStart,
    ReplayState, SavedEvidence, ServiceError,
)
from .publication_store import publication_lock


logger = logging.getLogger(__name__)


class PublicationService:
    def __init__(self, store):
        self.store = store

    @contextmanager
    def locked(self, *, wait=True):
        with self.store.connection() as db:
            if wait:
                publication_lock(db, self.store.schema)
            elif not db.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s))",
                                (f"aei-publication:{self.store.schema}",)).fetchone()[0]:
                raise ServiceError(409, "publication_busy", "Another publication or analysis is in progress; retry")
            yield db

    def _source(self, db):
        row = db.execute("SELECT payload, source_kind FROM dataset_manifests").fetchone()
        if row is None:
            raise EvidenceUnavailable("Import a validated dataset before using published evidence or replay")
        return row

    def _active(self, db, *, initialize=True):
        row = db.execute("""SELECT r.payload FROM replay_runs r
            JOIN active_run a USING (run_id)""").fetchone()
        if row:
            return ReplayState.model_validate(row[0])
        if not initialize:
            return None
        manifest, kind = self._source(db)
        end = db.execute("SELECT MAX(sample_date) FROM culture_events").fetchone()[0]
        if end is None:
            raise EvidenceUnavailable("Import at least one complete culture before creating a run")
        # First use exposes the already imported complete bundles; later imports
        # never silently change this run. Reset explicitly publishes a new source view.
        return self._create_run(db, manifest, kind, end, end)

    def _create_run(self, db, manifest, kind, seed, through):
        AnalysisQuery(as_of=seed, period="rolling_12_months")
        now = datetime.now(timezone.utc)
        state = ReplayState(run_id=str(uuid4()), state_revision=1,
            status="completed" if seed == through else "paused", demo_date=seed,
            seed_through=seed, replay_through=through, latest_visible_sample_date=None,
            published_cultures=0, published_isolates=0,
            source_manifest_id=manifest["manifest_id"], source_kind=kind,
            created_at=now, updated_at=now)
        db.execute("INSERT INTO replay_runs (run_id, manifest_id, payload) VALUES (%s, %s, %s)",
                   (state.run_id, state.source_manifest_id, Jsonb(state.model_dump(mode="json"))))
        state = self._publish(db, state, seed)
        self._save(db, state)
        db.execute("""INSERT INTO active_run (singleton, run_id) VALUES (TRUE, %s)
            ON CONFLICT (singleton) DO UPDATE SET run_id=EXCLUDED.run_id""", (state.run_id,))
        return state

    def _publish(self, db, state, through, from_date=date.min):
        # All cultures, including negative and mixed-growth samples, are published.
        cultures = db.execute("""INSERT INTO run_cultures (run_id, event_id, published_revision)
            SELECT %s, event_id, %s FROM culture_events WHERE sample_date BETWEEN %s AND %s
            ON CONFLICT DO NOTHING""", (state.run_id, state.state_revision, from_date, through)).rowcount
        isolates = db.execute("""INSERT INTO run_isolates (run_id, event_id, published_revision)
            SELECT %s, e.event_id, %s FROM isolate_enrichments e
            JOIN run_cultures c ON c.event_id=e.culture_event_id AND c.run_id=%s
            WHERE c.published_revision=%s
            ON CONFLICT DO NOTHING""", (state.run_id, state.state_revision, state.run_id, state.state_revision)).rowcount
        latest = db.execute("""SELECT MAX(c.sample_date) FROM culture_events c
            JOIN run_cultures r USING (event_id) WHERE r.run_id=%s""", (state.run_id,)).fetchone()[0]
        return state.model_copy(update={"demo_date": through, "latest_visible_sample_date": latest,
            "published_cultures": state.published_cultures + cultures,
            "published_isolates": state.published_isolates + isolates})

    def _save(self, db, state, next_tick=None):
        db.execute("UPDATE replay_runs SET payload=%s, next_tick_at=%s WHERE run_id=%s",
                   (Jsonb(state.model_dump(mode="json")), next_tick, state.run_id))

    def _check(self, state, expected):
        if expected.expected_run_id is not None and (
                expected.expected_run_id != state.run_id or expected.expected_revision != state.state_revision):
            raise ServiceError(409, "stale_state", "The published run changed; refresh state and retry",
                               {"state": state.model_dump(mode="json")})

    def state(self):
        with self.locked() as db:
            return self._active(db)

    def reset(self, request: ReplayReset):
        with self.locked() as db:
            previous = self._active(db)
            self._check(previous, request)
            manifest, kind = self._source(db)
            count = db.execute("SELECT COUNT(*) FROM culture_events").fetchone()[0]
            enriched = db.execute("SELECT COUNT(*) FROM isolate_enrichments").fetchone()[0]
            if count != manifest["files"]["cultures.csv"]["rows"] or enriched != manifest["files"]["isolates.csv"]["rows"]:
                raise ServiceError(409, "incomplete_source", "Import the complete source before resetting replay")
            start, end = date.fromisoformat(manifest["source_start"]), date.fromisoformat(manifest["source_end"])
            through = request.replay_through or end
            seed = request.seed_through or max(start - timedelta(days=1), through - timedelta(days=28))
            if not start <= through <= end or not start - timedelta(days=1) <= seed <= through:
                raise ServiceError(422, "invalid_replay_range",
                    "Replay must end within the source dates; seed must be between source start minus one day and replay end")
            if previous.status == "running":
                self._save(db, previous.model_copy(update={"status": "paused",
                    "state_revision": previous.state_revision + 1, "updated_at": datetime.now(timezone.utc)}))
            return self._create_run(db, manifest, kind, seed, through)

    def start(self, request: ReplayStart):
        with self.locked() as db:
            state = self._active(db)
            self._check(state, request)
            if state.status == "running":
                return state  # Repeated start never creates a second schedule.
            if state.status != "paused":
                raise ServiceError(409, "replay_not_paused", "Reset a completed or failed run before starting")
            state = state.model_copy(update={"status": "running", "interval_seconds": request.interval_seconds,
                "state_revision": state.state_revision + 1, "updated_at": datetime.now(timezone.utc)})
            self._save(db, state, datetime.now(timezone.utc) + timedelta(seconds=request.interval_seconds))
            return state

    def pause(self, request: ExpectedState):
        # Wait for any in-flight publication to commit before acknowledging pause.
        with self.locked() as db:
            state = self._active(db)
            self._check(state, request)
            if state.status == "running":
                state = state.model_copy(update={"status": "paused", "state_revision": state.state_revision + 1,
                                                 "updated_at": datetime.now(timezone.utc)})
                self._save(db, state)
            return state

    def _advance(self, db, state):
        state = state.model_copy(update={"state_revision": state.state_revision + 1})
        day = state.demo_date + timedelta(days=1)
        state = self._publish(db, state, day, day)
        complete = state.demo_date == state.replay_through
        state = state.model_copy(update={"status": "completed" if complete else state.status,
            "updated_at": datetime.now(timezone.utc)})
        due = (datetime.now(timezone.utc) + timedelta(seconds=state.interval_seconds)
               if state.status == "running" else None)
        self._save(db, state, due)
        return state

    def step(self, request: ExpectedState):
        with self.locked(wait=False) as db:
            state = self._active(db)
            self._check(state, request)
            if state.status != "paused":
                raise ServiceError(409, "replay_not_paused", "A single step requires a paused run")
            return self._advance(db, state)

    def worker_tick(self):
        """A persisted due time and database lock allow only one worker per tick."""
        attempted = None
        try:
            # Idle polling must not contend with a manual step in a paused run.
            # Recheck under the lock before making any publication decision.
            with self.store.connection() as db:
                observed = self._active(db, initialize=False)
            if observed is None or observed.status != "running":
                return
            with self.locked() as db:
                state = self._active(db, initialize=False)
                if state is None or state.status != "running":
                    return
                due = db.execute("SELECT next_tick_at FROM replay_runs WHERE run_id=%s", (state.run_id,)).fetchone()[0]
                if due is not None and due <= datetime.now(timezone.utc):
                    attempted = state
                    self._advance(db, state)
        except Exception:
            logger.exception("Replay publication failed")
            # The failed transaction rolled back membership AND clock changes.
            if attempted is not None:
                with self.locked() as db:
                    current = self._active(db, initialize=False)
                    if (current and current.run_id == attempted.run_id
                            and current.state_revision == attempted.state_revision and current.status == "running"):
                        self._save(db, current.model_copy(update={"status": "error",
                            "state_revision": current.state_revision + 1, "updated_at": datetime.now(timezone.utc),
                            "last_error": "Publication failed; inspect server logs and reset after correcting the cause"}))

    def evidence(self, request: EvidenceRequest):
        with self.locked() as db:
            state = self._active(db)
            self._check(state, request)
            query = request.resolved(state.demo_date)
            self._as_of(state, query.as_of)
            key, cached = self._cached(db, state, query, "evidence-v1", "evidence", SavedEvidence)
            if cached:
                return cached
            snapshot = snapshot_from_connection(db, state.run_id)
            report = calculate_evidence(snapshot, query)
            context = PublishedContext(**report.context.model_dump(), run_id=state.run_id,
                state_revision=state.state_revision, demo_date=state.demo_date,
                published_cultures=state.published_cultures, published_isolates=state.published_isolates,
                latest_visible_sample_date=state.latest_visible_sample_date, source_kind=state.source_kind,
                source_files=snapshot.manifest.files, importer_version=snapshot.manifest.importer_version)
            saved = SavedEvidence(analysis_id=str(uuid4()), context=context, windows=report.windows)
            return self._persist(db, saved, "evidence", key)

    def _as_of(self, state, value):
        as_of = value or state.demo_date
        if as_of > state.demo_date:
            raise ServiceError(422, "future_as_of", "as_of cannot be later than the published demo date",
                               {"demo_date": state.demo_date.isoformat()})
        return as_of

    def _cached(self, db, state, query, method, kind, model):
        values = query.model_dump(mode="json")
        if getattr(query, "known_at", None) is not None:
            values["known_at"] = query.known_at.astimezone(timezone.utc).isoformat()
        key = hashlib.sha256(json.dumps(values, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        row = db.execute("""SELECT payload FROM saved_analyses WHERE run_id=%s AND state_revision=%s
            AND method_version=%s AND kind=%s AND query_key=%s""",
            (state.run_id, state.state_revision, method, kind, key)).fetchone()
        return key, model.model_validate(row[0]) if row else None

    def _persist(self, db, saved, kind, key):
        db.execute("""INSERT INTO saved_analyses
            (analysis_id, run_id, state_revision, method_version, kind, query_key, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s)""",
            (saved.analysis_id, saved.context.run_id, saved.context.state_revision, saved.context.method_version,
             kind, key, Jsonb(saved.model_dump(mode="json"))))
        return saved

    def _source_context(self, db, state):
        manifest, _ = self._source(db)
        return {**state.model_dump(include=set(SourceContext.model_fields)),
                "source_files": manifest["files"], "importer_version": manifest["importer_version"],
                "generated_at": datetime.now(timezone.utc)}

    def comparison(self, request: ComparisonRequest):
        with self.locked() as db:
            state = self._active(db)
            self._check(state, request)
            query = request.resolved(state.demo_date)
            self._as_of(state, query.as_of)
            key, cached = self._cached(db, state, query, "comparison-v1", "comparison", SavedComparison)
            if cached:
                return cached
            row, latest_received = calculate_comparison(snapshot_from_connection(db, state.run_id), query)
            context = ComparisonContext(**self._source_context(db, state), filters=query,
                ward_scope="selected_ward" if query.ward_code else "all_wards", latest_received_at=latest_received)
            return self._persist(db, SavedComparison(analysis_id=str(uuid4()), context=context, comparison=row),
                                 "comparison", key)

    def patients(self, request: PatientSearchRequest):
        with self.locked() as db:
            state = self._active(db)
            self._check(state, request)
            as_of = self._as_of(state, request.as_of)
            patients, more = search_patients(db, state.run_id, request.search, as_of, request.limit)
            return PatientSearchResult(run_id=state.run_id, state_revision=state.state_revision, as_of=as_of,
                search=request.search, limit=request.limit, has_more=more, patients=patients)

    def patient_history(self, patient_id: str, request: HistoryRequest):
        with self.locked() as db:
            state = self._active(db)
            self._check(state, request)
            query = request.resolved(patient_id, state.demo_date)
            before = None
            if query.cursor:
                try:
                    previous = self._read_analysis(db, str(query.cursor))
                except ServiceError as error:
                    raise ServiceError(422, "invalid_cursor", "History cursor not found") from error
                if not isinstance(previous, SavedHistory):
                    raise ServiceError(422, "invalid_cursor", "Cursor must refer to a history page")
                self._check(state, ExpectedState(expected_run_id=previous.context.run_id,
                                                expected_revision=previous.context.state_revision))
                old = previous.context.filters
                if (old.patient_id, old.as_of, old.limit) != (query.patient_id, query.as_of, query.limit):
                    raise ServiceError(422, "invalid_cursor", "Cursor belongs to different history filters")
                if not previous.has_more or not previous.cultures:
                    raise ServiceError(422, "invalid_cursor", "This history page has no next page")
                last = previous.cultures[-1].record
                before = last.sample_date, last.culture_id
            self._as_of(state, query.as_of)
            if not patient_visible(db, state.run_id, patient_id, query.as_of):
                raise ServiceError(404, "patient_not_found", "Patient not found")
            key, cached = self._cached(db, state, query, "history-v1", "history", SavedHistory)
            if cached:
                return cached
            cultures, more = history_page(db, state.run_id, query, before)
            context = HistoryContext(**self._source_context(db, state), filters=query,
                start_date=history_start(query.as_of), end_date_exclusive=query.as_of)
            analysis_id = str(uuid4())
            return self._persist(db, SavedHistory(analysis_id=analysis_id, context=context, cultures=cultures,
                has_more=more, next_cursor=analysis_id if more else None), "history", key)

    def analysis(self, analysis_id):
        with self.store.connection() as db:
            return self._read_analysis(db, analysis_id)

    def _read_analysis(self, db, analysis_id):
        row = db.execute("SELECT kind, payload FROM saved_analyses WHERE analysis_id=%s", (analysis_id,)).fetchone()
        if row is None:
            raise ServiceError(404, "analysis_not_found", "Saved analysis not found")
        model = {"evidence": SavedEvidence, "comparison": SavedComparison, "history": SavedHistory}[row[0]]
        return model.model_validate(row[1])

    def meta(self):
        with self.locked() as db:
            state = self._active(db)
            manifest, kind = self._source(db)
            wards = [row[0] for row in db.execute("SELECT payload FROM wards ORDER BY ward_code")]
            drugs = [row[0] for row in db.execute("SELECT payload FROM antibiotics ORDER BY antibiotic_code")]
            return {"state": state.model_dump(mode="json"), "method_version": "evidence-v1",
                "base_rules_version": "prototype-v2", "minimum_tested": 30,
                "periods": ["rolling_28_days", "rolling_12_months"],
                "defaults": {"as_of": state.demo_date.isoformat(), "dedup_scope": "selected_cohort"},
                "source": {"kind": kind, "manifest_id": manifest["manifest_id"],
                           "start_date": manifest["source_start"], "end_date": manifest["source_end"]},
                "wards": wards, "antibiotics": drugs,
                "organisms": sorted({panel["organism"] for panel in manifest["observed_panels"]}),
                "specimens": ["urine", "blood", "wound"], "sex": ["F", "M"],
                "dedup_scopes": ["selected_cohort", "all_wards"],
                "comparison_antibiotics": [drug["antibiotic_code"] for drug in drugs if not drug["is_screening"]],
                "analysis_methods": {"evidence": "evidence-v1", "comparison": "comparison-v1", "history": "history-v1"},
                "observed_panels": manifest["observed_panels"], "warnings": manifest.get("warnings", []),
                "limitations": ["Synthetic laboratory susceptibility; not treatment efficacy",
                    "Metadata and panel availability describe the full source, not future patient results",
                    "Only published complete source bundles enter evidence; legacy event writes remain outside replay",
                    "Same-day ordering, ward-transfer policy and untested-first-isolate policy await partner confirmation",
                    "Patient history shows collection dates and raw recorded AST, not treatment outcomes",
                    "Alerts, dashboard, external bundle publication and Flink are not implemented"]}


def export_csv(report: SavedAnalysis) -> str:
    """Flatten the frozen document, including provenance and suppression nulls."""
    def flatten(value, prefix=""):
        result = {}
        for key, item in value.items():
            name = f"{prefix}.{key}" if prefix else key
            if isinstance(item, dict):
                result.update(flatten(item, name))
            else:
                # Prevent source labels being interpreted as spreadsheet formulas.
                result[name] = "'" + item if isinstance(item, str) and item.startswith(("=", "+", "-", "@", "\t", "\r")) else item
        return result

    document = report.model_dump(mode="json")
    common = flatten({"analysis_id": document["analysis_id"], "context": document["context"]})
    if isinstance(report, SavedEvidence):
        rows = [{**common, **flatten(window)} for window in document["windows"].values()]
    elif isinstance(report, SavedComparison):
        rows = [{**common, "record_type": "paired_comparison", **flatten(document["comparison"])}]
    else:
        rows = []
        page = {"has_more": report.has_more, "next_cursor": report.next_cursor}
        for culture in document["cultures"]:
            base = {**common, **page, **flatten({"culture": {key: item for key, item in culture.items() if key != "isolates"}})}
            rows.append({**base, "record_type": "culture"})
            for isolate in culture["isolates"]:
                event = {key: item for key, item in isolate["event"].items() if key != "tests"}
                details = {**base, **flatten({"isolate": {"event": event, "enrichment": isolate["enrichment"]}})}
                rows.append({**details, "record_type": "isolate"})
                for test in isolate["event"]["tests"]:
                    rows.append({**details, "record_type": "ast", **flatten({"test": test})})
        if not rows:
            rows = [{**common, **page, "record_type": "history_summary", "culture_count": 0}]
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=list(dict.fromkeys(key for row in rows for key in row)))
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()
