"""Monitoring persists inside the caller's publication transaction and lock."""

from datetime import datetime, timezone
from uuid import uuid4

from psycopg import sql
from psycopg.types.json import Jsonb

from .alert_models import (
    AlertConfiguration, AlertContext, AlertList, AlertRecord, AlertTransition,
    Investigation, SavedAlertAnalysis,
)
from .alerts import advance_alert, calculate_alert
from .evidence_source import snapshot_from_connection
from .publication_models import ServiceError, SourceContext


def initialize_alert_tables(db):
    db.execute("""CREATE TABLE IF NOT EXISTS alert_configurations (
        run_id TEXT PRIMARY KEY REFERENCES replay_runs,
        payload JSONB NOT NULL
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS alert_batches (
        run_id TEXT NOT NULL REFERENCES replay_runs,
        state_revision BIGINT NOT NULL CHECK (state_revision >= 1),
        monitoring_as_of DATE NOT NULL,
        PRIMARY KEY (run_id, state_revision)
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS alert_evaluations (
        run_id TEXT NOT NULL,
        state_revision BIGINT NOT NULL,
        monitor_id TEXT NOT NULL,
        analysis_id TEXT NOT NULL UNIQUE REFERENCES saved_analyses,
        PRIMARY KEY (run_id, state_revision, monitor_id),
        FOREIGN KEY (run_id, state_revision) REFERENCES alert_batches
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS alerts (
        alert_id TEXT PRIMARY KEY,
        run_id TEXT NOT NULL REFERENCES replay_runs,
        monitor_id TEXT NOT NULL,
        cohort_hash TEXT NOT NULL,
        antibiotic_code TEXT NOT NULL,
        rule_version TEXT NOT NULL,
        payload JSONB NOT NULL,
        UNIQUE (run_id, cohort_hash, antibiotic_code, rule_version),
        UNIQUE (run_id, monitor_id)
    )""")
    db.execute("""CREATE TABLE IF NOT EXISTS alert_transitions (
        alert_id TEXT NOT NULL REFERENCES alerts,
        state_revision BIGINT NOT NULL CHECK (state_revision >= 1),
        payload JSONB NOT NULL,
        PRIMARY KEY (alert_id, state_revision)
    )""")
    for table in ("alert_configurations", "alert_batches", "alert_evaluations", "alert_transitions"):
        db.execute(sql.SQL("""CREATE OR REPLACE TRIGGER forbid_alert_history_mutation
            BEFORE UPDATE OR DELETE OR TRUNCATE ON {}
            FOR EACH STATEMENT EXECUTE FUNCTION reject_event_mutation()""").format(sql.Identifier(table)))


class AlertEngine:
    def __init__(self, configuration):
        self.configuration = configuration

    def configuration_for(self, db, run_id):
        row = db.execute("SELECT payload FROM alert_configurations WHERE run_id=%s", (run_id,)).fetchone()
        return AlertConfiguration.model_validate(row[0]) if row else None

    def ensure(self, db, state):
        if self.configuration_for(db, state.run_id) is None:
            # Adopt an existing run at its present date; do not invent past alerts.
            db.execute("INSERT INTO alert_configurations VALUES (%s, %s)",
                       (state.run_id, Jsonb(self.configuration.model_dump(mode="json"))))
            self.evaluate(db, state)

    def report(self, db, state, monitor, configuration, snapshot=None):
        row = db.execute("""SELECT payload FROM saved_analyses WHERE run_id=%s AND state_revision=%s
            AND kind='alert' AND method_version='alert-v1' AND query_key=%s""",
            (state.run_id, state.state_revision, monitor.monitor_id)).fetchone()
        if row:
            return SavedAlertAnalysis.model_validate(row[0])
        snapshot = snapshot or snapshot_from_connection(db, state.run_id)
        context = AlertContext(**state.model_dump(include=set(SourceContext.model_fields)),
            source_files=snapshot.manifest.files, importer_version=snapshot.manifest.importer_version,
            generated_at=datetime.now(timezone.utc), monitoring_as_of=state.demo_date,
            monitor_id=monitor.monitor_id, cohort_hash=monitor.cohort_hash, monitor=monitor,
            configuration_hash=configuration.configuration_hash, rule_version=configuration.rule_version,
            minimum_tested=configuration.minimum_tested, threshold_pp=configuration.threshold_pp,
            confirmation=configuration.confirmation)
        report = SavedAlertAnalysis(analysis_id=str(uuid4()), context=context,
            evaluation=calculate_alert(snapshot, monitor, configuration, state.demo_date))
        db.execute("""INSERT INTO saved_analyses
            (analysis_id, run_id, state_revision, method_version, kind, query_key, payload)
            VALUES (%s, %s, %s, 'alert-v1', 'alert', %s, %s)""",
            (report.analysis_id, state.run_id, state.state_revision, monitor.monitor_id,
             Jsonb(report.model_dump(mode="json"))))
        return report

    def evaluate(self, db, state):
        configuration = self.configuration_for(db, state.run_id)
        if configuration is None:
            self.ensure(db, state)
            return
        inserted = db.execute("""INSERT INTO alert_batches VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING""", (state.run_id, state.state_revision, state.demo_date)).rowcount
        if not inserted:
            return
        snapshot = snapshot_from_connection(db, state.run_id) if configuration.monitors else None
        for monitor in configuration.monitors:
            report = self.report(db, state, monitor, configuration, snapshot)
            db.execute("INSERT INTO alert_evaluations VALUES (%s, %s, %s, %s)",
                       (state.run_id, state.state_revision, monitor.monitor_id, report.analysis_id))
            row = db.execute("SELECT payload FROM alerts WHERE run_id=%s AND monitor_id=%s",
                             (state.run_id, monitor.monitor_id)).fetchone()
            previous = AlertRecord.model_validate(row[0]) if row else None
            alert = advance_alert(previous, report)
            if alert is None:
                continue
            db.execute("""INSERT INTO alerts
                (alert_id, run_id, monitor_id, cohort_hash, antibiotic_code, rule_version, payload)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (alert_id) DO UPDATE SET payload=EXCLUDED.payload""",
                (alert.alert_id, alert.run_id, alert.monitor_id, alert.cohort_hash,
                 alert.antibiotic_code, alert.rule_version, Jsonb(alert.model_dump(mode="json"))))
            if previous is None or previous.status != alert.status:
                transition = AlertTransition(state_revision=state.state_revision, monitoring_as_of=state.demo_date,
                    previous_status=previous.status if previous else None, status=alert.status,
                    analysis_id=report.analysis_id)
                db.execute("INSERT INTO alert_transitions VALUES (%s, %s, %s)",
                           (alert.alert_id, state.state_revision, Jsonb(transition.model_dump(mode="json"))))

    def list(self, db, state, request):
        configuration = self.configuration_for(db, state.run_id)
        revision, as_of = db.execute("""SELECT state_revision, monitoring_as_of FROM alert_batches
            WHERE run_id=%s ORDER BY state_revision DESC LIMIT 1""", (state.run_id,)).fetchone()
        monitors = tuple(SavedAlertAnalysis.model_validate(row[0]) for row in db.execute("""
            SELECT a.payload FROM alert_evaluations e JOIN saved_analyses a USING (analysis_id)
            WHERE e.run_id=%s AND e.state_revision=%s ORDER BY e.monitor_id""", (state.run_id, revision)))
        alerts = tuple(AlertRecord.model_validate(row[0]) for row in db.execute("""
            SELECT payload FROM alerts WHERE run_id=%s AND payload->>'status'=ANY(%s)
            ORDER BY monitor_id""", (state.run_id, list(request.states))))
        return AlertList(run_id=state.run_id, state_revision=state.state_revision,
            monitoring_revision=revision, monitoring_as_of=as_of, configuration=configuration,
            configuration_hash=configuration.configuration_hash, alerts=alerts, monitors=monitors)

    def investigation(self, db, state, alert_id):
        row = db.execute("SELECT payload FROM alerts WHERE alert_id=%s", (alert_id,)).fetchone()
        if row is None:
            raise ServiceError(404, "investigation_not_found", "Alert investigation not found")
        alert = AlertRecord.model_validate(row[0])
        original = SavedAlertAnalysis.model_validate(db.execute(
            "SELECT payload FROM saved_analyses WHERE analysis_id=%s", (alert.original_analysis_id,)).fetchone()[0])
        current = None
        if alert.run_id == state.run_id:
            configuration = self.configuration_for(db, state.run_id)
            current = self.report(db, state, original.context.monitor, configuration)
        transitions = tuple(AlertTransition.model_validate(row[0]) for row in db.execute(
            "SELECT payload FROM alert_transitions WHERE alert_id=%s ORDER BY state_revision", (alert_id,)))
        return Investigation(alert=alert, original_detection=original, current_analysis=current,
            current_analysis_status="available" if current else "archived_run", transitions=transitions)
