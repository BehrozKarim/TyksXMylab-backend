"""Read only published specimens; resolve revisions before patient/date filtering."""

from datetime import timedelta

from .analysis_models import HistoryCulture, HistoryIsolate, PatientMatch
from .dataset_models import CultureRecord, IsolateEnrichment
from .models import AnalysisQuery, IsolateEvent


LATEST_CULTURES = """WITH latest_cultures AS (
    SELECT DISTINCT ON (c.culture_id) c.* FROM culture_events c
    JOIN run_cultures r USING (event_id) WHERE r.run_id=%s
    ORDER BY c.culture_id, c.revision DESC
) """


def history_start(as_of):
    # History is [prior anniversary, cutoff), unlike inclusive surveillance reports.
    return AnalysisQuery(as_of=as_of, period="rolling_12_months").period_start - timedelta(days=1)


def search_patients(db, run_id, search, as_of, limit):
    rows = db.execute(LATEST_CULTURES + """SELECT patient_id, MAX(sample_date), COUNT(*)
        FROM latest_cultures WHERE sample_date <= %s AND strpos(patient_id, %s) > 0
        GROUP BY patient_id ORDER BY patient_id COLLATE "C" LIMIT %s""",
        (run_id, as_of, search, limit + 1)).fetchall()
    return tuple(PatientMatch(patient_id=row[0], latest_sample_date=row[1], visible_cultures=row[2])
                 for row in rows[:limit]), len(rows) > limit


def patient_visible(db, run_id, patient_id, as_of):
    return db.execute(LATEST_CULTURES + """SELECT 1 FROM latest_cultures
        WHERE patient_id=%s AND sample_date <= %s LIMIT 1""", (run_id, patient_id, as_of)).fetchone() is not None


def history_page(db, run_id, query, before=None):
    start = history_start(query.as_of)
    clause = ""
    parameters = [run_id, query.patient_id, start, query.as_of]
    if before is not None:
        clause = ' AND (sample_date, culture_id COLLATE "C") < (%s, %s)'
        parameters.extend(before)
    parameters.append(query.limit + 1)
    rows = db.execute(LATEST_CULTURES + """SELECT event_id, revision, payload FROM latest_cultures
        WHERE patient_id=%s AND sample_date >= %s AND sample_date < %s""" + clause +
        ' ORDER BY sample_date DESC, culture_id COLLATE "C" DESC LIMIT %s', parameters).fetchall()
    selected = [(row[0], row[1], CultureRecord.model_validate(row[2])) for row in rows[:query.limit]]
    isolates = {item.culture_id: [] for _, _, item in selected}
    if selected:
        # Rank all published isolate revisions first. A correction that moved out of
        # this patient, date range or culture must not resurrect its older revision.
        events = db.execute("""WITH latest_events AS (
            SELECT DISTINCT ON (e.isolate_id) e.* FROM isolate_events e
            JOIN run_isolates r USING (event_id) WHERE r.run_id=%s
            ORDER BY e.isolate_id, e.revision DESC
        ) SELECT e.payload, m.payload FROM latest_events e
          LEFT JOIN isolate_enrichments m USING (event_id)
          WHERE e.patient_id=%s AND e.sample_date >= %s AND e.sample_date < %s
            AND e.payload->>'culture_id'=ANY(%s)
          ORDER BY e.isolate_id COLLATE "C"
          """,
          (run_id, query.patient_id, start, query.as_of, list(isolates))).fetchall()
        for event_payload, metadata in events:
            event = IsolateEvent.model_validate(event_payload)
            isolates[event.culture_id].append(HistoryIsolate(event=event,
                enrichment=IsolateEnrichment.model_validate(metadata) if metadata else None))
    return tuple(HistoryCulture(event_id=event_id, revision=revision, record=record,
        isolates=tuple(isolates[record.culture_id])) for event_id, revision, record in selected), len(rows) > query.limit
