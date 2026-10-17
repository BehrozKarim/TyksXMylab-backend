# Reserved Flink integration boundary

There is intentionally no fake Flink job or placeholder running service here.
The first milestones establish event and output contracts, a PostgreSQL source,
and tested reference results that the real Flink job must reproduce.
External feed ingestion, durable receipts and SSE dashboard notifications are already
implemented in Python/PostgreSQL; they do not require Flink. See
[the feed contract](../FEED_INTEGRATION.md). Flink remains deferred.

## Stable contracts

- Input: `aei.models.IsolateEvent`, schema version 1. Generate JSON Schema with
  `python -m aei schema`. Corrections are full snapshots with higher revisions.
- Complete publication input: `aei.feed_models.FeedBatch`, including cultures,
  revision-bound metadata and negative specimens. Published run membership and the
  reporting clock determine visibility; a job must not consume staged future events
  as if they had already been published. Consume complete committed versions.
- Query/rules: `AnalysisQuery`; current output identifies `prototype-v2`. Annual
  means rolling 12 calendar months; the other standard period is rolling 28 days.
- Output: `AnalysisResult` and `Statistic`. Missing percentages stay null.
- Oracle: `ReferenceProcessor.calculate(events, query)` is independent of HTTP
  and storage. It defines expected results for a specified date and knowledge cutoff.
- Storage boundary: `EventStore` now defaults to `PostgresEventStore`. The source
  table is `aei.isolate_events`, with unique event/revision identities and append-only
  enforcement. SQLite is retained only for migration and unit tests.

The future API will read materialized Flink outputs; it will not invoke a Flink job
synchronously through the Python reference processor interface.

## Implementation sequence

1. PostgreSQL storage and Docker Compose are implemented. Logical WAL is enabled;
   a replication user, publication and slot still need to be configured for Flink.
2. Design CDC around committed complete-bundle publication and clock changes, including
   metadata and negative specimens; isolate events alone cannot reproduce enriched
   evidence or run visibility. Output tables must not feed back into source processing.
3. Implement revision handling and keyed first-isolate state per configured scope.
   Retain later candidates: an isolate that is not first today can become first
   when the old one expires. Keep annual and rolling selections independent.
4. Drive expiration explicitly with a reporting clock. Event-time watermarks alone
   must not leave a quiet ward's current rolling report frozen.
5. Write versioned results with idempotent identifiers and publish complete result
   versions before notifying readers. Keep late corrections distinguishable from
   earlier published results.
6. Enable durable checkpoints, configure PostgreSQL logical replication/WAL retention,
   and test restart/retry behavior. Pin compatible Flink, CDC and JDBC versions
   before deployment; no connector compatibility has been tested in this slice.
7. Compare Flink output against the reference processor for the same data, clock,
   rule version and filters, including failures and out-of-order corrections.
8. Reproduce the existing versioned alert evaluations, lifecycle and frozen reports
   before replacing the Python implementation. Current thresholds and monitor groups
   still need partner confirmation. Emit SSE notifications only after the complete
   result/alert version commits, using the existing durable notification log.

The existing synthetic source has day-level dates and no arrival times. Do not
silently reinterpret its replay timestamps as real result-reporting timestamps.
