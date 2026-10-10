# Failure-mode table (living document)

Every component gets rows here **before** it is built. Each automatic response must have a test.
Each human step must have a runbook in `docs/runbooks/`.

| # | Component | Failure | Detection | Automatic response | Human step / runbook | Test |
|---|---|---|---|---|---|---|
| 1 | Replay service | Crash mid-replay | Pod restart; producer heartbeat gap | Resume from the checkpoint (saved only after Kafka acks; never passes a buffered late event); re-sent events deduped by `event_id` in silver | Page if down > 10 min | `test_crash_and_resume_loses_nothing`, `test_checkpoint_never_passes_a_late_event_still_buffered` |
| 2 | Kafka broker | Broker unavailable | Producer delivery errors; consumer lag metric stalls | Producer retries inside a bounded queue (back-pressure on `BufferError`); after `delivery.timeout.ms` the replay exits non-zero without moving its checkpoint; restart resumes | `runbooks/kafka-down.md` | `test_checkpoint_does_not_move_when_kafka_rejects_messages`, `test_full_local_queue_applies_back_pressure_then_sends` |
| 3 | Ingestion | Malformed / schema-breaking event | Schema registry validation failure | Route to DLQ topic; pipeline continues | Triage DLQ; fix; replay via tool | TODO |
| 4 | Ingestion | Duplicate events | Dedup on event ID in silver | Dropped idempotently (Delta MERGE) | None; tracked as metric | TODO |
| 5 | Spark streaming | Late events beyond watermark | Late-events table row count | Written to late-events table; merged by nightly batch | Investigate if late rate > threshold | TODO |
| 6 | Spark streaming | Job crash | Query progress stops; freshness SLO breach | Restart from checkpoint; exactly-once sink via Delta | `runbooks/stream-job-down.md` | TODO |
| 7 | Batch (Airflow) | Daily catalogue file missing or late | Sensor timeout; SLA miss | Retry; run with yesterday's dimension, flagged stale | Chase / backfill when file arrives | TODO |
| 8 | Batch | File fails validation | Data quality checks fail | Quarantine file; downstream tasks skipped | Quarantine triage queue | TODO |
| 9 | Reconciliation | Streaming vs batch metrics diverge | Nightly gap > threshold | Alert; mark affected day as under review | Root-cause; postmortem if real loss | TODO |
| 10 | Inference | Service slow or down | p99 latency, error rate SLOs | Fallback: previous model version -> heuristic | Investigate; rollback | TODO |
| 11 | Inference | Feature unavailable | Missing-feature counter | Default value + flag; heuristic if critical | Fix upstream | TODO |
| 12 | Scaling | Consumer lag spike | Kafka lag metric | KEDA scales consumers out | Check if lag persists at max replicas | TODO |
| 13 | Pretrained forecaster (ADR-006) | Timeout / OOM / service down | Error rate, circuit breaker state | Circuit opens; LightGBM forecast used | Review if open > 1 day | TODO |
| 14 | Model quality | Live performance degrades | Rolling metric vs threshold; drift metrics | Trigger retraining DAG; block auto-promotion | Approve or reject candidate | TODO |
| 15 | Human review | Approval queue backlog | Queue size / age SLO | Escalation alert; items past expiry follow safe default (no reorder) | Clear backlog | TODO |
| 16 | Infrastructure | Azure budget threshold hit | Cost Management alert | Scheduled `az aks stop` | Review spend; adjust schedule | TODO |
| 17 | Data prep | Malformed / invalid raw rows | `reject_reason` per row; counts in `_manifest.json` | Written to `status=rejected` with the raw record; valid rows continue | Review reject counts by reason | `test_each_bad_row_is_rejected_with_its_reason` |
| 18 | Data prep | Raw input file missing | Spark `AnalysisException` at read | Job fails fast before writing anything | Re-download / fix path, rerun (idempotent) | `test_missing_input_file_fails_fast` |
| 19 | Data prep | Exact duplicate rows in source | `exact_duplicate_rows` in split manifest | Same `event_id`; collapsed by silver dedup (#4) | Record rate in `docs/findings.md` | `test_exact_duplicates_share_an_event_id` |
| 20 | Data prep | Machine time zone leaks into timestamps | Tests compare inside Spark | Session time zone pinned to UTC; no Python-side timestamp maths | None | `test_shift_is_whole_weeks_and_keeps_weekday` |
| 21 | Schema registry | Registry down or not yet primary | `RegistryError` at replay start; `/_health` readiness check | Replay looks up the schema ID once at start, so a running replay is unaffected; a starting one fails fast with exit 1. `make up-stream` waits for primary | `runbooks/registry-down.md` | `test_unreachable_registry_fails_fast` |
| 22 | Contract | Producer builds an invalid event (naive time, unknown type, float price) | `to_record` raises before Kafka | Event rejected at the producer, counted | Fix producer / source data | `test_invalid_rows_are_rejected_before_kafka` |
| 23 | Spark (local) | Scratch space on RAM-backed tmpfs exhausts memory | `IOException: Cannot allocate memory`; job aborts | Job fails without publishing a manifest; rerun overwrites cleanly | Set `SPARK_LOCAL_DIRS` to disk ([postmortem](postmortems/2026-10-10-prep-sample-oom.md)) | TODO (preflight check) |
| 24 | Kafka broker | Consumer lag exceeds retention (silent data loss) | Lag (in time) vs `retention.ms` (7 d) | None automatic: alert before lag reaches retention | Scale consumers / pause replay; backfill from prepared Parquet | TODO |
| 25 | Kafka broker | Broker disk full | Disk usage | `retention.bytes` 1 GiB per partition caps the topic at ~6 GiB; DLQ capped at 1 GiB | Raise cap or volume | TODO |
| 26 | Replay / Kafka | Event time in the future rejected by broker (Kafka 4 `message.timestamp.after.max.ms`) | Producer errors | Prevented by design: Kafka timestamp = send time; event time travels in the payload | None | `test_chaos_replay_round_trips_through_kafka` (integration) |
| 27 | Local dev (WSL2) | VM idle-stopped, containers killed | Containers `Exited (143)` | `restart: unless-stopped`; `.wslconfig` idle timeouts disabled | `make up-stream` | None (environment) |

## Review notes (2026-10-07) — to resolve as each component is designed

- **#1:** Resolved by the replay design: checkpoint after ack + deterministic event IDs + silver dedup.
- **#3:** State where validation happens (producer serializer, Spark deserialisation, or both).
  The DLQ "replay tool" needs a home (replay or review-queue service).
- **#6:** Exactly-once holds for a plain Delta sink; `foreachBatch` + MERGE needs an idempotent
  MERGE or `txnAppId`/`txnVersion`.
- **#7:** Late catalogue backfill must use the real price effective date for SCD2, not arrival date.
- **#10:** Inference is a Kafka consumer: "down" means delayed, not degraded. Clarify p99 definition.
- **#12:** Max consumer replicas = topic partition count.
- **#16:** Cost data lags by hours; split into (a) scheduled stop and (b) alert-triggered stop
  (which needs an action group + automation).
- **Missing rows:** Kafka retention exceeded by lag (silent loss), Kafka disk full, schema
  registry down, storage throttling/outage, corrupt/incompatible checkpoint, MLflow/Postgres
  down, Key Vault / OIDC failure, reconciliation job silently not running, Airflow scheduler
  down, dbt test failure, Delta small-file growth.
- **Thresholds needed:** #5, #9, #14, #15.
