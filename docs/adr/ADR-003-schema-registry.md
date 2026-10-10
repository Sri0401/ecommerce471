# ADR-003: Schema registry — Karapace

- **Status:** Accepted
- **Date:** 2026-10-07

## Context

Events on Kafka are Avro, validated against a schema registry (ADR-001). The project is
free-and-open-source only, which rules out Confluent Schema Registry (Confluent Community
License, source-available). The dev machine has 8 GB RAM (ADR-007).

## Options

| | Karapace (Aiven) | Apicurio Registry (Red Hat) |
|---|---|---|
| Licence | Apache 2.0 | Apache 2.0 |
| API | Confluent Schema Registry API, natively | Own API + Confluent-compatible endpoint (`/apis/ccompat/v7`) |
| Storage | A Kafka topic (`_schemas`), like Confluent | In-memory, SQL database, or Kafka (KafkaSQL) |
| Runtime | Python | Java (Quarkus) |
| Extras | Also a Kafka REST proxy | Web UI, more formats, artifact groups |
| Activity | Active (pushed 2026); also offered by Instaclustr | Active |

## Decision

**Karapace.**
- It is a drop-in for the Confluent API, so the standard `confluent-kafka` Avro serializers and
  Spark's Confluent wire-format handling work unchanged; no compatibility layer to debug.
- Schemas live in a Kafka topic: no extra database to run, back up or put in the failure table.
- Lighter on memory than a JVM service, which matters on the 8 GB machine.

## Consequences

- Compatibility mode for `shoppulse.events.v1-value` is set to **BACKWARD**: a new schema must
  be able to read data written with the previous one (add fields only with defaults).
- No UI; schemas are inspected with `curl` against the REST API. Acceptable.
- The registry depends on Kafka being up. Failure-mode row needed: *schema registry down* —
  producers cannot register or look up schemas; the serializer caches schema IDs it has
  already used, so a running producer keeps working for known schemas.
- If Karapace maintenance stalls, Apicurio's Confluent-compatible endpoint is the fallback;
  clients would only change the registry URL.
