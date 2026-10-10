# ADR-001: Scope, datasets, and batch + streaming architecture

- **Status:** Accepted
- **Date:** 2026-10-07

## Context

ShopPulse is a portfolio flagship for Data Engineer / MLOps roles. It must demonstrate
multi-source ingestion, streaming and batch processing, dimensional modelling, ML deployment,
human-in-the-loop decisions, failure handling and monitoring, on a USD 100 Azure for Students
budget, built by one person in about 12 weeks.

No free, live public e-commerce data stream exists. Several domains were considered
(electricity market, Wikipedia edits, software supply chain, maritime AIS, chess). E-commerce
was chosen because it naturally exercises the patterns data engineering interviews test most:
dimensional modelling, slowly changing dimensions, sessionisation, late-arriving data and
business metrics.

## Decision

### Datasets
1. **REES46 eCommerce Behavior Data (multi-category store, Kaggle)** as the primary store.
   Real view/cart/remove/purchase events with product, category, brand, price, user and session.
   Its price changes over time give genuine SCD Type 2 history.
2. **OTTO Recommender Systems Dataset (official CC BY 4.0 release)** as a second store,
   added in the hardening phase. Different schema (JSON lines, anonymised items) exercises
   per-store schema normalisation into one canonical event contract.

The platform is framed as **one platform serving multiple independent stores**. Unrelated
datasets are never joined into a single fictional store.

### Making historical data "live"
- Split each dataset by time: earlier period = batch history; final period = streaming replay.
- Shift timestamps forward by a multiple of 7 days to preserve weekday seasonality.
- The replay service emits events in event-time order with configurable time compression and
  supports deterministic fault injection (duplicates, late events, schema changes, outages).

### Architecture
- **Streaming:** replay -> Kafka -> Spark Structured Streaming -> Delta (bronze/silver) ->
  real-time inference scaled by KEDA on consumer lag.
- **Batch:** daily catalogue/price files -> ADLS -> validation -> Delta -> dbt gold star schema
  -> forecasting, approvals, retraining, reconciliation (Airflow).
- **One storage layer:** Delta Lake on ADLS Gen2 for both paths.
- **One codebase:** the same Spark transformations run continuously and as backfill
  (`availableNow`).
- **Reconciliation:** nightly batch recompute vs streaming results; the gap is an alerted metric.

### Explicitly deferred
Microsoft Fabric, Strimzi (Kafka runs as a single-broker KRaft StatefulSet instead),
Debezium/CDC, Argo CD (GitHub Actions + Helm deploy instead), canary releases.

## Consequences

**Positive**
- Reproducible streaming: identical replays make fault injection and postmortems repeatable.
- Covers the most-tested data engineering patterns with real behavioural data.
- Fits the budget: local-first development, AKS only in bursts.

**Negative / accepted risks**
- The "live" data is replayed 2019-era history. Mitigation: state this plainly in the README;
  the differentiation comes from engineering, not data novelty.
- REES46 is widely used on Kaggle. Mitigation: same as above.
- Online evaluation is time-delayed offline evaluation, because outcomes are known in advance.
  Mitigation: describe it honestly as such.
- Single-broker Kafka is not highly available. Accepted for cost; documented as non-production HA.
- No CDC in the MVP. May be added later with Debezium.
