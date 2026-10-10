# CLAUDE.md — ShopPulse

This file is read automatically by Claude Code at the start of every session.
It is the single source of truth for project scope and working rules. Keep it current.

## Project summary

ShopPulse is a production-grade, multi-store e-commerce data and ML platform. It ingests
behavioural and catalogue data in both **streaming** and **batch**, models it for analytics,
serves ML predictions in real time, keeps humans in key decision loops, and is designed to
keep working correctly when components fail.

It is a portfolio flagship project for Data Engineer / MLOps Engineer roles. The goal is not
the number of tools; it is **evidence of production judgement**: measured results,
documented trade-offs (ADRs), failure handling, postmortems and cost awareness.

The repository root is this folder (`flagshipProject/`).

## Owner context

- 2.5 years DevOps experience (TCS), Master of Data Science (University of Melbourne).
- Target roles: Data Engineer, Platform/DevOps engineer on data/ML teams, MLOps Engineer.

## Datasets

| Dataset | Role | Source / licence |
|---|---|---|
| REES46 eCommerce Behavior Data (multi-category store) | Store 1, primary | Kaggle; attribute REES46 |
| OTTO Recommender Systems Dataset | Store 2 (hardening phase, not MVP) | Official CC BY 4.0 release only (OTTO GitHub / OTTO's Kaggle dataset). NOT the Kaggle competition download |

Rules:
- Each dataset is split by time: earlier period = batch history; final period = replayed as "live".
- Timestamps are shifted forward by a **multiple of 7 days** so weekday seasonality is preserved.
- Models train **only on real behaviour data**, never on simulated/generated data.
- The README must state clearly that the "live" stream is replayed historical data.
- Development runs on a deterministic user-level sample; full data runs in bursts (ADR-009).

## Architecture (MVP)

Streaming path:
Replay service -> Kafka (single broker, KRaft, official `apache/kafka` image, own StatefulSet)
-> Spark Structured Streaming (sessionisation, stateful features, watermarks)
-> Delta Lake bronze/silver (local filesystem in dev, ADLS Gen2 in cloud)
-> feature consumer -> purchase-intent inference service (KEDA scales on consumer lag).

Batch path (Airflow, daily):
Catalogue/price files -> landing -> validation -> Delta -> dbt star schema (gold)
-> demand forecasting -> planner approval queue -> retraining -> stream-vs-batch reconciliation.

Unified principles:
- One storage layer (Delta). No separate speed-layer database. Storage root is configuration:
  a local path in dev, `abfss://` on ADLS in cloud. No code changes between the two.
- Same Spark transformation code for streaming and backfill (`trigger(availableNow=True)`).
- Nightly reconciliation: batch recompute of yesterday's metrics vs streaming results; gap is
  an alerted data-quality metric.
- Explicit late-data policy: watermark -> late-events table -> merged by nightly batch.

Gold layer (star schema):
- Facts: `fact_events`, `fact_sessions`, `fact_orders`
- Dimensions: `dim_product` (SCD Type 2 on real price changes), `dim_user`, `dim_date`,
  `dim_category`, `dim_store`

REST APIs (FastAPI, ADR-008):
- Review-queue service: planner approvals and quarantine triage.
- Inference service: `POST /predict` alongside the Kafka consumer; `/health`, `/ready`, `/metrics`.

## ML (see ADR-002, amended by ADR-006)

- Task A, demand forecasting (batch): seasonal naive baseline; LightGBM tuned with Optuna;
  open pretrained time-series model, self-hosted (ADR-006); optional ensemble.
  Same splits, same metrics.
- Task B, purchase intent (streaming): logistic regression baseline; LightGBM tuned with Optuna;
  optional GRU. Multi-objective tuning: PR-AUC vs p99 latency.
- MLflow: tracking, registry, champion/challenger aliases, shadow mode before promotion.
- Fallback chain: champion -> previous version -> heuristic. Pretrained model failure -> LightGBM.

## Human in the loop

- Planner approval queue for large/unusual reorder recommendations.
- Human approval for model promotion (GitHub Environment with required reviewers); every
  decision logged.
- Quarantine triage: inspect, fix-and-replay, or discard with a reason.

## Stack

**Free and open-source software only.** Every tool and library must have an OSI-approved
licence. Check the licence before proposing anything (e.g. Terraform is BSL -> use OpenTofu;
Confluent Schema Registry is source-available -> not allowed; Docker Desktop is proprietary).
Azure managed services are allowed as the place things run (hosting, not software we adopt).

| Concern | Choice |
|---|---|
| Streaming | Kafka (single-broker KRaft), Avro + Karapace schema registry (ADR-003) |
| Processing | Spark Structured Streaming / PySpark, Delta Lake |
| Batch orchestration | Airflow |
| Analytics modelling | dbt Core (gold layer) |
| Data quality | Great Expectations or Soda Core (decide in ADR-005) |
| ML | LightGBM, scikit-learn, Optuna, MLflow; open pretrained forecasting model (ADR-006) |
| Metadata database | PostgreSQL (Airflow, MLflow backend, Optuna study storage) |
| APIs / serving | FastAPI service on AKS, KEDA (Kafka lag scaler) |
| Storage | Local filesystem (dev), ADLS Gen2 (cloud) |
| Observability | OpenTelemetry -> Application Insights / Azure Monitor |
| IaC | OpenTofu (remote state in Azure Storage) |
| CI/CD | GitHub Actions, OIDC to Azure (no stored secrets), Helm |
| Secrets | Azure Key Vault |
| Local dev | Docker Engine in WSL2 (not Docker Desktop), Docker Compose with profiles, k3d |
| Python tooling | uv (workspace), ruff, mypy, pytest, pre-commit |

## Local development constraints

The dev machine has **8 GB RAM**. The full stack cannot run at once.
- Docker Compose profiles: start only the group being worked on (`stream`, `ml`, `batch`).
- WSL2 memory capped (~5 GB) via `.wslconfig`.
- k3d only for KEDA / inference work, with nothing else running.
- Heavy full-data jobs (Spark backfill, Optuna studies) run as short Azure bursts.

## Deferred (do NOT build until the MVP is done)

Microsoft Fabric, Strimzi, Debezium/CDC, Argo CD, canary releases, OTTO second store,
GRU model, ensemble, gold-layer read API. Dropped entirely: Amazon Reviews, LLM catalogue
enrichment, TimeGEN-1 (proprietary).

## ADRs

| ADR | Topic | Status |
|---|---|---|
| 001 | Scope, datasets, batch + streaming architecture | Accepted |
| 002 | Model strategy | Accepted (amended by 006) |
| 003 | Schema registry: Karapace | Accepted |
| 004 | How dbt runs against Delta (prototype first) | Open |
| 005 | Great Expectations vs Soda Core | Open |
| 006 | Open pretrained forecasting model (replaces TimeGEN-1) | Open |
| 007 | Local runtime under 8 GB; where Spark runs | Accepted |
| 008 | REST APIs with FastAPI | Open |
| 009 | Development data sampling | Accepted |

Each open ADR must be accepted before its component is implemented.

## Repository layout (target)

```
flagshipProject/
  CLAUDE.md
  README.md
  pyproject.toml           # uv workspace root + ruff/mypy/pytest config
  Makefile                 # make up / make test / make replay (added with Compose)
  docs/
    adr/                   # ADR-NNN-title.md
    failure-modes.md
    postmortems/
    runbooks/
  infra/tofu/              # ADLS, AKS, ACR, Key Vault, App Insights
  deploy/helm/             # kafka, replay, inference, airflow, mlflow
  services/
    replay/                # time-split, time-shift, event-time replay into Kafka
    inference/             # purchase-intent service (FastAPI + Kafka consumer)
    review-queue/          # planner approval + quarantine triage (FastAPI)
  pipelines/
    spark/                 # streaming + backfill jobs (shared code)
    dbt/                   # gold star schema + tests
    airflow/dags/
  ml/
    forecasting/
    purchase_intent/
    tuning/                # Optuna studies
  contracts/               # Avro schemas, data contracts
  tests/
  .github/workflows/
```

## Working rules for Claude Code

1. **Explain before writing.** For any non-trivial component, explain the design and the
   trade-offs first, and wait for agreement before generating code.
2. **The owner must be able to defend every line in an interview.** Prefer small, reviewable
   changes. When asked, review the owner's code rather than rewriting it.
3. **Respect the scope.** Do not add tools or components that are not listed above. If something
   seems needed, propose an ADR instead.
4. **Idempotency by default.** Every write path must be safe to replay (event IDs, Delta MERGE).
5. **Every failure path gets a test** and a row in `docs/failure-modes.md`.
6. **No secrets in code or config.** Key Vault + workload identity / OIDC only.
7. **Time-based splits only** for any ML validation. Never random splits on temporal data.
8. **Cost awareness.** Anything that runs on Azure must be stoppable; note its cost impact.
9. Code style: Python 3.11+, ruff, mypy, pytest; type hints on all public functions.
10. **Free and open source only.** Verify the licence of every new tool or library.
11. **Build a thin slice first.** Replay -> Kafka -> Spark -> Delta -> trivial consumer
    working end to end locally by the end of week 3, then harden each stage.

## Budget

Azure for Students (USD 100 credit). Develop locally; run AKS in bursts and stop it
when idle (`az aks stop`). Cost Management budgets with alerts at 50/75/90% (note: cost data
lags by hours, so alerts are not a real-time cut-off). Report cost per 1,000 predictions as a
deliverable.

## Timeline

| Phase | Weeks | Outcome |
|---|---|---|
| Foundations | 1 | Repo, CI, data download, sampling, time-split job, local runtime working |
| Ingestion | 2-3 | Replay service, Kafka, bronze/silver, DLQs, contracts; thin slice end to end |
| Modelling | 4 | dbt star schema, SCD2, sessionisation, purchase-intent baseline |
| Serving + HITL | 5 | Inference + KEDA, planner queue API, MLflow registry |
| Deploy + monitor | 6 | Azure account, OpenTofu, AKS, App Insights, SLOs. MVP; start applying for jobs |
| Hardening | 7-10 | Continuous training, Optuna at scale, fault injection, reconciliation, OTTO store |
| Write-up | 11-12 | Postmortems, results, cost report, demo video, blog posts |
