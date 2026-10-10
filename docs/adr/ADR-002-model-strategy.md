# ADR-002: Model strategy (baselines, own tuned models, pretrained cloud model)

- **Status:** Accepted, amended 2026-10-07 (see Amendment below and ADR-006)
- **Date:** 2026-10-07

> **Amendment (2026-10-07):** The project adopted a free-and-open-source-only rule.
> Nixtla TimeGEN-1 via Microsoft Foundry is proprietary and is replaced by a **self-hosted open
> pretrained time-series model**, selected in ADR-006. The build-vs-buy comparison becomes
> "own tuned model vs pretrained foundation model"; everything else in this ADR stands.
> The original text is kept below unchanged as the decision record.

## Context

The project should show both the ability to build and tune custom models and the judgement to
evaluate a pretrained cloud model. Calling an external endpoint alone is not a meaningful
signal; an **evidence-based build-vs-buy comparison** is.

REES46 has no product names or descriptions, so LLM-based catalogue enrichment was dropped:
the model would have almost nothing to work from.

## Decision

### Task A: daily demand forecasting (batch), per category and store

| Model | Type |
|---|---|
| Seasonal naive (same weekday last week) | Baseline that all others must beat |
| LightGBM global model (lags, rolling stats, price features) | Own model, Optuna-tuned |
| Nixtla TimeGEN-1 via Microsoft Foundry | Pretrained, zero-shot |
| Ensemble of the above (optional, hardening) | Combined |

Metrics: WAPE and MASE per category, cost per forecast run, latency, behaviour on failure.
TimeGEN-1 may win because REES46 history is short; that is an acceptable and reportable result.

### Task B: real-time purchase intent (streaming)

Logistic regression baseline -> LightGBM (Optuna) -> optional small GRU (Optuna).
Selection uses multi-objective tuning: **PR-AUC vs p99 inference latency**; choose the best
Pareto-front model that meets the latency SLO.

### Optuna rules
1. Time-based (rolling-origin) validation only. The replayed "live" period is never used in
   tuning and serves as the out-of-time test set.
2. TPE sampler; Hyperband or median pruning; LightGBM pruning callback.
3. Persistent study storage (Postgres/SQLite) so interrupted studies resume.
4. Parallel trials as Kubernetes Jobs sharing study storage, run in bursts.
5. Every trial logged to MLflow as a nested run; best model registered.
6. Retraining reuses stored best params; retuning only when live performance degrades.
7. Report tuning gain vs default params honestly, with a fixed trial budget and fixed seeds.

### Operating the external model
- Timeouts, retries with backoff, circuit breaker; automatic fallback to LightGBM.
- Its forecasts are logged to MLflow and scored with the same metrics.
- Cost logged per call; API key in Key Vault.
- Only aggregated category-level sales leave the platform; no user-level data.

### Promotion
MLflow champion/challenger aliases. Candidate -> automated gates (metrics, latency, slices)
-> human approval -> shadow mode -> champion. Automatic rollback on live SLO breach.

## Consequences

- Strong interview narrative: "I built, tuned, compared and chose based on evidence."
- Depends on TimeGEN-1 being deployable from the student subscription (Marketplace billing,
  region restrictions). If blocked, ADR-006 selects a fallback pretrained model.
- Extra evaluation work, but it reuses the same splits and metric code for all models.

## Open points (to settle before the ML work, week 4)
- Exact purchase-intent label and prediction point (to avoid leakage).
- Category granularity for forecasting (`category_id` vs `category_code`).
- Where the "live" split falls relative to Black Friday 2019 / early-2020 months.
- How p99 latency is measured (inside the serving container, fixed hardware).
- Definition of "degrades" for the retuning trigger.
