# ADR-009: Development data sampling

- **Status:** Accepted
- **Date:** 2026-10-07

## Context

REES46 monthly CSVs are roughly 5-9 GB each (hundreds of millions of rows across months).
The dev machine has 8 GB RAM (ADR-007). Iterating on the full data locally is too slow, but
results reported in the README must come from the full data.

## Decision

- Develop on a **deterministic user-level sample**: keep a user if
  `pmod(xxhash64(user_id), 100) < SAMPLE_PCT` (default 5).
- The sample is taken **once** from the raw files and written as Parquet; every later step
  reads the sample, never the raw CSVs.
- `SAMPLE_PCT=100` means the full dataset; same code path, run in bursts on Azure.
- Reported results (model metrics, throughput, cost) always state which sample produced them;
  headline numbers use the full data.

## Why hash on user, not random rows

- **Whole user histories survive:** sessions, cart->purchase sequences and per-user features stay
  intact. Random row sampling would break sessions and make purchase intent meaningless.
- **Stable across months and across the time split:** a user in the sample in October is in it
  in November too, so history and "live" periods stay consistent.
- **Reproducible:** no seed to lose; anyone re-running gets the identical sample.

## Consequences

- Category-level demand in the sample is roughly `SAMPLE_PCT`% of the real demand; forecasting
  metrics like WAPE/MASE are scale-free, but absolute numbers are not comparable to full data.
- Rare products appear in fewer events, so some real price changes (SCD2) are not visible in
  the sample. Acceptable for development; SCD2 is verified on full data.
- Hash bucketing is only approximately `SAMPLE_PCT`%; the sampling job logs the actual ratio.
