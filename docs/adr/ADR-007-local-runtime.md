# ADR-007: Local runtime on an 8 GB machine; where Spark runs

- **Status:** Accepted
- **Date:** 2026-10-07

## Context

The development machine is Windows 11 with 7.9 GB RAM and 12 CPU threads. The stack
(Kafka, schema registry, Spark, Airflow, MLflow, Postgres, later k3d) needs well over 8 GB if
started together. Only free and open-source software may be used, so Docker Desktop is excluded.
Spark also runs poorly on native Windows (Hadoop `winutils` workarounds).

## Decision

1. **Linux via WSL2.** All tooling (uv, Spark, Docker Engine, k3d) runs inside a WSL2 Ubuntu
   distro. The repository stays on `D:\flagshipProject` (accessed as `/mnt/d/flagshipProject`).
2. **Docker Engine (Apache 2.0) inside WSL2**, not Docker Desktop.
3. **WSL2 memory capped at 5 GB** in `%USERPROFILE%\.wslconfig` so Windows stays responsive.
4. **Docker Compose profiles** so only one group runs at a time:

   | Profile | Services | Approx. memory |
   |---|---|---|
   | `stream` | Kafka (KRaft), schema registry | ~1.5 GB est.; **measured 0.57 GB** after a 1.25M-message replay |
   | `ml` | Postgres, MLflow | ~0.7 GB |
   | `batch` | Postgres, Airflow (LocalExecutor) | ~1.5 GB |

5. **Spark in dev = PySpark local mode** (`local[*]`, driver memory ~2 GB), installed from PyPI
   into the uv environment, run as a normal process in WSL2. No Spark cluster locally.
6. **Spark in cloud = native Spark-on-Kubernetes** (`spark-submit --master k8s://...`), run as
   short bursts on AKS. No Spark operator (would be an extra component).
7. **k3d only for KEDA / inference work**, started with no Compose profile running.
8. **Spark scratch space on disk:** `SPARK_LOCAL_DIRS=$HOME/.cache/spark-local`. On Ubuntu
   26.04 `/tmp` is a RAM-backed tmpfs, and Spark's default shuffle directory there counts
   against the WSL2 memory cap (see `docs/postmortems/2026-10-10-prep-sample-oom.md`).

### Dev environment (WSL2 Ubuntu, `~/.bashrc`)

```bash
export UV_PROJECT_ENVIRONMENT=$HOME/.venvs/shoppulse   # venv on ext4, not /mnt/d
export SPARK_LOCAL_DIRS=$HOME/.cache/spark-local       # never tmpfs
```

## Alternatives considered

- *Docker Desktop:* proprietary licence. Rejected (FOSS rule).
- *Spark standalone cluster in Compose:* master + worker containers cost ~1.5 GB extra for no
  benefit on a single machine. Rejected.
- *Spark operator on k8s:* nicer job lifecycle, but another component to run and explain.
  Deferred; revisit if native spark-submit proves painful.
- *Repository inside the WSL2 filesystem:* faster file I/O than `/mnt/d`, but the owner wants
  the project on `D:`. Accepted the slower I/O; measure it, revisit if it hurts.

## Consequences

- Every component must fit the profile table; anything heavier runs on Azure in bursts.
- Same PySpark code runs locally and on AKS; only the master URL and storage root change.
- `/mnt/d` I/O is slower than native Linux; the one-time sampling of raw files (ADR-009) pays
  most of this cost.
- Memory figures above are estimates; replace with measured numbers once the profiles run.
