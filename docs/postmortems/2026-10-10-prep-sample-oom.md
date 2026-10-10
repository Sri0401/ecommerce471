# Postmortem: first full-data prep run fails with "Cannot allocate memory"

- **Date:** 2026-10-10
- **Severity:** Low (development; no data loss, no downstream consumers yet)
- **Component:** Data prep, `sample` step (`shoppulse_spark.prep`)
- **Status:** Resolved

## Summary

The first run of the sample step over the full REES46 data (Oct 2019 - Feb 2020, ~39 GB of CSV)
failed after 13 minutes with `java.io.IOException: Cannot allocate memory` while reading
`2020-Feb.csv`. Spark's shuffle files were being written to `/tmp`, which on Ubuntu 26.04 is a
**tmpfs held in RAM**. Shuffle data plus the 2 GB driver heap exhausted the 5 GB WSL2 memory cap.

## Impact

- The prep run was lost (13 minutes of compute); no output was published.
- No partial data was left behind that could be mistaken for a result: the job writes in
  overwrite mode and never wrote its `_manifest.json`, so the output was visibly incomplete
  and the rerun replaced it cleanly.

## Timeline (AEDT)

| Time | Event |
|---|---|
| 20:10:04 | Sample step started (local mode, 12 cores, 2 GB driver heap, WSL2 capped at 5 GB) |
| 20:23:06 | Task 206 of ~300 in stage 0 fails: `FAILED_READ_FILE` caused by `IOException: Cannot allocate memory`; many `Error deleting /tmp/blockmgr-.../temp_shuffle_*` warnings |
| 20:23:07 | Spark aborts the job (task failure in local mode is not retried) |
| 20:23 | Diagnosed from the log and `findmnt /tmp` (`tmpfs 2.4G`) |
| 20:24:17 | Fix applied (`SPARK_LOCAL_DIRS` on disk); rerun started |

## Root cause

Spark writes shuffle and spill files to `spark.local.dir`, which defaults to `/tmp`. Since
Ubuntu 25.04, `/tmp` is mounted as **tmpfs** (RAM-backed, sized at half of RAM). The sample
step repartitions its output by `(status, event_date)`, so all sampled and rejected rows pass
through a shuffle. Shuffle files in tmpfs count against the same 5 GB WSL2 limit as the JVM, so
memory ran out while still reading input, and the failure surfaced on a file read.

## Why it was not caught earlier

- The tests run on a 12-row fixture; shuffle volume is negligible, so tmpfs never filled.
- Memory planning in ADR-007 counted the JVM heap but treated `/tmp` as disk.
- The Windows test environment has no tmpfs at all.

## Resolution

`SPARK_LOCAL_DIRS=$HOME/.cache/spark-local` (on the WSL2 ext4 disk) is set in the dev
environment. It is environment configuration, not code: CI's `/tmp` is disk-backed, and on AKS
Kubernetes provides the local directories.

## What went well

- The failure was loud and fast, not silent: the job aborted and the missing manifest made the
  incomplete output obvious.
- Idempotent overwrite meant the rerun needed no cleanup logic.
- Peak memory and wall time were being recorded (`/usr/bin/time -v`), which narrowed the search.

## Action items

| Action | Status |
|---|---|
| Set `SPARK_LOCAL_DIRS` in the dev environment; document in ADR-007 | Done |
| Failure-mode row for "Spark scratch space on tmpfs" | Done (#23) |
| Preflight check: `local_session` warns if the Spark local dir is on tmpfs | Open |
| Include `/tmp`-type scratch space in the memory budget for every new component | Open (ADR-007) |
