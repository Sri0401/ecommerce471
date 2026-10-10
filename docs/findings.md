# Findings

Things learned from the data and from building the platform. Each entry says how it was
found and what was done about it. Numbers come from `_manifest.json` files; state the sample.

## Data findings (REES46)

### Category taxonomy changes on 2019-12-01 (found 2026-10-07)
- **What:** from December 2019 the same products (stable `product_id`) appear under new
  `category_id` values, and the new `category_code` labels are wrong. Example, first 3M rows of
  each month:

  | product_id | Oct-Nov 2019 | Dec 2019 - Feb 2020 |
  |---|---|---|
  | 1005105, 1004785, 1004233 (phones) | `2053013555631882655` / `electronics.smartphone` | `2232732093077520756` / `construction.tools.light` |

- **Impact:** per-category demand series break on 1 December; `dim_category` cannot trust
  `category_code` after November; naive per-category forecasting would see categories vanish
  and new ones appear.
- **How found:** header and boundary checks on the raw files before the first prep run.
- **Handling:** prep keeps the raw values unchanged (faithful source data). Harmonisation is
  a modelling decision: see the ADR to be written (mapping new -> old categories through
  products seen on both sides, with a coverage metric).

### Exact duplicate rows
- **What:** the raw files contain rows identical in every field.
- **Handling:** they get the same deterministic `event_id` and are collapsed by the silver
  dedup, treated as logging duplicates. Not dropped in prep, so the pipeline handles real
  duplicates, not only the ones injected by the replay service.
- **Rate (5% user sample, Oct 2019 - Feb 2020, run 2026-10-10):** 54,016 of 14,363,401 rows
  = **0.376%** are exact duplicates of another row.

### Rejected rows
- **Rate (all 288,779,227 raw rows, run 2026-10-10):** **0 rejects.** Every row parsed: valid
  timestamp, known event type, numeric IDs, non-negative price. The raw files are clean at the
  row level; REES46's quality problems are semantic (category re-keying above, duplicates),
  not syntactic. The reject path stays, because the replay's malformed messages and the
  second store will exercise it.

### Volumes (first full prep run, 2026-10-10)

| | Rows | Source period | Shifted period |
|---|---|---|---|
| Raw (all users) | 288,779,227 | 2019-10-01 - 2020-02-29 | |
| 5% user sample | 14,363,401 (4.97% of rows) | | |
| History | 13,130,640 | 2019-10-01 - 2020-02-15 (138 days) | |
| Live | 1,232,761 | 2020-02-16 - 2020-02-29 (14 days) | 2026-10-11 - 2026-10-24 |

- Live averages ~88k events/day in the sample (~1 event/s at real time, ~60/s at 60x replay).
- Run times on the 8 GB laptop (WSL2, 5 GB cap, local[*]): sample **21m15s** over 39 GB of
  CSV; split **10m34s**. Outputs: 1.3 GB sampled, 1.4 GB split (Parquet).

## Engineering findings

### First full replay into Kafka (2026-10-10)
- **Input:** 1,232,761 live events (5% sample), `chaos` scenario, `--max-speed`.
- **Output:** 1,246,375 messages = events + 13,614 injected faults (~1% duplicates,
  ~0.1% malformed; late events re-timed, not added).
- **Throughput:** 91 s wall, **~13.7k messages/s** from one Python process, peak RSS 458 MB.
  At the real 60x replay speed the stream needs ~60 msg/s, so producer headroom is ~200x.
- **Balance:** 6 partitions, 204,475-211,448 messages each (±1.7%) with key `store_id:user_id`.
- **Exactness:** topic offsets = replay messages + integration-test messages, to the message.
  Rerunning without `--reset` resumed from the checkpoint and sent 0 messages.
- **Broker:** Kafka 465 MB, Karapace 102 MB resident after the run (limits 1 GB / 384 MB).

### WSL2 stops the VM when the last terminal closes, killing containers (2026-10-10)
- **How found:** Karapace "unreachable" between two commands; containers showed `Exited (143)`
  (SIGTERM) and the VM uptime was 2 minutes.
- **Fix:** `vmIdleTimeout=-1` and `[general] instanceIdleTimeout=-1` in `.wslconfig`, plus
  `restart: unless-stopped` on Kafka and Karapace. Free memory with `make down` /
  `wsl --shutdown`.

### Karapace reports healthy before it can accept writes (2026-10-10)
- **How found:** the first `make register` after a restart timed out; a retry seconds later
  took 38 ms.
- **Why:** reads work as soon as the HTTP server is up; writes need the instance to be elected
  primary and to have read `_schemas`. The health check only tested a read.
- **Fix:** the health check requires `schema_registry_ready` and `schema_registry_is_primary`
  from `/_health`, so `make up-stream` returns only when writes work.

### PySpark `collect()` converts timestamps to the machine's local time zone (2026-10-07)
- **How found:** tests passed in Spark but failed in Python on a machine in Melbourne:
  a 347-week shift appeared to be 347 weeks minus one hour (daylight saving).
- **Why:** Spark stores UTC instants; `collect()` turns them into naive Python datetimes in
  the local time zone. The session time zone setting does not affect this conversion.
- **Fix:** all timestamp maths and formatting stay inside Spark (`unix_timestamp`,
  `date_format` with the session time zone pinned to UTC). The manifest's time ranges were
  affected too and are now formatted in Spark.
