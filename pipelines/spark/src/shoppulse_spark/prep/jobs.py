"""Prep jobs: read, transform, write, and record a manifest of what was produced.

Both jobs overwrite their whole output, so re-running with the same inputs and parameters
produces the same data (idempotent, rule 4).
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, Observation, SparkSession
from pyspark.sql import functions as F

from shoppulse_spark.prep import rees46

MANIFEST_NAME = "_manifest.json"  # leading underscore: Spark ignores it when reading the folder


def write_manifest(output: str, manifest: dict[str, Any]) -> None:
    # Local filesystem only for now; becomes a storage-agnostic writer with the move to ADLS.
    path = Path(output) / MANIFEST_NAME
    text = json.dumps(manifest, indent=2, sort_keys=True, default=str) + "\n"
    path.write_text(text, encoding="utf-8")


def run_sample(
    spark: SparkSession, input_paths: list[str], output: str, sample_pct: int
) -> dict[str, Any]:
    """Validate raw CSVs and keep the user sample, in a single pass over the raw files.

    Output is partitioned by `status` (valid / rejected) and `event_date`. Rejected rows are
    kept in full (whatever the user) so they can be triaged.
    """
    parsed = rees46.parse(rees46.read_raw(spark, input_paths))
    is_rejected = F.col("reject_reason").isNotNull()
    in_sample = F.coalesce(rees46.in_user_sample(sample_pct), F.lit(False))

    # observe() counts rows as they stream through the one write, avoiding a second scan of
    # tens of GB of CSV. Counts are best-effort if Spark retries a stage; the data is exact.
    stats = Observation("sample")
    observed = parsed.observe(
        stats,
        F.count(F.lit(1)).alias("rows_read"),
        F.sum(is_rejected.cast("long")).alias("rows_rejected"),
        F.sum((~is_rejected & in_sample).cast("long")).alias("rows_sampled"),
    )
    (
        observed.where(is_rejected | in_sample)
        .withColumn("status", F.when(is_rejected, "rejected").otherwise("valid"))
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("raw_record", F.when(is_rejected, F.col("raw_record")))
        .repartition("status", "event_date")
        .write.mode("overwrite")
        .partitionBy("status", "event_date")
        .parquet(output)
    )

    counts = stats.get
    rows_valid = counts["rows_read"] - counts["rows_rejected"]
    rejects_by_reason = {
        row["reject_reason"]: row["count"]
        for row in spark.read.parquet(output)
        .where(F.col("status") == "rejected")
        .groupBy("reject_reason")
        .count()
        .collect()
    }
    manifest: dict[str, Any] = {
        "step": "sample",
        "store_id": rees46.STORE_ID,
        "inputs": input_paths,
        "sample_pct": sample_pct,
        "rows_read": counts["rows_read"],
        "rows_rejected": counts["rows_rejected"],
        "rows_valid": rows_valid,
        "rows_sampled": counts["rows_sampled"],
        "sampled_share_of_valid": counts["rows_sampled"] / rows_valid if rows_valid else None,
        "rejects_by_reason": rejects_by_reason,
    }
    write_manifest(output, manifest)
    return manifest


def _time_range(df: DataFrame, column: str) -> dict[str, Any]:
    # Format inside Spark (session time zone = UTC). collect() would convert timestamps to the
    # machine's local time zone, giving different manifests on different machines.
    iso = "yyyy-MM-dd'T'HH:mm:ssXXX"
    row = df.agg(
        F.date_format(F.min(column), iso).alias("min"),
        F.date_format(F.max(column), iso).alias("max"),
    ).first()
    if row is None:  # cannot happen for a global aggregate; keeps the type checker honest
        return {"min": None, "max": None}
    return {"min": row["min"], "max": row["max"]}


def run_split(
    spark: SparkSession, input_path: str, output: str, split_at: datetime, shift_weeks: int
) -> dict[str, Any]:
    """Shift the sampled valid events and write `history/` and `live/` partitioned by date."""
    valid = spark.read.parquet(input_path).where(F.col("status") == "valid")
    history, live = rees46.shift_and_split(valid, split_at, shift_weeks)
    for name, df in (("history", history), ("live", live)):
        (
            df.repartition("event_date")
            .write.mode("overwrite")
            .partitionBy("event_date")
            .parquet(f"{output}/{name}")
        )

    written = {name: spark.read.parquet(f"{output}/{name}") for name in ("history", "live")}
    rows = {name: df.count() for name, df in written.items()}
    everything = written["history"].unionByName(written["live"])
    total = rows["history"] + rows["live"]
    duplicates = total - everything.select("event_id").distinct().count()

    manifest: dict[str, Any] = {
        "step": "split",
        "store_id": rees46.STORE_ID,
        "input": input_path,
        "split_at": split_at.isoformat(),
        "shift_weeks": shift_weeks,
        "shift_days": shift_weeks * 7,
        "rows_history": rows["history"],
        "rows_live": rows["live"],
        "exact_duplicate_rows": duplicates,
        "exact_duplicate_rate": duplicates / total if total else None,
        "history_source_time": _time_range(written["history"], "source_event_time"),
        "live_source_time": _time_range(written["live"], "source_event_time"),
        "live_shifted_time": _time_range(written["live"], "event_time"),
    }
    write_manifest(output, manifest)
    return manifest
