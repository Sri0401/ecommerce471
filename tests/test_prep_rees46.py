import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

from shoppulse_spark.prep import jobs, rees46

SPLIT_AT = datetime(2019, 10, 10, tzinfo=UTC)

# Writing files with Spark on native Windows needs Hadoop's winutils; these run in WSL2 / CI.
needs_posix = pytest.mark.skipif(sys.platform == "win32", reason="Spark writes need WSL2/Linux")


@pytest.fixture
def parsed(spark: SparkSession, rees46_csv: str) -> DataFrame:
    return rees46.parse(rees46.read_raw(spark, [rees46_csv])).cache()


def _valid(parsed: DataFrame) -> DataFrame:
    return parsed.where(F.col("reject_reason").isNull())


# --- parse and validate -------------------------------------------------------------------


def test_each_bad_row_is_rejected_with_its_reason(parsed: DataFrame) -> None:
    reasons = sorted(
        r["reject_reason"] for r in parsed.where("reject_reason IS NOT NULL").collect()
    )
    assert reasons == [
        "bad_event_time",
        "bad_event_type",
        "bad_price",
        "bad_user_id",
        "malformed_row",
    ]
    assert _valid(parsed).count() == 7


def test_rejected_rows_keep_the_raw_record_for_triage(parsed: DataFrame) -> None:
    bad_user = parsed.where(F.col("reject_reason") == "bad_user_id").first()
    assert bad_user is not None
    assert json.loads(bad_user["raw_record"])["user_id"] == "abc"


def test_null_session_is_valid(parsed: DataFrame) -> None:
    assert _valid(parsed).where(F.col("user_session").isNull()).count() == 1


def test_price_is_decimal_not_float(parsed: DataFrame) -> None:
    assert parsed.schema["price"].dataType == DecimalType(12, 2)


# Timestamp checks run inside Spark: collect() converts timestamps to the machine's local time
# zone (including DST), so comparing collected values in Python depends on where tests run.


def test_event_time_is_parsed_as_utc(parsed: DataFrame) -> None:
    first = _valid(parsed).agg(F.date_format(F.min("event_time"), "yyyy-MM-dd HH:mm:ss")).first()
    assert first is not None
    assert first[0] == "2019-10-01 00:00:00"


# --- event_id -----------------------------------------------------------------------------


def test_exact_duplicates_share_an_event_id(parsed: DataFrame) -> None:
    valid = _valid(parsed)
    assert valid.count() == 7
    assert valid.select("event_id").distinct().count() == 6


def test_event_id_is_stable_across_runs(spark: SparkSession, rees46_csv: str) -> None:
    def ids() -> list[str]:
        df = rees46.parse(rees46.read_raw(spark, [rees46_csv]))
        return sorted(r["event_id"] for r in df.select("event_id").collect())

    assert ids() == ids()


# --- sampling -----------------------------------------------------------------------------


def test_sample_100_keeps_all_and_0_keeps_none(parsed: DataFrame) -> None:
    valid = _valid(parsed)
    assert valid.where(rees46.in_user_sample(100)).count() == valid.count()
    assert valid.where(rees46.in_user_sample(0)).count() == 0


def test_sample_keeps_whole_users(parsed: DataFrame) -> None:
    valid = _valid(parsed)
    per_user = valid.groupBy("user_id").agg(
        F.count_distinct(rees46.in_user_sample(50)).alias("decisions")
    )
    assert per_user.where(F.col("decisions") > 1).count() == 0


@pytest.mark.parametrize("pct", [-1, 101])
def test_sample_pct_out_of_range_is_rejected(pct: int) -> None:
    with pytest.raises(ValueError):
        rees46.in_user_sample(pct)


# --- shift and split ----------------------------------------------------------------------


def test_split_is_disjoint_and_complete(parsed: DataFrame) -> None:
    valid = _valid(parsed)
    history, live = rees46.shift_and_split(valid, SPLIT_AT, shift_weeks=1)
    assert history.count() + live.count() == valid.count()
    assert history.join(live, "event_id").count() == 0


def test_event_exactly_at_split_goes_to_live(parsed: DataFrame) -> None:
    history, live = rees46.shift_and_split(_valid(parsed), SPLIT_AT, shift_weeks=1)
    boundary = F.lit(SPLIT_AT)  # aware datetime -> correct UTC instant
    assert live.where(F.col("source_event_time") == boundary).count() == 1
    assert history.where(F.col("source_event_time") >= boundary).count() == 0


def test_shift_is_whole_weeks_and_keeps_weekday(parsed: DataFrame) -> None:
    history, live = rees46.shift_and_split(_valid(parsed), SPLIT_AT, shift_weeks=347)
    shift_seconds = int(timedelta(weeks=347).total_seconds())
    wrong = history.unionByName(live).where(
        (F.unix_timestamp("event_time") - F.unix_timestamp("source_event_time") != shift_seconds)
        | (F.dayofweek("event_time") != F.dayofweek("source_event_time"))
        | (F.col("event_date") != F.to_date("event_time"))
    )
    assert wrong.count() == 0


def test_negative_shift_is_rejected(parsed: DataFrame) -> None:
    with pytest.raises(ValueError):
        rees46.shift_and_split(parsed, SPLIT_AT, shift_weeks=-1)


def test_naive_split_time_is_rejected(parsed: DataFrame) -> None:
    with pytest.raises(ValueError):
        rees46.shift_and_split(parsed, datetime(2019, 10, 10), shift_weeks=1)


# --- jobs ---------------------------------------------------------------------------------


def test_missing_input_file_fails_fast(spark: SparkSession, tmp_path: Path) -> None:
    with pytest.raises(AnalysisException):
        jobs.run_sample(spark, [str(tmp_path / "missing.csv")], str(tmp_path / "out"), 5)


@needs_posix
def test_jobs_end_to_end_and_rerun_is_identical(
    spark: SparkSession, rees46_csv: str, tmp_path: Path
) -> None:
    sampled, split = str(tmp_path / "sampled"), str(tmp_path / "split")

    def run() -> list[tuple[object, ...]]:
        jobs.run_sample(spark, [rees46_csv], sampled, sample_pct=100)
        jobs.run_split(spark, sampled, split, SPLIT_AT, shift_weeks=1)
        rows = spark.read.parquet(f"{split}/history").unionByName(
            spark.read.parquet(f"{split}/live")
        )
        return sorted(tuple(r) for r in rows.collect())

    first = run()
    assert run() == first  # idempotent: overwriting gives the same data

    sample_manifest = json.loads((Path(sampled) / jobs.MANIFEST_NAME).read_text("utf-8"))
    assert sample_manifest["rows_read"] == 12
    assert sample_manifest["rows_rejected"] == 5
    assert sample_manifest["rows_sampled"] == 7
    assert sum(sample_manifest["rejects_by_reason"].values()) == 5

    split_manifest = json.loads((Path(split) / jobs.MANIFEST_NAME).read_text("utf-8"))
    assert split_manifest["rows_history"] == 5
    assert split_manifest["rows_live"] == 2
    assert split_manifest["exact_duplicate_rows"] == 1
