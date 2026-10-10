"""REES46 transformations: parse and validate, user sampling, time shift and time split.

Pure DataFrame -> DataFrame functions (apart from `read_raw`) so they can be unit-tested
without writing files. See ADR-001 (split and shift) and ADR-009 (sampling).
"""

from datetime import datetime

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

STORE_ID = "rees46"
EVENT_TYPES = ("view", "cart", "remove_from_cart", "purchase")
RAW_TIME_FORMAT = "yyyy-MM-dd HH:mm:ss 'UTC'"
CORRUPT_COL = "_corrupt_record"

RAW_COLUMNS = (
    "event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
)

# Every column is read as a string: event_id is hashed from the exact source text, and type
# conversion is explicit so bad values are routed to rejects instead of silently becoming null.
# An explicit schema also avoids schema inference, which would scan the raw files twice.
RAW_SCHEMA = StructType(
    [StructField(c, StringType(), True) for c in RAW_COLUMNS]
    + [StructField(CORRUPT_COL, StringType(), True)]
)

OUTPUT_COLUMNS = (
    "event_id",
    "store_id",
    "event_time",
    "source_event_time",
    "event_type",
    "product_id",
    "category_id",
    "category_code",
    "brand",
    "price",
    "user_id",
    "user_session",
    "event_date",
)

# Separator and null marker that cannot occur in CSV text fields, so different rows can never
# produce the same hash input.
_ID_SEPARATOR = "\u001f"
_ID_NULL = "\u0000"


def read_raw(spark: SparkSession, paths: list[str]) -> DataFrame:
    """Read raw REES46 CSVs (plain or .gz). Malformed lines are kept, flagged in CORRUPT_COL."""
    return (
        spark.read.option("header", True)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        .schema(RAW_SCHEMA)
        .csv(paths)
        .withColumn("source_file", F.col("_metadata.file_path"))
    )


def _try_cast(name: str, sql_type: str) -> Column:
    # try_cast returns null on failure instead of raising (Spark 4 runs in ANSI mode).
    return F.expr(f"try_cast({name} AS {sql_type})").alias(name)


def _event_id() -> Column:
    """Deterministic ID from the store and the exact source fields.

    Exact duplicate source rows get the same ID on purpose: they are collapsed by the silver
    dedup and reported as a data finding.
    """
    parts = [F.coalesce(F.col(c), F.lit(_ID_NULL)) for c in RAW_COLUMNS]
    return F.sha2(F.concat_ws(_ID_SEPARATOR, F.lit(STORE_ID), *parts), 256)


def _reject_reason() -> Column:
    """First failed rule, or null for a valid row."""
    return (
        F.when(F.col("_malformed"), "malformed_row")
        .when(F.col("event_time").isNull(), "bad_event_time")
        .when(
            F.col("event_type").isNull() | ~F.col("event_type").isin(*EVENT_TYPES),
            "bad_event_type",
        )
        .when(F.col("product_id").isNull(), "bad_product_id")
        .when(F.col("category_id").isNull(), "bad_category_id")
        .when(F.col("user_id").isNull(), "bad_user_id")
        .when(F.col("price").isNull() | (F.col("price") < 0), "bad_price")
    )


def parse(raw: DataFrame) -> DataFrame:
    """Type the raw strings and add event_id, store_id and reject_reason (null = valid)."""
    typed = raw.select(
        _event_id().alias("event_id"),
        F.lit(STORE_ID).alias("store_id"),
        F.try_to_timestamp(F.col("event_time"), F.lit(RAW_TIME_FORMAT)).alias("event_time"),
        F.col("event_type"),
        _try_cast("product_id", "BIGINT"),
        _try_cast("category_id", "BIGINT"),
        F.col("category_code"),
        F.col("brand"),
        _try_cast("price", "DECIMAL(12,2)"),  # money as decimal, never float
        _try_cast("user_id", "BIGINT"),
        F.col("user_session"),
        F.to_json(F.struct(*RAW_COLUMNS)).alias("raw_record"),
        F.col(CORRUPT_COL).isNotNull().alias("_malformed"),
        F.col("source_file"),
    )
    return typed.withColumn("reject_reason", _reject_reason()).drop("_malformed")


def in_user_sample(sample_pct: int) -> Column:
    """True for users in the deterministic hash sample (ADR-009).

    Hashing the typed BIGINT user_id keeps whole user histories together and selects the
    same users in every month and in both halves of the time split.
    """
    if not 0 <= sample_pct <= 100:
        raise ValueError(f"sample_pct must be between 0 and 100, got {sample_pct}")
    return F.pmod(F.xxhash64(F.col("user_id")), F.lit(100)) < F.lit(sample_pct)


def shift_and_split(
    valid: DataFrame, split_at: datetime, shift_weeks: int
) -> tuple[DataFrame, DataFrame]:
    """Shift event times forward by whole weeks and split into (history, live).

    The shift is given in weeks so it is a multiple of 7 days by construction, which keeps
    weekday seasonality. The split uses the original (source) time, so `split_at` is always
    expressed in the dataset's own calendar.
    """
    if shift_weeks < 0:
        raise ValueError(f"shift_weeks must be >= 0, got {shift_weeks}")
    if split_at.tzinfo is None:
        raise ValueError("split_at must be timezone-aware")

    shifted = (
        valid.withColumnRenamed("event_time", "source_event_time")
        .withColumn(
            "event_time",
            F.col("source_event_time") + F.expr(f"make_dt_interval({shift_weeks * 7})"),
        )
        .withColumn("event_date", F.to_date("event_time"))
        .select(*OUTPUT_COLUMNS)
    )
    is_live = F.col("source_event_time") >= F.lit(split_at)
    return shifted.where(~is_live), shifted.where(is_live)
