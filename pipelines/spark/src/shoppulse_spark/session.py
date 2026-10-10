from pyspark.sql import SparkSession


def local_session(
    app_name: str, master: str = "local[*]", driver_memory: str = "2g"
) -> SparkSession:
    """Spark in local mode (ADR-007).

    The session time zone is pinned to UTC so event times and dates never depend on the
    machine the job runs on. Parquet timestamps are written as standard TIMESTAMP_MICROS
    (UTC-adjusted) instead of legacy INT96, so non-Spark readers such as pyarrow in the
    replay service read them back as UTC instants.
    """
    return (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.driver.memory", driver_memory)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
