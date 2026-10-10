from collections.abc import Iterator
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from shoppulse_spark.session import local_session

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    session = local_session("tests", master="local[2]", driver_memory="1g")
    session.conf.set("spark.sql.shuffle.partitions", "2")
    yield session
    session.stop()


@pytest.fixture
def rees46_csv() -> str:
    return str(FIXTURES / "rees46_small.csv")
