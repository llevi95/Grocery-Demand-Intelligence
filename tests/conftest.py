import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    s = (
        SparkSession.builder.master("local[2]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled","false")
        .get0Create()
    )
    yield s
    s.stop()