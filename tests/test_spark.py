from src.spark import get_spark
from src.paths import ROOT, RAW, BRONZE


def test_get_spark_applies_config():
    s = get_spark("test-app")
    assert s.conf.get("spark.sql.shuffle.partitions") == "32"


def test_paths_point_at_repo():
    assert (ROOT / "pyproject.toml").exists()
    assert RAW == ROOT / "data" / "raw"
    assert BRONZE == ROOT / "data" / "bronze"