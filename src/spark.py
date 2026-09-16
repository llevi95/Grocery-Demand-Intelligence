import os
from pathlib import Path

from pyspark.sql import SparkSession

# Spark 3.5 needs Java 17. GUI-launched apps (VS Code from the Dock, JupyterLab from a stale
# terminal) do not read ~/.zshrc, so point at the brew JDK if nothing else is set.
_JAVA17 = Path("/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home")
if "JAVA_HOME" not in os.environ and _JAVA17.exists():
    os.environ["JAVA_HOME"] = str(_JAVA17)


def get_spark(app_name: str = "grocery-demand") -> SparkSession:
    """One Spark session config for the whole project"""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )