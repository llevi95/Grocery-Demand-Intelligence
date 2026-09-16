from pyspark.sql import SparkSession


def get_spark(app_name: str = "grocery-demand") -> SparkSession:
    """One Spark session config for the whole project"""
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.driver.memory", "6g")
        .config("spark.sql.shuffle.partitions", "32")
        .getOrCreate()
    )