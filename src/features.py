from pyspark.sql import DataFrame, functions as F, Window

SERIES = ["store_nbr", "family"]


def add_calendar(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("dow", F.dayofweek("date"))                    # 1 = Sunday ... 7 = Saturday
          .withColumn("dom", F.dayofmonth("date"))
          .withColumn("month", F.month("date"))
          .withColumn("woy", F.weekofyear("date"))
          .withColumn("payday", ((F.col("dom") == 15) | (F.col("date") == F.last_day("date"))).cast("int"))
          .withColumn("is_closed_day", ((F.col("month") == 12) & (F.col("dom") == 25)).cast("int"))
    )


def trim_to_first_sale(df: DataFrame) -> DataFrame:
    """Drop the all-zero prefix of each series (store not open yet). Zeros after opening are kept."""
    w = Window.partitionBy(*SERIES)
    first = F.min(F.when(F.col("sales") > 0, F.col("date"))).over(w)
    return df.withColumn("first_sale_date", first).filter(F.col("date") >= F.col("first_sale_date"))