from pyspark.sql import DataFrame, functions as F, Window


def date_range(spark, start: str, end: str) -> DataFrame:
    """One row per calendar day in [start, end], column `date`."""
    return spark.sql(f"SELECT explode(sequence(to_date('{start}'), to_date('{end}'))) AS date")


def complete_grid(sales: DataFrame, start: str, end: str) -> DataFrame:
    """Every store x family x day. Missing days become sales=0, onpromotion=0."""
    dates = date_range(sales.sparkSession, start, end)
    keys = sales.select("store_nbr", "family").distinct()
    grid = keys.crossJoin(dates)
    return (
        grid.join(sales.select("store_nbr", "family", "date", "sales", "onpromotion"),
                  ["store_nbr", "family", "date"], "left")
            .fillna({"sales": 0.0, "onpromotion": 0})
    )

def forward_fill_oil(oil: DataFrame, start: str, end: str) -> DataFrame:
    """Daily oil price with weekends/nulls forward-filled. Unpartitioned window: OK for ~1.7k rows only."""
    dates = date_range(oil.sparkSession, start, end)
    w = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return (
        dates.join(oil, "date", "left")
             .withColumn("oil_price", F.last("dcoilwtico", ignorenulls=True).over(w))
             .select("date", "oil_price")
    )

HOLIDAY_TYPES = ["Holiday", "Additional", "Bridge", "Transfer"]   # not Work Day, not Event


def holiday_flags(holidays: DataFrame) -> DataFrame:
    """Collapse the messy holidays file to one row per date with two int flags."""
    return (
        holidays.filter((~F.col("transferred")) & F.col("type").isin(HOLIDAY_TYPES))
                .withColumn("is_national", (F.col("locale") == "National").cast("int"))
                .groupBy("date")
                .agg(F.max("is_national").alias("national_holiday"), F.lit(1).alias("any_holiday"))
    )
