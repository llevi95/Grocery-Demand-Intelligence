from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


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

START, END = "2013-01-01", "2017-08-15"

SCOPED_STORES = [44, 51, 50, 9, 39, 34, 40, 1, 24, 38, 28, 43]
SCOPED_FAMILIES = ["BREAD/BAKERY", "DAIRY", "PRODUCE", "BEVERAGES",
                   "CLEANING", "GROCERY I", "POULTRY", "FROZEN FOODS"]


def build_silver(sales, stores, oil, holidays, transactions, start: str = START, end: str = END) -> DataFrame:
    """One row per store x family x day, joined with store metadata, oil, holiday flags, transactions."""
    grid = complete_grid(sales, start, end)
    return (
        grid.join(stores, "store_nbr", "left")
            .join(forward_fill_oil(oil, start, end), "date", "left")
            .join(holiday_flags(holidays), "date", "left")
            .join(transactions, ["store_nbr", "date"], "left")
            .fillna({"national_holiday": 0, "any_holiday": 0, "transactions": 0})
    )


def scope(df: DataFrame, stores: list[int] = SCOPED_STORES, families: list[str] = SCOPED_FAMILIES) -> DataFrame:
    return df.filter(F.col("store_nbr").isin(stores) & F.col("family").isin(families))