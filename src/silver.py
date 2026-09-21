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