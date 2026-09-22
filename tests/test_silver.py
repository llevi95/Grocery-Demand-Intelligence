import datetime as dt
from pyspark.sql import functions as F
from src.silver import complete_grid
from src.silver import forward_fill_oil


def _sales(spark):
    rows = [
        # store, family, date, sales, promo. 2 series, day 2 missing in series A, day 1-3 missing in B
        (1, "A", dt.date(2017, 1, 1), 5.0, 0),
        (1, "A", dt.date(2017, 1, 3), 7.0, 1),
        (2, "B", dt.date(2017, 1, 4), 9.0, 0),
    ]
    return spark.createDataFrame(rows, "store_nbr int, family string, date date, sales double, onpromotion int")


def test_grid_has_one_row_per_key_per_day(spark):
    out = complete_grid(_sales(spark), "2017-01-01", "2017-01-04")
    assert out.count() == 2 * 4
    assert out.groupBy("store_nbr", "family", "date").count().filter("count > 1").count() == 0


def test_missing_days_filled_with_zero_and_existing_kept(spark):
    out = complete_grid(_sales(spark), "2017-01-01", "2017-01-04")
    row = out.filter((F.col("store_nbr") == 1) & (F.col("date") == dt.date(2017, 1, 2))).first()
    assert row["sales"] == 0.0 and row["onpromotion"] == 0
    row = out.filter((F.col("store_nbr") == 1) & (F.col("date") == dt.date(2017, 1, 3))).first()
    assert row["sales"] == 7.0 and row["onpromotion"] == 1

def test_forward_fill_oil_fills_gaps_and_nulls(spark):
    oil = spark.createDataFrame(
        [(dt.date(2017, 1, 1), None), (dt.date(2017, 1, 2), 50.0), (dt.date(2017, 1, 4), None)],
        "date date, dcoilwtico double",
    )
    out = forward_fill_oil(oil, "2017-01-01", "2017-01-05").orderBy("date").collect()
    assert [r["oil_price"] for r in out] == [None, 50.0, 50.0, 50.0, 50.0]
    assert set(forward_fill_oil(oil, "2017-01-01", "2017-01-05").columns) == {"date", "oil_price"}