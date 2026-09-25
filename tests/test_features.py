import datetime as dt
from pyspark.sql import functions as F
from src.features import add_calendar, trim_to_first_sale


def test_add_calendar(spark):
    df = spark.createDataFrame([(dt.date(2017, 1, 15),), (dt.date(2017, 2, 28),), (dt.date(2016, 12, 25),), (dt.date(2017, 1, 10),)], "date date")
    out = {r["date"]: r for r in add_calendar(df).collect()}
    assert out[dt.date(2017, 1, 15)]["payday"] == 1
    assert out[dt.date(2017, 2, 28)]["payday"] == 1          # last day of Feb
    assert out[dt.date(2017, 1, 10)]["payday"] == 0
    assert out[dt.date(2016, 12, 25)]["is_closed_day"] == 1
    assert out[dt.date(2017, 1, 15)]["dow"] == 1              # Sunday in Spark's dayofweek


def test_trim_to_first_sale(spark):
    d = dt.date
    df = spark.createDataFrame([
        (1, "A", d(2017, 1, 1), 0.0), (1, "A", d(2017, 1, 2), 0.0), (1, "A", d(2017, 1, 3), 4.0), (1, "A", d(2017, 1, 4), 0.0),
        (2, "A", d(2017, 1, 1), 1.0),
    ], "store_nbr int, family string, date date, sales double")
    out = trim_to_first_sale(df)
    assert out.filter("store_nbr = 1").count() == 2       # Jan 3 and Jan 4 kept (zero after opening is real)
    assert out.filter("store_nbr = 2").count() == 1