import datetime as dt

from pyspark.sql import functions as F

from src.silver import (
    build_silver,
    complete_grid,
    forward_fill_oil,
    holiday_flags,
    scope,
)


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

def test_holiday_flags_one_row_per_date_and_filters(spark):
    d = dt.date
    hol = spark.createDataFrame([
        (d(2017, 1, 1), "Holiday", "National", "Ecuador", "Primer dia", False),
        (d(2017, 1, 1), "Holiday", "Local", "Quito", "Local thing", False),     # duplicate date
        (d(2017, 1, 2), "Holiday", "Local", "Cuenca", "Fundacion", False),      # local only
        (d(2017, 1, 3), "Holiday", "National", "Ecuador", "Moved", True),       # transferred -> ignored
        (d(2017, 1, 4), "Work Day", "National", "Ecuador", "Recupero", False),  # work day -> ignored
        (d(2017, 1, 5), "Event", "National", "Ecuador", "Terremoto", False),    # event -> ignored
    ], "date date, type string, locale string, locale_name string, description string, transferred boolean")
    out = {r["date"]: (r["national_holiday"], r["any_holiday"]) for r in holiday_flags(hol).collect()}
    assert out == {d(2017, 1, 1): (1, 1), d(2017, 1, 2): (0, 1)}

def test_build_silver_keeps_grid_row_count_and_fills(spark):
    d = dt.date
    sales = spark.createDataFrame([(1, "A", d(2017, 1, 1), 5.0, 0)],
                                  "store_nbr int, family string, date date, sales double, onpromotion int")
    stores = spark.createDataFrame([(1, "Quito", "Pichincha", "D", 13)],
                                   "store_nbr int, city string, state string, type string, cluster int")
    oil = spark.createDataFrame([(d(2017, 1, 1), 50.0)], "date date, dcoilwtico double")
    hol = spark.createDataFrame([(d(2017, 1, 2), "Holiday", "National", "Ecuador", "x", False)],
                                "date date, type string, locale string, locale_name string, description string, transferred boolean")
    tx = spark.createDataFrame([(d(2017, 1, 1), 1, 100)], "date date, store_nbr int, transactions int")

    out = build_silver(sales, stores, oil, hol, tx, "2017-01-01", "2017-01-03").orderBy("date").collect()
    assert len(out) == 3
    assert [r["oil_price"] for r in out] == [50.0, 50.0, 50.0]
    assert [r["national_holiday"] for r in out] == [0, 1, 0]
    assert [r["transactions"] for r in out] == [100, 0, 0]
    assert out[0]["city"] == "Quito"


def test_scope_filters_both_keys(spark):
    df = spark.createDataFrame([(1, "A"), (1, "B"), (2, "A")], "store_nbr int, family string")
    assert scope(df, [1], ["A"]).count() == 1