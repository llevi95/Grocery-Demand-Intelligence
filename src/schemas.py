from pyspark.sql import types as T

SCHEMAS: dict[str, tuple[str, T.StructType]] = {
    "sales": ("train.csv", T.StructType([
        T.StructField("id", T.LongType()),
        T.StructField("date", T.DateType()),
        T.StructField("store_nbr", T.IntegerType()),
        T.StructField("family", T.StringType()),
        T.StructField("sales", T.DoubleType()),
        T.StructField("onpromotion", T.IntegerType()),
    ])),
    "stores": ("stores.csv", T.StructType([
        T.StructField("store_nbr", T.IntegerType()),
        T.StructField("city", T.StringType()),
        T.StructField("state", T.StringType()),
        T.StructField("type", T.StringType()),
        T.StructField("cluster", T.IntegerType()),
    ])),
    "oil": ("oil.csv", T.StructType([
        T.StructField("date", T.DateType()),
        T.StructField("dcoilwtico", T.DoubleType()),
    ])),
    "holidays": ("holidays_events.csv", T.StructType([
        T.StructField("date", T.DateType()),
        T.StructField("type", T.StringType()),
        T.StructField("locale", T.StringType()),
        T.StructField("locale_name", T.StringType()),
        T.StructField("description", T.StringType()),
        T.StructField("transferred", T.BooleanType()),
    ])),
    "transactions": ("transactions.csv", T.StructType([
        T.StructField("date", T.DateType()),
        T.StructField("store_nbr", T.IntegerType()),
        T.StructField("transactions", T.IntegerType()),
    ])),
}