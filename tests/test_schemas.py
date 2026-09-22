from pyspark.sql import types as T

from src.schemas import SCHEMAS


def test_all_five_tables_present():
    assert set(SCHEMAS) == {"sales", "stores", "oil", "holidays", "transactions"}


def test_sales_schema_types():
    _, schema = SCHEMAS["sales"]
    assert schema["date"].dataType == T.DateType()
    assert schema["sales"].dataType == T.DoubleType()
    assert schema["store_nbr"].dataType == T.IntegerType()


def test_holidays_transferred_is_boolean():
    _, schema = SCHEMAS["holidays"]
    assert schema["transferred"].dataType == T.BooleanType()


def test_csv_column_order_matches_schema():
    """Schema column order must match the CSV header, otherwise Spark maps columns by position."""
    headers = {
        "sales": "id,date,store_nbr,family,sales,onpromotion",
        "stores": "store_nbr,city,state,type,cluster",
        "oil": "date,dcoilwtico",
        "holidays": "date,type,locale,locale_name,description,transferred",
        "transactions": "date,store_nbr,transactions",
    }
    for name, (_, schema) in SCHEMAS.items():
        assert ",".join(schema.fieldNames()) == headers[name]