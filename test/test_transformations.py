
import pytest

# Test apply_schema_and_rename for valid case scenario
def test_apply_schema_and_rename_success(spark):
    from src.silver_pipeline import apply_schema_and_rename

    df = spark.createDataFrame([(1, "A")], ["id", "name"])

    schema_config = [
        {"source_name": "id", "name": "customer_id", "type": "int"},
        {"source_name": "name", "name": "customer_name", "type": "string"},
    ]

    result = apply_schema_and_rename(df, schema_config)

    assert "customer_id" in result.columns
    assert "customer_name" in result.columns

# Testing missing column for  apply_schema_and_rename 
def test_apply_schema_and_rename_missing_column(spark):
    from src.silver_pipeline import apply_schema_and_rename

    df = spark.createDataFrame([(1,)], ["id"])

    schema_config = [
        {"source_name": "name", "name": "customer_name", "type": "string"},
    ]

    with pytest.raises(Exception):
        apply_schema_and_rename(df, schema_config)



# Testing apply_transformations (simple cast)
def test_apply_transformations_cast(spark):
    from src.silver_pipeline import apply_transformations

    df = spark.createDataFrame([("10",)], ["value"])

    transformations = [
        {"type": "cast", "columns": {"value": "int"}}
    ]

    result = apply_transformations(df, transformations)

    assert dict(result.dtypes)["value"] == "int"

#Test apply_transformations (date format)
def test_apply_transformations_date_format(spark):
    from src.silver_pipeline import apply_transformations

    df = spark.createDataFrame([("21/08/2016",)], ["order_date"])

    transformations = [
        {
            "type": "cast",
            "columns": {
                "order_date": {"type": "date", "format": "dd/MM/yyyy"}
            }
        }
    ]

    result = apply_transformations(df, transformations)

    assert dict(result.dtypes)["order_date"] == "date"


#Test apply_deduplication
def test_apply_deduplication(spark):
    from src.silver_pipeline import apply_deduplication

    data = [
        (1, "2024-01-01"),
        (1, "2024-02-01")
    ]

    df = spark.createDataFrame(data, ["id", "date"])

    config = {
        "keys": ["id"],
        "order_by": "date"
    }

    result = apply_deduplication(df, config)

    assert result.count() == 1

#. Test select_columns
def test_select_columns(spark):
    from src.silver_pipeline import select_columns

    df = spark.createDataFrame([(1, "A")], ["id", "name"])

    result = select_columns(df, ["id"])

    assert result.columns == ["id"]

#Test apply_quality_checks (not_null)
def test_apply_quality_checks_not_null(spark):
    from src.silver_pipeline import apply_quality_checks

    df = spark.createDataFrame([(1,), (None,)], ["id"])

    rules = [{"column": "id", "rule": "not_null"}]

    result = apply_quality_checks(df, rules)

    assert result.count() == 1

#Test apply_quality_checks (custom condition)
def test_apply_quality_checks_condition(spark):
    from src.silver_pipeline import apply_quality_checks

    df = spark.createDataFrame([(1,), (5,), (10,)], ["value"])

    rules = [{"column": "value", "rule": "> 5"}]

    result = apply_quality_checks(df, rules)

    values = [row["value"] for row in result.collect()]

    assert values == [10]

#