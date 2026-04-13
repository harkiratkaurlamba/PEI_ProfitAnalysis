# Test get_reader() valid forma
def test_get_reader_valid():
    from src.bronze_pipeline import get_reader

    reader = get_reader("csv")
    assert reader is not None

#Tst handle_pii() invalid format
def test_handle_pii_hash(spark):
    from src.bronze_pipeline import handle_pii

    df = spark.createDataFrame(
        [("abc@test.com",)], ["email"]
    )

    config = {
        "pii_columns": ["email"],
        "pii_strategy": "hash"
    }

    result = handle_pii(df, config)

    assert "email" not in result.columns
    assert "email_hash" in result.columns

#Test read_csv() basic load
def test_read_csv(spark, tmp_path):
    from src.bronze_pipeline import read_csv

    file_path = tmp_path / "test.csv"

    # create test file
    with open(file_path, "w") as f:
        f.write("id,name\n1,Alice")

    config = {
        "source_path": str(file_path),
        "options": {"header": "true"}
    }

    df = read_csv(spark, config)

    assert df.count() == 1
    assert set(df.columns) == {"id", "name"}