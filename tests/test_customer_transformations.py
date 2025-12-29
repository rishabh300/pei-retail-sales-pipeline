from src.transformations.customer_transforms import transform_customers, upsert_customer
from pyspark.sql import Row
import pytest


def test_customer_phone_length_validation(spark): 
    test_rows = [
        Row(customer_id="1", customer_name="foo bar", phone="1234567890")
    ]

    df = spark.createDataFrame(test_rows, ["customer_id", "customer_name", "phone"])

    result_df = transform_customers(df)

    result_rows = result_df.collect()
    assert result_rows[0]["phone"] == "1234567890"


def test_customer_phone_length_not_in_range(spark): 
    test_rows = [
        Row(customer_id="1", customer_name="foo bar", phone="12345678"),
        Row(customer_id="2", customer_name="foo1 bar1", phone="123456784647484345")
    ]

    df = spark.createDataFrame(test_rows, ["customer_id", "customer_name", "phone"])

    result_df = transform_customers(df)

    result_rows = result_df.collect()
    assert result_rows[0]["phone"] == None
    assert result_rows[1]["phone"] == None


def test_customer_phone_length_handles_None(spark): 
    test_rows = [
        Row(customer_id="1", customer_name="foo bar", phone=None)
    ]

    schema = "customer_id STRING, customer_name STRING, phone STRING"
    df = spark.createDataFrame(test_rows, schema=schema)

    result_df = transform_customers(df)

    result_rows = result_df.collect()
    assert result_rows[0]["phone"] == None


def test_customer_name_whitespace_standardization(spark):
    test_data = [
        ("1", "  Foo Bar  ", "1234567890"),       # Scenario: Leading/Trailing
        ("2", "Foo    Bar", "1234567890"),        # Scenario: Multiple Internal
        ("3", "  Foo   Bar  ", "1234567890"),     # Scenario: Both
        ("4", None, "1234567890"),                # Scenario: Null Handling
        ("5", "FooBar", "1234567890")             # Scenario: Already clean
    ]
    df = spark.createDataFrame(test_data, ["customer_id", "customer_name", "phone"])

    result_df = transform_customers(df)
    
    results = {row["customer_id"]: row["customer_name"] for row in result_df.collect()}

    assert results["1"] == "Foo Bar"
    assert results["2"] == "Foo Bar"
    assert results["3"] == "Foo Bar"
    assert results["4"] == None
    assert results["5"] == "FooBar"


@pytest.mark.skip(reason="Delta Lake JARs not configured in Spark")
def test_upsert_customer_idempotency(spark, tmp_path):

    table_path = str(tmp_path / "silver_customers")
    table_name = f"delta.`{table_path}`"
    
    initial_data = [Row(customer_id="1", customer_name="Old Name", phone="111")]
    spark.createDataFrame(initial_data).write.format("delta").save(table_path)

    batch_data = [
        Row(customer_id="1", customer_name="New Name", phone="111"),
        Row(customer_id="2", customer_name="New Customer", phone="222")
    ]
    batch_df = spark.createDataFrame(batch_data)

    upsert_customer(spark, batch_df, table_name)

    result_df = spark.read.format("delta").load(table_path)
    results = {row["customer_id"]: row for row in result_df.collect()}


    assert results["1"]["customer_name"] == "New Name"

    assert "2" in results
    assert results["2"]["customer_name"] == "New Customer"

    assert result_df.count() == 2