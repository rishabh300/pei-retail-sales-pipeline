from src.transformations.customer_transforms import enrich_customer_metadata
from datetime import datetime


def test_enrich_customer_metadata_all(spark):
    """Unified test for enrich_customer_metadata to cover ID casting and timestamp addition."""

    test_data = [
        ("1", "John Doe", "5551234567"),
        ("100", "Jane Smith", "5559876543"),
        ("999", "Bob Johnson", "5551111111"),
        ("0", "Alice Brown", "5552222222"),
        ("12345", "Charlie Wilson", "5553333333"),
    ]
    
    schema = "customer_id STRING, customer_name STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)
    
    result_df = enrich_customer_metadata(df)
    results = result_df.collect()
    

    # Assert customer_id type is string
    assert result_df.schema["customer_id"].dataType.typeName() == "string"
    
    # Assert customer_ids are preserved as strings
    assert results[0]["customer_id"] == "1"
    assert results[1]["customer_id"] == "100"
    assert results[2]["customer_id"] == "999"
    assert results[3]["customer_id"] == "0"
    assert results[4]["customer_id"] == "12345"
    

    # Assert processing_timestamp column exists
    assert "processing_timestamp" in result_df.columns
    assert result_df.schema["processing_timestamp"].dataType.typeName() == "timestamp"
    
    # Assert rows have processing_timestamp
    for result in results:
        assert result["processing_timestamp"] is not None
        assert isinstance(result["processing_timestamp"], datetime)
    

    # Assert customer_name is unchanged
    assert results[0]["customer_name"] == "John Doe"
    assert results[1]["customer_name"] == "Jane Smith"
    assert results[2]["customer_name"] == "Bob Johnson"
    
    # Assert phone is unchanged
    assert results[0]["phone"] == "5551234567"
    assert results[1]["phone"] == "5559876543"
    assert results[4]["phone"] == "5553333333"
    
    # Assert columns exist
    expected_columns = {"customer_id", "customer_name", "phone", "processing_timestamp"}
    actual_columns = set(result_df.columns)
    assert expected_columns.issubset(actual_columns)
    
    
