from src.transformations.order_transforms import apply_order_schema_types
from datetime import datetime


def test_schema_types_basic_conversion(spark):
    """Test basic schema type conversions for all columns."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", "10", "100.00", "0.10", "50.00"),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_schema_types(df)
    results = result_df.collect()

    # Check data types
    assert result_df.schema["row_id"].dataType.typeName() == "integer"
    assert result_df.schema["quantity"].dataType.typeName() == "integer"
    assert result_df.schema["price"].dataType.typeName() == "decimal"  # decimal(10,2)
    assert result_df.schema["discount"].dataType.typeName() == "decimal"  # decimal(5,2)
    assert result_df.schema["profit"].dataType.typeName() == "decimal"  # decimal(10,2)
    assert result_df.schema["order_date"].dataType.typeName() == "timestamp"
    assert result_df.schema["ship_date"].dataType.typeName() == "timestamp"


def test_schema_types_date_parsing(spark):
    """Test correct parsing of date strings to timestamps."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "15/06/2025", "20/06/2025", "5", "99.99", "0.15", "45.50"),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_schema_types(df)
    results = result_df.collect()

    # Verify date parsing
    order_date = results[0]["order_date"]
    ship_date = results[0]["ship_date"]
    
    assert order_date.year == 2025
    assert order_date.month == 6
    assert order_date.day == 15
    
    assert ship_date.year == 2025
    assert ship_date.month == 6
    assert ship_date.day == 20


def test_schema_types_integer_conversion(spark):
    """Test conversion of row_id and quantity to integers."""
    test_data = [
        ("42", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", "100", "100.00", "0.10", "50.00"),
        ("99", "O-2", "P-2", "C-2", "02/01/2025", "06/01/2025", "5", "50.00", "0.20", "25.00"),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_schema_types(df)
    results = result_df.collect()

    assert results[0]["row_id"] == 42
    assert results[0]["quantity"] == 100
    assert results[1]["row_id"] == 99
    assert results[1]["quantity"] == 5


def test_schema_types_decimal_conversion(spark):
    """Test conversion of numeric columns to decimal with correct precision."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", "10", "123.45", "0.99", "67.89"),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_schema_types(df)
    results = result_df.collect()

    # Verify decimal values
    assert float(results[0]["price"]) == 123.45
    assert float(results[0]["discount"]) == 0.99
    assert float(results[0]["profit"]) == 67.89


def test_schema_types_adds_processing_timestamp(spark):
    """Test that processing_timestamp column is added."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", "10", "100.00", "0.10", "50.00"),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_schema_types(df)
    
    # Check that processing_timestamp column exists
    assert "processing_timestamp" in result_df.columns
    assert result_df.schema["processing_timestamp"].dataType.typeName() == "timestamp"


def test_schema_types_batch_processing(spark):
    """Test batch processing of multiple rows."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", "10", "100.00", "0.10", "50.00"),
        ("2", "O-2", "P-2", "C-2", "02/02/2025", "06/02/2025", "20", "200.00", "0.20", "100.00"),
        ("3", "O-3", "P-3", "C-3", "03/03/2025", "07/03/2025", "30", "300.00", "0.30", "150.00"),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_schema_types(df)
    results = result_df.collect()

    assert len(results) == 3
    assert results[0]["row_id"] == 1
    assert results[1]["row_id"] == 2
    assert results[2]["row_id"] == 3


def test_schema_types_preserves_original_columns(spark):
    """Test that all original columns are preserved."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", "10", "100.00", "0.10", "50.00"),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_schema_types(df)
    
    expected_columns = {"row_id", "order_id", "product_id", "customer_id", "order_date", "ship_date", "quantity", "price", "discount", "profit"}
    actual_columns = set(result_df.columns)
    
    assert expected_columns.issubset(actual_columns)



def test_schema_types_zero_values(spark):
    """Test handling of zero values for numeric columns."""
    test_data = [
        ("0", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", "0", "0.00", "0.00", "0.00"),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_schema_types(df)
    results = result_df.collect()

    assert results[0]["row_id"] == 0
    assert results[0]["quantity"] == 0
    assert float(results[0]["price"]) == 0.00
    assert float(results[0]["discount"]) == 0.00
    assert float(results[0]["profit"]) == 0.00


def test_schema_types_large_values(spark):
    """Test handling of large numeric values."""
    test_data = [
        ("999999", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", "9999", "9999999.99", "99.99", "9999999.99"),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_schema_types(df)
    results = result_df.collect()

    assert results[0]["row_id"] == 999999
    assert results[0]["quantity"] == 9999
    assert float(results[0]["price"]) == 9999999.99


def test_schema_types_different_date_formats(spark):
    """Test date parsing with various valid date formats (d/M/yyyy)."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "1/1/2025", "5/1/2025", "10", "100.00", "0.10", "50.00"),
        ("2", "O-2", "P-2", "C-2", "31/12/2025", "31/12/2025", "20", "200.00", "0.20", "100.00"),
        ("3", "O-3", "P-3", "C-3", "29/2/2024", "1/3/2024", "30", "300.00", "0.30", "150.00"),  # Leap year
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_schema_types(df)
    results = result_df.collect()

    # Verify parsing
    assert results[0]["order_date"].day == 1
    assert results[0]["order_date"].month == 1
    
    assert results[1]["order_date"].day == 31
    assert results[1]["order_date"].month == 12
    
    assert results[2]["order_date"].day == 29
    assert results[2]["order_date"].month == 2


def test_schema_types_negative_profit(spark):
    """Test handling of negative profit values."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", "10", "100.00", "0.10", "-50.00"),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_schema_types(df)
    results = result_df.collect()

    assert float(results[0]["profit"]) == -50.00


def test_schema_types_decimal_precision(spark):
    """Test that decimal precision is maintained correctly."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", "10", "123.456", "0.125", "45.678"),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_schema_types(df)
    results = result_df.collect()

    # Verify decimal(10,2) precision for price and profit (rounds to 2 decimals)
    assert float(results[0]["price"]) == 123.46  # Rounded from 123.456
    # decimal(5,2) for discount - banker's rounding rounds 0.125 to 0.13
    assert float(results[0]["discount"]) == 0.13  # Rounded from 0.125 (banker's rounding)


def test_schema_types_timestamp_includes_time(spark):
    """Test that timestamps are created with time component."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", "10", "100.00", "0.10", "50.00"),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_schema_types(df)
    results = result_df.collect()

    # Check that processing_timestamp has been populated
    assert results[0]["processing_timestamp"] is not None
    assert isinstance(results[0]["processing_timestamp"], datetime)


def test_schema_types_processing_timestamp_recent(spark):
    """Test that processing_timestamp is set to current time."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", "10", "100.00", "0.10", "50.00"),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    before = datetime.now()
    result_df = apply_order_schema_types(df)
    results = result_df.collect()
    after = datetime.now()

    processing_time = results[0]["processing_timestamp"]
    # Allow 5 seconds buffer for test execution
    assert before.replace(microsecond=0) <= processing_time <= after.replace(microsecond=0)


def test_schema_types_single_decimal_discount(spark):
    """Test discount with single decimal place."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", "10", "100.00", "0.5", "50.00"),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_schema_types(df)
    results = result_df.collect()

    assert float(results[0]["discount"]) == 0.50
