from src.transformations.order_transforms import transform_orders


from datetime import datetime
from decimal import Decimal

def test_transform_orders(spark):
    # 1. Arrange
    data = [(
        "1", "ORD-123", "PROD-99", "CUST-01", 
        "25/12/2025", "27/12/2025", "10", "150.50", "0.15", "45.00"
    )]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, " \
             "order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(data, schema)

    # 2. Act
    result_df = transform_orders(df)
    row = result_df.first()

    # 3. Assert
    assert isinstance(row["order_date"], datetime)
    assert row["year_month"] == "2025-12"         
    assert row["quantity"] == 10                  
    assert row["price"] == Decimal("150.50")      
    assert row["is_critical"] == False         
    assert row["quarantine_reason"] == ""      
      

def test_transform_orders_data_quality_failures(spark):
    # 1. Arrange: 
    test_data = [
        # Missing Order ID
        ("2", None, "P-1", "C-1", "01/01/2025", "02/01/2025", "1", "10", "0", "1"),
        # Invalid Dates (Ship before Order)
        ("3", "O-2", "P-1", "C-1", "10/01/2025", "05/01/2025", "1", "10", "0", "1"),
        # Multiple failures
        ("4", None, None, "C-1", "01/01/2025", "02/01/2025", "1", "10", "0", "1")
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, " \
             "order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    # 2. Act
    result_df = transform_orders(df)
    results = {row["row_id"]: row for row in result_df.collect()}

    # 3. Assert - use integer keys since row_id is cast to integer in transform_orders
    assert results[2]["is_critical"] is True
    assert "Missing Order ID" in results[2]["quarantine_reason"]

    # Test Date Logic Failure
    assert results[3]["is_critical"] is True
    assert "Invalid Dates" in results[3]["quarantine_reason"]

    # Test Concatenation of multiple reasons
    assert "Missing Order ID" in results[4]["quarantine_reason"]
    assert "Missing Product ID" in results[4]["quarantine_reason"]



def test_transform_orders_warnings_and_nulls(spark):
    # 1. Arrange
    test_data = [
        # Discount > 1.0 (150%)
        ("5", "O-3", "P-1", "C-1", "01/01/2025", "02/01/2025", "1", "10", "1.5", "1"),
        # Null values in numeric columns
        ("6", "O-4", "P-1", "C-1", "01/01/2025", "02/01/2025", None, None, None, None)
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, " \
             "order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    # 2. Act
    result_df = transform_orders(df)
    results = {row["row_id"]: row for row in result_df.collect()}

    # 3. Assert
    assert results[5]["is_warning"] == True
   
    assert results[6]["quantity"] == None
    assert results[6]["price"] == None