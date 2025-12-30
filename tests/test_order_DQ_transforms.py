from src.transformations.order_transforms import apply_order_quality_rules



def test_quality_rules_all_valid_data(spark):
    """Test that valid orders are not flagged as critical."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", 10, 100.00, 0.1, 50.00),
        ("2", "O-2", "P-2", "C-2", "02/01/2025", "06/01/2025", 5, 50.00, 0.2, 25.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    results = result_df.collect()

    assert len(results) == 2
    assert results[0]["is_critical"] is False
    assert results[0]["is_warning"] is False
    assert results[0]["quarantine_reason"] == ""  # Empty string when no issues
    assert results[1]["is_critical"] is False
    assert results[1]["is_warning"] is False


def test_quality_rules_valid_discount_range(spark):
    """Test valid discount values between 0.0 and 1.0."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", 10, 100.00, 0.0, 50.00),
        ("2", "O-2", "P-2", "C-2", "02/01/2025", "06/01/2025", 5, 50.00, 0.5, 25.00),
        ("3", "O-3", "P-3", "C-3", "03/01/2025", "07/01/2025", 8, 75.00, 1.0, 30.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    results = result_df.collect()

    # All should have is_warning = False
    for result in results:
        assert result["is_warning"] is False


def test_quality_rules_same_order_and_ship_date(spark):
    """Test that same order and ship dates are valid."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "01/01/2025", 10, 100.00, 0.1, 50.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    results = result_df.collect()

    assert results[0]["is_critical"] is False
    assert results[0]["quarantine_reason"] == ""  # Empty string when no issues


def test_quality_rules_batch_processing(spark):
    """Test processing multiple valid orders."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", 10, 100.00, 0.1, 50.00),
        ("2", "O-2", "P-2", "C-2", "02/01/2025", "06/01/2025", 5, 50.00, 0.2, 25.00),
        ("3", "O-3", "P-3", "C-3", "03/01/2025", "07/01/2025", 8, 75.00, 0.15, 35.00),
        ("4", "O-4", "P-4", "C-4", "04/01/2025", "08/01/2025", 12, 120.00, 0.05, 60.00),
        ("5", "O-5", "P-5", "C-5", "05/01/2025", "09/01/2025", 6, 80.00, 0.25, 40.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    results = result_df.collect()

    assert len(results) == 5
    for result in results:
        assert result["is_critical"] is False


def test_quality_rules_missing_order_id(spark):
    """Test that missing order_id flags as critical."""
    test_data = [
        ("1", None, "P-1", "C-1", "01/01/2025", "05/01/2025", 10, 100.00, 0.1, 50.00),
        ("2", "O-2", "P-2", "C-2", "02/01/2025", "06/01/2025", 5, 50.00, 0.2, 25.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    results = result_df.collect()

    assert results[0]["is_critical"] is True
    assert "Missing Order ID" in results[0]["quarantine_reason"]
    assert results[1]["is_critical"] is False


def test_quality_rules_missing_product_id(spark):
    """Test that missing product_id flags as critical."""
    test_data = [
        ("1", "O-1", None, "C-1", "01/01/2025", "05/01/2025", 10, 100.00, 0.1, 50.00),
        ("2", "O-2", "P-2", "C-2", "02/01/2025", "06/01/2025", 5, 50.00, 0.2, 25.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    results = result_df.collect()

    assert results[0]["is_critical"] is True
    assert "Missing Product ID" in results[0]["quarantine_reason"]
    assert results[1]["is_critical"] is False


def test_quality_rules_missing_customer_id(spark):
    """Test that missing customer_id flags as critical."""
    test_data = [
        ("1", "O-1", "P-1", None, "01/01/2025", "05/01/2025", 10, 100.00, 0.1, 50.00),
        ("2", "O-2", "P-2", "C-2", "02/01/2025", "06/01/2025", 5, 50.00, 0.2, 25.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    results = result_df.collect()

    assert results[0]["is_critical"] is True
    assert "Missing Customer ID" in results[0]["quarantine_reason"]
    assert results[1]["is_critical"] is False


def test_quality_rules_invalid_dates(spark):
    """Test that ship_date before order_date flags as critical."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "05/01/2025", "01/01/2025", 10, 100.00, 0.1, 50.00),
        ("2", "O-2", "P-2", "C-2", "02/01/2025", "06/01/2025", 5, 50.00, 0.2, 25.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    results = result_df.collect()

    assert results[0]["is_critical"] is True
    assert "Invalid Dates" in results[0]["quarantine_reason"]
    assert results[1]["is_critical"] is False


def test_quality_rules_multiple_critical_issues(spark):
    """Test that multiple critical issues are all captured in quarantine_reason."""
    test_data = [
        ("1", None, None, "C-1", "05/01/2025", "01/01/2025", 10, 100.00, 0.1, 50.00),
        ("2", "O-2", "P-2", "C-2", "02/01/2025", "06/01/2025", 5, 50.00, 0.2, 25.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    results = result_df.collect()

    assert results[0]["is_critical"] is True
    # Should contain all three missing IDs and invalid dates
    reason = results[0]["quarantine_reason"]
    assert "Missing Order ID" in reason
    assert "Missing Product ID" in reason
    assert "Invalid Dates" in reason


def test_quality_rules_discount_negative(spark):
    """Test that negative discount flags as warning."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", 10, 100.00, -0.1, 50.00),
        ("2", "O-2", "P-2", "C-2", "02/01/2025", "06/01/2025", 5, 50.00, 0.2, 25.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    results = result_df.collect()

    assert results[0]["is_warning"] is True
    assert results[0]["is_critical"] is False
    assert results[1]["is_warning"] is False


def test_quality_rules_discount_exceeds_100(spark):
    """Test that discount > 1.0 (100%) flags as warning."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", 10, 100.00, 1.5, 50.00),
        ("2", "O-2", "P-2", "C-2", "02/01/2025", "06/01/2025", 5, 50.00, 0.2, 25.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    results = result_df.collect()

    assert results[0]["is_warning"] is True
    assert results[0]["is_critical"] is False
    assert results[1]["is_warning"] is False


def test_quality_rules_discount_boundary_values(spark):
    """Test boundary values for discount (0.0 and 1.0 are valid, -0.01 and 1.01 are warnings)."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", 10, 100.00, 0.0, 50.00),
        ("2", "O-2", "P-2", "C-2", "02/01/2025", "06/01/2025", 5, 50.00, 1.0, 25.00),
        ("3", "O-3", "P-3", "C-3", "03/01/2025", "07/01/2025", 8, 75.00, -0.01, 35.00),
        ("4", "O-4", "P-4", "C-4", "04/01/2025", "08/01/2025", 12, 120.00, 1.01, 60.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    results = result_df.collect()

    assert results[0]["is_warning"] is False
    assert results[1]["is_warning"] is False
    assert results[2]["is_warning"] is True
    assert results[3]["is_warning"] is True



def test_quality_rules_critical_and_warning(spark):
    """Test record with both critical and warning issues."""
    test_data = [
        ("1", None, "P-1", "C-1", "05/01/2025", "01/01/2025", 10, 100.00, -0.1, 50.00),
        ("2", "O-2", "P-2", "C-2", "02/01/2025", "06/01/2025", 5, 50.00, 0.2, 25.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    results = result_df.collect()

    # First record has both critical and warning issues
    assert results[0]["is_critical"] is True
    assert results[0]["is_warning"] is True
    assert "Missing Order ID" in results[0]["quarantine_reason"]
    assert "Invalid Dates" in results[0]["quarantine_reason"]


def test_quality_rules_preserves_columns(spark):
    """Test that the function preserves all original columns."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", 10, 100.00, 0.1, 50.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    
    # Check that original columns are preserved
    expected_columns = {"row_id", "order_id", "product_id", "customer_id", "order_date", "ship_date", "quantity", "price", "discount", "profit"}
    actual_columns = set(result_df.columns)
    
    assert expected_columns.issubset(actual_columns), \
        f"Missing columns. Expected {expected_columns} but got {actual_columns}"


def test_quality_rules_adds_required_columns(spark):
    """Test that the function adds is_critical, is_warning, and quarantine_reason columns."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", 10, 100.00, 0.1, 50.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    
    # Check that new columns exist
    expected_new_columns = {"is_critical", "is_warning", "quarantine_reason"}
    actual_columns = set(result_df.columns)
    
    assert expected_new_columns.issubset(actual_columns), \
        f"Missing new columns. Expected {expected_new_columns} but got {actual_columns}"


def test_quality_rules_null_quarantine_reason_for_valid_data(spark):
    """Test that quarantine_reason is None for valid orders."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", 10, 100.00, 0.1, 50.00),
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    results = result_df.collect()

    # quarantine_reason should be empty string when there are no issues
    assert results[0]["quarantine_reason"] == ""


def test_quality_rules_complex_scenario(spark):
    """Test complex scenario with mix of valid, warning, and critical records."""
    test_data = [
        ("1", "O-1", "P-1", "C-1", "01/01/2025", "05/01/2025", 10, 100.00, 0.1, 50.00),      # Valid
        ("2", "O-2", "P-2", "C-2", "02/01/2025", "06/01/2025", 5, 50.00, -0.1, 25.00),       # Warning
        ("3", None, "P-3", "C-3", "03/01/2025", "07/01/2025", 8, 75.00, 0.15, 35.00),        # Critical
        ("4", "O-4", None, "C-4", "04/01/2025", "08/01/2025", 12, 120.00, 1.5, 60.00),       # Critical + Warning
        ("5", "O-5", "P-5", None, "05/01/2025", "09/01/2025", 6, 80.00, 0.25, 40.00),        # Critical
        ("6", "O-6", "P-6", "C-6", "06/01/2025", "05/01/2025", 9, 90.00, 0.05, 45.00),       # Critical (invalid dates)
    ]
    schema = "row_id STRING, order_id STRING, product_id STRING, customer_id STRING, order_date STRING, ship_date STRING, quantity STRING, price STRING, discount STRING, profit STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = apply_order_quality_rules(df)
    results = result_df.collect()

    assert len(results) == 6
    
    # Valid
    assert results[0]["is_critical"] is False
    assert results[0]["is_warning"] is False
    
    # Warning only
    assert results[1]["is_critical"] is False
    assert results[1]["is_warning"] is True
    
    # Critical (missing order_id)
    assert results[2]["is_critical"] is True
    
    # Critical (missing product_id) + Warning (high discount)
    assert results[3]["is_critical"] is True
    assert results[3]["is_warning"] is True
    
    # Critical (missing customer_id)
    assert results[4]["is_critical"] is True
    
    # Critical (invalid dates)
    assert results[5]["is_critical"] is True
