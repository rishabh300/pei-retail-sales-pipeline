from src.transformations.customer_transforms import validate_phone_numbers



def test_validate_phone_basic_valid_10_digits(spark):
    """Test validation of valid 10-digit phone numbers."""
    test_data = [
        ("C-1", "1234567890"),
        ("C-2", "9876543210"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    assert len(results) == 2
    assert results[0]["phone"] == "1234567890"
    assert results[1]["phone"] == "9876543210"


def test_validate_phone_valid_15_digits(spark):
    """Test validation of valid 15-digit phone numbers."""
    test_data = [
        ("C-1", "123456789012345"),
        ("C-2", "987654321098765"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    assert results[0]["phone"] == "123456789012345"
    assert results[1]["phone"] == "987654321098765"


def test_validate_phone_with_special_characters(spark):
    """Test that special characters are removed and digits are validated."""
    test_data = [
        ("C-1", "(123) 456-7890"),
        ("C-2", "+1-555-123-4567"),
        ("C-3", "001-541-754-3010"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    # Special characters removed, digits extracted
    assert results[0]["phone"] == "1234567890"
    assert results[1]["phone"] == "15551234567"
    assert results[2]["phone"] == "0015417543010"


def test_validate_phone_with_spaces(spark):
    """Test that spaces are removed from phone numbers."""
    test_data = [
        ("C-1", "123 456 7890"),
        ("C-2", "555 123 4567"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    assert results[0]["phone"] == "1234567890"
    assert results[1]["phone"] == "5551234567"


def test_validate_phone_too_short(spark):
    """Test that phone numbers with < 10 digits are rejected."""
    test_data = [
        ("C-1", "123456789"),      # 9 digits
        ("C-2", "12345"),           # 5 digits
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    assert results[0]["phone"] is None
    assert results[1]["phone"] is None


def test_validate_phone_too_long(spark):
    """Test that phone numbers with > 15 digits are rejected."""
    test_data = [
        ("C-1", "1234567890123456"),  
        ("C-2", "12345678901234567890"),  
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    assert results[0]["phone"] is None
    assert results[1]["phone"] is None


def test_validate_phone_null_phone(spark):
    """Test handling of null phone numbers."""
    test_data = [
        ("C-1", None),
        ("C-2", "1234567890"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    assert results[0]["phone"] is None
    assert results[1]["phone"] == "1234567890"


def test_validate_phone_empty_string(spark):
    """Test handling of empty string phone numbers."""
    test_data = [
        ("C-1", ""),
        ("C-2", "1234567890"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    assert results[0]["phone"] is None
    assert results[1]["phone"] == "1234567890"


def test_validate_phone_only_special_characters(spark):
    """Test phone numbers containing only special characters."""
    test_data = [
        ("C-1", "()-+."),
        ("C-2", "1234567890"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    # No digits extracted, should be None
    assert results[0]["phone"] is None
    assert results[1]["phone"] == "1234567890"


def test_validate_phone_only_letters(spark):
    """Test phone numbers containing only letters."""
    test_data = [
        ("C-1", "abcdefghij"),
        ("C-2", "1234567890"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    # Letters don't contain digits, should be None
    assert results[0]["phone"] is None
    assert results[1]["phone"] == "1234567890"


def test_validate_phone_with_letters_and_digits(spark):
    """Test phone numbers with mixed letters and digits."""
    test_data = [
        ("C-1", "123abc456def7890"),
        ("C-2", "1234567890"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    # Only digits extracted
    assert results[0]["phone"] == "1234567890"
    assert results[1]["phone"] == "1234567890"


def test_validate_phone_boundary_9_digits(spark):
    """Test boundary case: 9 digits (just below minimum)."""
    test_data = [
        ("C-1", "123456789"),
        ("C-2", "1234567890"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    assert results[0]["phone"] is None
    assert results[1]["phone"] == "1234567890"


def test_validate_phone_boundary_16_digits(spark):
    """Test boundary case: 16 digits (just above maximum)."""
    test_data = [
        ("C-1", "1234567890123456"),
        ("C-2", "123456789012345"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    assert results[0]["phone"] is None
    assert results[1]["phone"] == "123456789012345"


def test_validate_phone_leading_zeros(spark):
    """Test phone numbers with leading zeros."""
    test_data = [
        ("C-1", "0012345678901"),
        ("C-2", "001-541-754-3010"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    # Leading zeros are preserved, digits extracted
    assert results[0]["phone"] == "0012345678901"
    assert results[1]["phone"] == "0015417543010"


def test_validate_phone_plus_sign_prefix(spark):
    """Test international phone format with + prefix."""
    test_data = [
        ("C-1", "+1 (555) 123-4567"),
        ("C-2", "+44 20 7946 0958"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    # + sign removed, digits extracted
    assert results[0]["phone"] == "15551234567"
    assert results[1]["phone"] == "442079460958"


def test_validate_phone_batch_processing(spark):
    """Test processing multiple rows with mixed valid/invalid phone numbers."""
    test_data = [
        ("C-1", "1234567890"),
        ("C-2", "123"),
        ("C-3", "(555) 123-4567"),
        ("C-4", "12345678901234567890"),
        ("C-5", "555-666-7777"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    assert len(results) == 5
    assert results[0]["phone"] == "1234567890"
    assert results[1]["phone"] is None
    assert results[2]["phone"] == "5551234567"
    assert results[3]["phone"] is None
    assert results[4]["phone"] == "5556667777"


def test_validate_phone_preserves_column_count(spark):
    """Test that the function doesn't lose original columns."""
    test_data = [
        ("C-1", "1234567890"),
        ("C-2", "5551234567"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    
    # Check that expected columns exist
    expected_columns = {"customer_id", "phone"}
    actual_columns = set(result_df.columns)
    
    assert expected_columns.issubset(actual_columns), \
        f"Missing columns. Expected {expected_columns} but got {actual_columns}"


def test_validate_phone_extension_format(spark):
    """Test phone numbers with extensions."""
    test_data = [
        ("C-1", "555-123-4567 ext 123"),
        ("C-2", "555-123-4567 x456"),
    ]
    schema = "customer_id STRING, phone STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = validate_phone_numbers(df)
    results = result_df.collect()

    # Extensions are numbers, so they get included (13 and 14 digits respectively)
    assert results[0]["phone"] == "5551234567123"
    assert results[1]["phone"] == "5551234567456"
