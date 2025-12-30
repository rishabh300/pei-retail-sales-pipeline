from src.transformations.customer_transforms import standardize_customer_names


def test_standardize_names_basic(spark):
    """Test basic name standardization with simple single and names."""
    test_data = [
        ("C-1", "John Doe"),
        ("C-2", "Alice Smith"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    assert len(results) == 2
    # Name should not change
    assert results[0]["customer_name"] == "John Doe"
    assert results[1]["customer_name"] == "Alice Smith"



def test_standardize_names_with_leading_trailing_spaces(spark):
    """Test trimming of leading and trailing whitespace."""
    test_data = [
        ("C-1", "  John Doe  "),
        ("C-2", "   Jane Wilson   "),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    # leading and trailing whitespaces should be trimmed
    assert results[0]["customer_name"] == "John Doe"
    assert results[1]["customer_name"] == "Jane Wilson"



def test_standardize_names_with_multiple_internal_spaces(spark):
    """Test collapsing multiple internal spaces to single space."""
    test_data = [
        ("C-1", "John    Doe"),
        ("C-2", "Alice  Brown"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    # Name part should have a single whitespace
    assert results[0]["customer_name"] == "John Doe"
    assert results[1]["customer_name"] == "Alice Brown"



def test_standardize_names_with_special_characters(spark):
    """Test removal of special characters from names."""
    test_data = [
        ("C-1", "John-123 O'Brien"),
        ("C-2", "M@ry Johnson"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    # Non alphanumeric characters should be removed
    assert results[0]["customer_name"] == "John OBrien"
    assert results[1]["customer_name"] == "Mry Johnson"



def test_standardize_names_single_name(spark):
    """Test handling of single names (no last name)."""
    test_data = [
        ("C-1", "Madonna"),
        ("C-2", "Prince"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    # Single name should remain intact
    assert results[0]["customer_name"] == "Madonna"
    assert results[1]["customer_name"] == "Prince"


def test_standardize_names_mixed_case(spark):
    """Test that alphabetic characters are preserved in mixed case."""
    test_data = [
        ("C-1", "jOhN dOe"),
        ("C-2", "aLiCe SmItH"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    # standarized named using inticap
    assert results[0]["customer_name"] == "John Doe"
    assert results[1]["customer_name"] == "Alice Smith"



def test_standardize_names_with_numbers_in_name(spark):
    """Test removal of numeric characters from names."""
    test_data = [
        ("C-1", "John2 Doe3"),
        ("C-2", "123 Jane 456 Brown"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    # Numbers should be removed
    assert results[0]["customer_name"] == "John Doe"
    assert results[1]["customer_name"] == "Jane Brown"



def test_standardize_names_with_three_part_names(spark):
    """Test handling of three-part names (only first two parts extracted)."""
    test_data = [
        ("C-1", "Juan Carlos Garcia"),
        ("C-2", "Anna Maria Rossi"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    # name should consist of first and last name
    assert results[0]["customer_name"] == "Juan Carlos"
    assert results[1]["customer_name"] == "Anna Maria"



def test_standardize_names_null_customer_name(spark):
    """Test handling of null customer names."""
    test_data = [
        ("C-1", None),
        ("C-2", "John Doe"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    # Assert First row has null customer_name
    assert results[0]["customer_name"] is None
    # Assert Second row should be normal
    assert results[1]["customer_name"] == "John Doe"



def test_standardize_names_empty_string(spark):
    """Test handling of empty string names."""
    test_data = [
        ("C-1", ""),
        ("C-2", "John Doe"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    # works with empty names
    assert results[0]["customer_name"] is None
    assert results[1]["customer_name"] == "John Doe"



def test_standardize_names_only_special_characters(spark):
    """Test names that contain only special characters."""
    test_data = [
        ("C-1", "!@#$ %^&*"),
        ("C-2", "John Doe"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    assert results[0]["customer_name"] is None
    assert results[1]["customer_name"] == "John Doe"



def test_standardize_names_only_numbers(spark):
    """Test names that contain only numeric characters."""
    test_data = [
        ("C-1", "123 456"),
        ("C-2", "John Doe"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    # All numbers should be removed, resulting in None
    assert results[0]["customer_name"] is None
    assert results[1]["customer_name"] == "John Doe"



def test_standardize_names_only_whitespace(spark):
    """Test names that contain only whitespace."""
    test_data = [
        ("C-1", "     "),
        ("C-2", "John Doe"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    # Whitespace should be trimmed, resulting in None
    assert results[0]["customer_name"] is None
    assert results[1]["customer_name"] == "John Doe"



def test_standardize_names_unicode_characters(spark):
    """Test handling of unicode characters (non-ASCII letters)."""
    test_data = [
        ("C-1", "José García"),
        ("C-2", "François Müller"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    # Unicode characters should be removed as they don't match regex pattern
    assert results[0]["customer_name"] == "Jos Garca"
    assert results[1]["customer_name"] == "Franois Mller"


def test_standardize_names_with_dots_interrupting_words(spark):
    """Test handling of names where dots interrupt word continuity like 'Ad..am Hart' or 'A.d.a.m'."""
    test_data = [
        ("C-1", "Ad..am Hart"),
        ("C-2", "Jo...hn Sm...ith"),
        ("C-3", "A.d.a.m Jo.h.n"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    # Dots within words removed, preserving word structure, taking first 2 parts
    assert results[0]["customer_name"] == "Adam Hart"
    assert results[1]["customer_name"] == "John Smith"
    assert results[2]["customer_name"] == "Adam John"


def test_standardize_names_prevents_blank_results(spark):
    """Test that blank customer names are not created."""
    test_data = [
        ("C-1", "!!!"),
        ("C-2", "John Doe"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    # All special characters removed should result in None, not blank string
    assert results[0]["customer_name"] is None
    assert results[1]["customer_name"] == "John Doe"



def test_standardize_names_with_mixed_special_chars_and_spaces(spark):
    """Test names with interleaved special characters and spaces."""
    test_data = [
        ("C-1", "A-d-a-m  H-a-r-t"),
        ("C-2", "M@r!a G@rc!a"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    assert results[0]["customer_name"] == "Adam Hart"
    assert results[1]["customer_name"] == "Mra Grca"



def test_standardize_names_large_special_chars(spark):
    """Test names dominated by special characters."""
    test_data = [
        ("C-1", "!!!John!!!Smith!!!"),
        ("C-2", "---Mary---Williams---"),
    ]
    schema = "customer_id STRING, customer_name STRING"
    df = spark.createDataFrame(test_data, schema)

    result_df = standardize_customer_names(df)
    results = result_df.collect()

    assert results[0]["customer_name"] == "JohnSmith"
    assert results[1]["customer_name"] == "MaryWilliams"
