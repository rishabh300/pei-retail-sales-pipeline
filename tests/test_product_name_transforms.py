from src.transformations.product_transforms import cleanse_product_data


def test_cleanse_product_data_all(spark):
    """Comprehensive test for cleanse_product_data covering encoding artifact removal and space handling."""
    
    # Test data with various encoding issues
    test_data = [
        # Normal product name (no encoding issues)
        ("1", "Basic Product", "100.00"),
        # Non-breaking space character (Â\xc2\xa0)
        ("2", "Product\xc2\xa0Name", "50.00"),
        # Multiple non-breaking spaces
        ("3", "Product\xc2\xa0With\xc2\xa0Spaces", "75.00"),
        # Encoding artifact 'Â' character
        ("4", "ÂProduct Name", "60.00"),
        # Mixed encoding artifacts
        ("5", "Â Product\xc2\xa0 Name", "85.00"),
        # Product name with special characters preserved
        ("6", "Premium-Grade Product", "95.00"),
        # Product name with numbers
        ("7", "Product 123 Name", "40.00"),
        # Product name with only encoding issue
        ("8", "\xc2\xa0", "55.00"),
        # Multiple Â artifacts
        ("9", "ÂÂProduct", "110.00"),
        # Complex mix of issues
        ("10", "Â Premium\xc2\xa0 Product Â", "99.99"),
    ]
    
    schema = "product_id STRING, product_name STRING, price_per_product STRING"
    df = spark.createDataFrame(test_data, schema)
    
    result_df = cleanse_product_data(df)
    results = result_df.collect()
    
    # =====================
    # Test 1: Normal Product Names (unchanged)
    # =====================
    assert results[0]["product_name"] == "Basic Product"
    assert results[6]["product_name"] == "Product 123 Name"
    assert results[5]["product_name"] == "Premium-Grade Product"
    
    # =====================
    # Test 2: Non-breaking Space Removal (\xc2\xa0)
    # =====================
    # Product with single non-breaking space becomes product name without space
    assert results[1]["product_name"] == "ProductName"
    
    # Multiple non-breaking spaces removed
    assert results[2]["product_name"] == "ProductWithSpaces"
    
    # =====================
    # Test 3: Encoding Artifact Removal ('Â')
    # =====================
    # Â character at beginning removed
    assert results[3]["product_name"] == "Product Name"
    
    # Multiple Â characters removed
    assert results[8]["product_name"] == "Product"
    
    # =====================
    # Test 4: Mixed Encoding Artifacts
    # =====================
    # Both Â and non-breaking spaces removed, but regular spaces preserved
    assert results[4]["product_name"] == " Product Name"  # Â removed, \xc2\xa0 removed, spaces preserved
    assert results[9]["product_name"] == " Premium Product "  # Â removed at start/end, \xc2\xa0 removed
    
    # =====================
    # Test 5: Only Encoding Issues (becomes empty/minimal)
    # =====================
    # Just non-breaking space becomes empty
    assert results[7]["product_name"] == ""
    
    # =====================
    # Test 6: Batch Processing
    # =====================
    assert len(results) == 10
    
    # Verify all product_ids are preserved
    for i, result in enumerate(results):
        assert result["product_id"] == str(i + 1)
    
    # =====================
    # Test 7: Price Column Unchanged
    # =====================
    assert results[0]["price_per_product"] == "100.00"
    assert results[1]["price_per_product"] == "50.00"
    assert results[9]["price_per_product"] == "99.99"
    
    # =====================
    # Test 8: Data Types Preserved
    # =====================
    assert result_df.schema["product_id"].dataType.typeName() == "string"
    assert result_df.schema["product_name"].dataType.typeName() == "string"
    assert result_df.schema["price_per_product"].dataType.typeName() == "string"
    
    # =====================
    # Test 9: Column Structure Preserved
    # =====================
    expected_columns = {"product_id", "product_name", "price_per_product"}
    actual_columns = set(result_df.columns)
    assert expected_columns == actual_columns
