from pyspark.sql import Row
import pytest
from src.transformations.product_transforms import transform_products, upsert_product
from decimal import Decimal


def test_product_name_cleansing_removes_artifacts(spark):
    """
    Verifies that 'Â' and '\xa0' are removed from product names.
    """

    test_data = [
        ("P1", "OrganicÂ Apples\xa0", "10"), 
        ("P2", "ÂProduct Name\xa0", "20")
    ]

    schema = "product_id string, product_name string, price_per_product string"
    df = spark.createDataFrame(test_data, schema=schema)

    result_df = transform_products(df)

    results = {row["product_id"]: row["product_name"] for row in result_df.collect()}
    assert results["P1"] == "Organic Apples"
    assert results["P2"] == "Product Name"


def test_product_price_casting(spark):
    """
    Verifies that prices are cast to Decimal(10,2) and handled correctly.
    """

    test_data = [
        ("P1", "Organic Apples", "10.555"), 
        ("P2", "Mango", "5"),               
        ("P3", "SG Cricket bats", "99.99")   
    ]

    df = spark.createDataFrame(test_data, ["product_id", "product_name", "price_per_product"])

   
    result_df = transform_products(df)
    
 
    results = {row["product_id"]: row["price_per_product"] for row in result_df.collect()}
    
    assert results["P1"] == Decimal("10.56")
    assert results["P2"] == Decimal("5.00")
    assert results["P3"] == Decimal("99.99")


def test_product_transformation_handle_nulls(spark):

    test_data = [("P_NULL", None, None)]
    schema = "product_id string, product_name string, price_per_product string"
    df = spark.createDataFrame(test_data, schema=schema)


    result_df = transform_products(df)

    row = result_df.first()
    assert row["product_name"] == None
    assert row["price_per_product"] == None