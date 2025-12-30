import pyspark.sql.functions as f
from delta.tables import DeltaTable
from pyspark.sql import DataFrame


def cleanse_product_data(
    df: DataFrame
    ) -> DataFrame:
    """
    Removes encoding artifacts and hidden non-breaking spaces 
    from product descriptions.
    """
    return df.withColumn(
        "product_name", 
        f.regexp_replace(f.col('product_name'), r"[Â\xc2\xa0]", "")
    )


def apply_product_schema_types(
    df: DataFrame
    ) -> DataFrame:
    """Enforces financial precision and adds system metadata."""
    return (
        df.withColumn("price_per_product", f.col("price_per_product").cast("Decimal(10,2)"))
          .withColumn("processing_timestamp", f.current_timestamp())
    )


def transform_products(
    df: DataFrame
    ) -> DataFrame:
    """Orchestrator for Product transformations."""
    df = cleanse_product_data(df)
    df = apply_product_schema_types(df)
    return df