import pyspark.sql.functions as f
from delta.tables import DeltaTable
from pyspark.sql import DataFrame


def standardize_customer_names(
    df: DataFrame
    ) -> DataFrame:
    """Standardizes names by trimming and removing special characters."""
    return (
        df.withColumn("customer_name", f.trim(f.regexp_replace(f.col("customer_name"), r"\s+", " ")))
        .withColumn("name_parts", f.split(f.col("customer_name"), " "))
        .withColumn("first_name", f.regexp_replace(f.col("name_parts").getItem(0), r"[^A-Za-z]", ""))
        .withColumn("last_name", 
            f.when(f.size(f.col("name_parts")) > 1, 
                   f.regexp_replace(f.col("name_parts").getItem(1), r"[^A-Za-z]", ""))
            .otherwise(None)
        )
        .withColumn("customer_name", f.concat_ws(" ", f.col("first_name"), f.col("last_name")))
        .drop("name_parts")
    )


def validate_phone_numbers(
    df: DataFrame
    ) -> DataFrame:
    """Extracts digits and validates phone length between 10-15 chars."""
    return (
        df.withColumn("phone_digits", f.regexp_replace(f.col("phone"), r"[^0-9]", ""))
        .withColumn("phone",
            f.when((f.length("phone_digits") >= 10) & (f.length("phone_digits") <= 15), f.col("phone_digits"))
            .otherwise(None)
        )
        .drop("phone_digits")
    )


def enrich_customer_metadata(
    df: DataFrame
    ) -> DataFrame:
    """Enforces ID typing and adds system metadata."""
    return (
        df.withColumn("customer_id", f.col("customer_id").cast("string"))
        .withColumn("processing_timestamp", f.current_timestamp())
    )


def transform_customers(df: DataFrame) -> DataFrame:
    """Orchestrator for customer transformations."""
    df = standardize_customer_names(df)
    df = validate_phone_numbers(df)
    df = enrich_customer_metadata(df)
    return df
