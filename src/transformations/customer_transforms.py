import pyspark.sql.functions as f
from delta.tables import DeltaTable
from pyspark.sql import DataFrame


def standardize_customer_names(
    df: DataFrame
    ) -> DataFrame:
    """Standardizes names by removing special characters first, then extracting parts.
    """
    return (
        df.withColumn("customer_name", f.trim(f.col("customer_name")))
        # Remove all non-alphabetic characters (except spaces)
        .withColumn("customer_name", f.regexp_replace(f.col("customer_name"), r"[^A-Za-z\s]", ""))
        # Collapse multiple spaces to single space
        .withColumn("customer_name", f.regexp_replace(f.col("customer_name"), r"\s+", " "))
        # Trim again after space collapse
        .withColumn("customer_name", f.trim(f.col("customer_name")))
        # Split into parts for first and last name extraction
        .withColumn("name_parts", f.split(f.col("customer_name"), " "))
        # Extract first name (first non-empty part)
        .withColumn("first_name", 
            f.when(f.size(f.col("name_parts")) > 0, f.coalesce(f.col("name_parts").getItem(0), f.lit("")))
            .otherwise(f.lit(""))
        )
        # Extract last name (second non-empty part)
        .withColumn("last_name", 
            f.when(f.size(f.col("name_parts")) > 1, f.coalesce(f.col("name_parts").getItem(1), f.lit("")))
            .otherwise(f.lit(""))
        )
        # Ensure we never have blank customer_name by checking both parts
        .withColumn("customer_name", 
            f.when(
                (f.length(f.col("first_name")) > 0) | (f.length(f.col("last_name")) > 0),
                f.trim(f.concat_ws(" ", f.col("first_name"), f.col("last_name")))
            ).otherwise(None)
        )
        .withColumn("customer_name", f.initcap(f.col("customer_name")))
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
