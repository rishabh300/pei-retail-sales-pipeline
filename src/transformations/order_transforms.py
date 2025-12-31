import pyspark.sql.functions as f
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def apply_order_schema_types(
    df: DataFrame
    ) -> DataFrame:
    """Enforces technical schema types and date parsing."""
    return (
        df.withColumn("order_date", f.to_timestamp(f.col("order_date"), "d/M/yyyy"))
          .withColumn("ship_date", f.to_timestamp(f.col("ship_date"), "d/M/yyyy"))
          .withColumn("row_id", f.col("row_id").cast("integer"))
          .withColumn("quantity", f.col("quantity").cast("integer"))
          .withColumn("price", f.col("price").cast("decimal(10,2)"))
          .withColumn("discount", f.col("discount").cast("decimal(5,2)"))
          .withColumn("profit", f.col("profit").cast("decimal(10,2)"))
          .withColumn("processing_timestamp", f.current_timestamp())
    )


def add_partition_columns(
    df: DataFrame
    ) -> DataFrame:
    """Calculates partition keys based on business dates."""
    return (
        df.withColumn("year_month", f.date_format(f.col("order_date"), "yyyy-MM"))
        .withColumn("order_year", f.year(f.col("order_date")).cast("int"))
    )


def apply_order_quality_rules(
    df: DataFrame
    ) -> DataFrame:
    """Evaluates critical DQ rules and flags records for quarantine."""
    # Define conditions for reuse to keep code DRY (Don't Repeat Yourself)
    cond_missing_order = f.col("order_id").isNull()
    cond_missing_product = f.col("product_id").isNull()
    cond_missing_customer = f.col("customer_id").isNull()
    cond_invalid_dates = f.col("ship_date") < f.col("order_date")
    
    return (
        df.withColumn("is_critical", 
            cond_missing_order | cond_missing_product | cond_missing_customer | cond_invalid_dates
        )
        .withColumn("is_warning", (f.col("discount") < 0.0) | (f.col("discount") > 1.0))
        .withColumn("quarantine_reason", f.concat_ws(", ",
            f.when(cond_missing_order, "Missing Order ID"),
            f.when(cond_missing_product, "Missing Product ID"),
            f.when(cond_missing_customer, "Missing Customer ID"),
            f.when(cond_invalid_dates, "Invalid Dates")
        ))
    )


def transform_orders(
    df: DataFrame
    ) -> DataFrame:
    """Orchestrator for Order transformations."""
    df = apply_order_schema_types(df)
    df = add_partition_columns(df)
    df = apply_order_quality_rules(df)
    return df


def enrich_order_data(
    df: DataFrame, 
    df_prod: DataFrame, 
    df_cust: DataFrame
    ) -> DataFrame:
    """
    Handles left joins using BROADCAST for performance.
    Small dimension tables are sent to all worker nodes to avoid shuffling 1TB of fact data.
    """
    return (
        df.alias("s")
        # Use broadcast() on the dimension tables
        .join(f.broadcast(df_prod).alias("p"), "product_id", "left")
        .join(f.broadcast(df_cust).alias("c"), "customer_id", "left")
        
        # Apply the "Unknown" labels for missing dimension keys
        .withColumn("category", f.coalesce(f.col("category"), f.lit("Unknown Category")))
        .withColumn("sub_category", f.coalesce(f.col("sub_category"), f.lit("Unknown Sub-Category")))
        .withColumn("customer_name", f.coalesce(f.col("customer_name"), f.lit("Unknown Customer")))
    )


    



