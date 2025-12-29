import pyspark.sql.functions as f
from delta.tables import DeltaTable

def transform_products(df): 
    enriched_df = (
        df
        # standarize product name
        .withColumn("product_name", f.regexp_replace(f.col('product_name'), r"[Â\xc2\xa0]", ""))
        # coverts price per product to decimal(10, 2)
        .withColumn("price_per_product", f.col("price_per_product").cast("Decimal(10,2)"))
        .withColumn("processing_timestamp", f.current_timestamp())
    )

    return enriched_df


def upsert_product(spark_session, df, target_table_name): 
    """
    Performs an idempotent Upsert (MERGE) into the Products Silver table.
    
    This function implements a Slowing Changing Dimension (SCD) Type 1 pattern, 
    overwriting existing records with the latest source attributes and inserting 
    new records.

    Args:
        spark_session (SparkSession): The active Spark session.
        df (DataFrame): The enriched source DataFrame containing deduplicated 
            customer records.
        target_table_name (str): The fully qualified Delta table name 
            (catalog.schema.table).

    Returns:
        None: The function executes the MERGE operation in-place on the target table.
    """
    target_table = DeltaTable.forName(spark_session, target_table_name)

    (
        target_table.alias("t")
        .merge(df.alias("s"), "t.product_id = s.product_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )