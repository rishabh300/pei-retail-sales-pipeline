import pyspark.sql.functions as f
from delta.tables import DeltaTable

def transform_customers(df): 
    enriched_df = (
        df
        # Standardize customer name
        .withColumn("customer_name", f.trim(f.regexp_replace(f.col("customer_name"), r"\s+", " ")))
        
        # Split names
        .withColumn("name_parts", f.split(f.col("customer_name"), " "))
        .withColumn("first_name", f.regexp_replace(f.col("name_parts").getItem(0), r"[^A-Za-z]", ""))
        .withColumn("last_name", 
            f.when(f.size(f.col("name_parts")) > 1, 
                   f.regexp_replace(f.col("name_parts").getItem(1), r"[^A-Za-z]", ""))
            .otherwise(None)
        )
        
        # Phone cleaning
        .withColumn("phone_digits", f.regexp_replace(f.col("phone"), r"[^0-9]", ""))
        .withColumn("phone",
            f.when((f.length("phone_digits") >= 10) & (f.length("phone_digits") <= 15), f.col("phone_digits"))
            .otherwise(None)
        )
        .withColumn("customer_id", f.col("customer_id").cast("string"))
        .withColumn("processing_timestamp", f.current_timestamp())
    )

    return enriched_df


def upsert_customer(spark_session, df, target_table_name): 
    """
    Performs an idempotent Upsert (MERGE) into the Customer Silver table.
    
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
        .merge(df.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )