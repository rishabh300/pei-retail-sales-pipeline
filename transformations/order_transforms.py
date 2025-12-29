import pyspark.sql.functions as f
from delta.tables import DeltaTable

def transform_orders(df): 
    df_transformed = (
        df.withColumn("order_date", f.to_timestamp(f.col("order_date"), "d/M/yyyy"))
          .withColumn("ship_date", f.to_timestamp(f.col("ship_date"), "d/M/yyyy"))
          .withColumn("processing_timestamp", f.current_timestamp()) 
          .withColumn("row_id", f.col("row_id").cast("integer"))
          .withColumn("quantity", f.col("quantity").cast("integer"))
          .withColumn("price", f.col("price").cast("decimal(10,2)"))
          .withColumn("discount", f.col("discount").cast("decimal(5,2)"))
          .withColumn("profit", f.col("profit").cast("decimal(10,2)"))
          # Define year_month for Partitioning
          .withColumn("year_month", f.date_format(f.col("order_date"), "yyyy-MM"))
          # DQ Logic
          .withColumn("is_critical", 
              (f.col("order_id").isNull()) | (f.col("product_id").isNull()) | 
              (f.col("customer_id").isNull()) | (f.col("ship_date") < f.col("order_date"))
          )
          .withColumn("is_warning", (f.col("discount") < 0.0) | (f.col("discount") > 1.0))
          .withColumn("quarantine_reason", f.concat_ws(", ",
              f.when(f.col("order_id").isNull(), "Missing Order ID"),
              f.when(f.col("product_id").isNull(), "Missing Product ID"),
              f.when(f.col("customer_id").isNull(), "Missing Customer ID"),
              f.when(f.col("ship_date") < f.col("order_date"), "Invalid Dates")
          ))
    )

    return df_transformed


def enrich_order_data(df, df_prod, df_cust):
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
    )


def upsert(spark_session, df, target_table_name): 
    batch_months = [row[0] for row in df.select("year_month").distinct().collect()]
    partition_filter = ", ".join([f"'{m}'" for m in batch_months])

    target_table = DeltaTable.forName(spark_session, target_table_name)
    
    # 2. Construct the Join Condition with explicit pruning
    # This ensures Spark ONLY looks at the specific month folders
    join_condition = f"""t.year_month IN ({partition_filter}) AND t.order_id = s.order_id"""

    (
        target_table.alias("t")
        .merge(df.alias("s"), join_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    



