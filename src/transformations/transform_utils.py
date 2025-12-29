import pyspark.sql.functions as f

def get_watermark(spark_session, table_name): 
    """
    Retrieves the high-watermark timestamp for a specific table from the metadata store.

    Args:
        spark_session (SparkSession): The active Spark session.
        table_name (str): The name of the source table (e.g., 'customers_raw') 
            to look up in the watermark registry.

    Returns:
        str/datetime: The last ingested timestamp. Returns '1900-01-01 00:00:00' 
            as a default 'bootstrap' date if no record exists for the table.
    """
    watermark_df = (
        spark_session.read.table("pei.default.batch_watermark")
        .filter(f.col("table_name") == table_name)
        .select("last_ingestion_ts")
        .first()
    )

    return watermark_df.last_ingestion_ts if watermark_df else "1900-01-01 00:00:00"


def update_watermark(spark_session, table_name, max_ts): 
    """
    Updates or inserts the high-watermark timestamp in the metadata registry.

    Args:
        spark_session (SparkSession): The active Spark session.
        table_name (str): The name of the table being tracked.
        max_ts (str/datetime): The upper bound of the current batch (usually the 
            batch start time or the maximum ingestion timestamp found in the source).

    Returns:
        None: Executes an in-place MERGE on the metadata table.
    """
    if max_ts: 
        spark_session.sql(f"""
            MERGE INTO pei.default.batch_watermark t
            USING (SELECT '{table_name}' as table_name, TIMESTAMP('{max_ts}') as last_ts) s
            ON t.table_name = s.table_name
            WHEN MATCHED THEN UPDATE SET last_ingestion_ts = s.last_ts
            WHEN NOT MATCHED THEN INSERT (table_name, last_ingestion_ts) VALUES (s.table_name, s.last_ts)
        """)
        

def normalize_raw_schema(df): 
    """
    Standardizes column names 
    1. Replaces spaces(_), hyphens(-) with underscores
    2. Converts all characters to lowercase
    """
    return df.select([
        f.col(f"`{c}`").alias(c.replace(' ', '_').replace('-', '_').lower()) 
        for c in df.columns
    ])


def optimize_partitions(spark_session, table_name, df, partition_col, zorder_cols):
    """
    Performs targeted Delta Lake optimization (Compaction and Z-Ordering) on modified partitions.

    Args:
        spark_session (SparkSession): The active Spark session.
        table_name (str): The fully qualified name of the Delta table to optimize.
        df (DataFrame): The source DataFrame from the current batch used to identify 
            affected partitions.
        partition_col (str): The name of the column used for partitioning (e.g., 'year_month').
        zorder_cols (str): A comma-separated string of columns to Z-Order by (e.g., 'customer_id, category').

    Returns:
        None: Executes the SQL OPTIMIZE command in-place.
    """
    affected_values = [row[0] for row in df.select(partition_col).distinct().collect()]
    if affected_values:
        partition_list = ", ".join(map(str, affected_values))
        spark_session.sql(f"OPTIMIZE {table_name} WHERE {partition_col} IN ({partition_list}) ZORDER BY ({zorder_cols})")