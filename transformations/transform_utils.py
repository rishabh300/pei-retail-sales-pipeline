import pyspark.sql.functions as f

def get_watermark(spark_session, table_name): 
    watermark_df = (
        spark_session.read.table("pei.default.batch_watermark")
        .filter(f.col("table_name") == table_name)
        .select("last_ingestion_ts")
        .first()
    )

    return watermark_df.last_ingestion_ts if watermark_df else "1900-01-01 00:00:00"


def update_watermark(spark_session, table_name, max_ts): 
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
    1. Replaces spaces with underscores
    2. Converts all characters to lowercase
    """
    return df.select([
        f.col(f"`{c}`").alias(c.replace(' ', '_').replace('-', '_').lower()) 
        for c in df.columns
    ])


def optimize_partitions(spark_session, table_name, df, partition_col, zorder_cols):
    affected_values = [row[0] for row in df.select(partition_col).distinct().collect()]
    if affected_values:
        partition_list = ", ".join(map(str, affected_values))
        spark_session.sql(f"OPTIMIZE {table_name} WHERE {partition_col} IN ({partition_list}) ZORDER BY ({zorder_cols})")