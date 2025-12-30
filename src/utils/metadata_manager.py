from datetime import datetime
import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def get_last_processed_version(
    spark: SparkSession, 
    catalog_name: str,
    schema_name: str,
    table_name: str
    ) -> int:
    """Retrieves the last processed Delta version for the table"""
    row = (spark.read.table("pei.default.pipeline_versions")
           .filter(f.col("table_name") == table_name)
           .select("last_processed_ver_num")
           .first())
    return row.last_processed_ver_num if row else -1


def update_last_processed_version(
    spark: SparkSession, 
    catalog_name: str,
    schema_name: str,
    table_name: str, 
    version_num: int
    ) -> None:
    """Updates the Delta version number for the table."""
    spark.sql(f"""
        MERGE INTO pei.default.pipeline_versions t
        USING (
            SELECT '{catalog_name}' as catalog_name, 
            '{schema_name}' as schema_name,
            '{table_name}' as table_name, 
            {version_num} as ver
            ) s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET last_processed_ver_num = s.ver
        WHEN NOT MATCHED THEN INSERT (catalog_name, schema_name, table_name, last_processed_ver_num) 
        VALUES (s.catalog_name, s.schema_name, s.table_name, s.ver)
    """)


def get_latest_table_version(
    spark: SparkSession, 
    catalog_name: str, 
    schema_name: str, 
    table_name: str
    ) -> int:
    """
    Retrieves the most recent version number from a Delta table's history.
    """
    full_table_path = f"{catalog_name}.{schema_name}.{table_name}"
    history_df = spark.sql(f"DESCRIBE HISTORY {full_table_path} LIMIT 1")
    return int(history_df.select("version").first()[0])


def get_pipeline_version_range (
    spark: SparkSession, 
    catalog_name: str, 
    schema_name: str, 
    table_name: str, 
    start_val: str, 
    end_val: str
) -> tuple[int, int]:
    """
    Calculates the start and end Delta versions for a pipeline run, 
    handling both manual backfills (via widgets) and incremental watermarks.
    
    Returns:
        tuple: (start_version, end_version)
    """
    latest_table_version = get_latest_table_version(spark, catalog_name, schema_name, table_name)

    if start_val.strip() != "":
        start_version = int(start_val.strip())
    else:
        last_processed = get_last_processed_version(spark, catalog_name, schema_name, table_name)
        start_version = last_processed + 1

    # 3. Logic for End Version
    if end_val.strip() != "":
        end_version = int(end_val.strip())
    else:
        end_version = latest_table_version

    return start_version, end_version