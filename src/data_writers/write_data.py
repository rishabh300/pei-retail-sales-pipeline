from typing import Optional, Dict, Any
import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable


def write_bronze_data(
    df: DataFrame, 
    target_table: str, 
    checkpoint_location: str | None = None, 
    is_streaming: bool = True, 
    file_source_path: str | None = None, 
    trigger: Dict[str, Any] | None = None
    ) -> None:
    """
    To ADD
    """
    
    if is_streaming:
        df = df.withColumn("file_path", f.col("_metadata.file_path"))
    else:
        df = df.withColumn("file_path", f.lit(file_source_path))
        
    df = df.withColumn("ingestion_timestamp", f.current_timestamp())

    if is_streaming:
        if not checkpoint_location:
            raise ValueError("Streaming writes require a checkpoint_location.")
            
        (
            df.writeStream
            .option("checkpointLocation", checkpoint_location)
            .option("mergeSchema", "true") 
            .outputMode("append")
            .trigger(**trigger)
            .toTable(target_table)
        )
    else:
        (
            df.write
            .format("delta")
            .option("mergeSchema", "true")
            .mode("append")
            .saveAsTable(target_table)
        )


def upsert_delta_table(
    spark_session: SparkSession, 
    df: DataFrame, 
    target_table_name: str, 
    join_key: str, 
    partition_col: Optional[str] = None
    ) -> None: 
    """
    Idempotent Upsert (MERGE) handler for Delta tables.
    Supports primary-key merges and partition-pruning merges 
    
    Args:
        spark_session (SparkSession): The active Spark session.
        df (DataFrame): The source DataFrame containing records to be merged.
        target_table_name (str): The fully qualified Delta table name.
        join_key (str): The unique natural key for the join (e.g., 'customer_id').
        partition_col (str, optional): The column used for partition pruning. 
            If provided, the merge will only scan the affected partitions.
    """
    target_table = DeltaTable.forName(spark_session, target_table_name)
    
    # base join
    keys = [k.strip() for k in join_key.split(",")]
    join_condition = " AND ".join([f"t.{k} = s.{k}" for k in keys])

    # Construct partition pruning filter
    if partition_col:
        affected_partitions = [row[0] for row in df.select(partition_col).distinct().collect()]
        
        if affected_partitions:
            partition_values = ", ".join([f"'{v}'" if isinstance(v, (str)) else str(v) for v in affected_partitions])
            
            join_condition = (
                f"t.{partition_col} = s.{partition_col} "
                f"AND t.{partition_col} IN ({partition_values}) "
                f"AND {join_condition}"
            )

    # Execute merge
    (
        target_table.alias("t")
        .merge(df.alias("s"), join_condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )




