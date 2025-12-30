from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as f

def optimize_partitions(
    spark_session: SparkSession, 
    table_name: str, 
    df: DataFrame, 
    partition_col: str, 
    zorder_cols: str
    ) -> None:
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
        partition_list = ", ".join([
            f"'{v}'" if isinstance(v, str) else str(v) 
            for v in affected_values
        ])
        
        # Execute the optimization command
        spark_session.sql(f"""
            OPTIMIZE {table_name} 
            WHERE {partition_col} IN ({partition_list}) 
            ZORDER BY ({zorder_cols})
        """)


