from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession

def move_file(spark_session: SparkSession, from_path: str, to_path: str) -> None:
    """
    Moves a file using Databricks File System (DBFS) utilities.
    """
    dbutils = DBUtils(spark_session)
    dbutils.fs.mv(from_path, to_path)