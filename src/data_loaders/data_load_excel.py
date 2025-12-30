from pyspark.sql import DataFrame, SparkSession

def load_raw_data(
    spark_session: SparkSession, 
    file_path: str
    ) -> DataFrame: 
    """
    Performs a batch read of an Excel spreadsheet from the specified path.

    Args:
        spark_session (SparkSession): The active Spark session.
        file_path (str): The cloud storage path (ADLS/S3/GCS) or local path 
            to the Excel (.xlsx) file.

    Returns:
        DataFrame: A Spark DataFrame containing the raw contents of the 
            specified worksheet.
    """
    return (
        spark_session.read
        .format("excel")
        .option("header", "true")
        .option("inferSchema", "false") 
        .option("sheetName", "Sheet1") 
        .load(file_path)
    )