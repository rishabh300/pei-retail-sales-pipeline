import pyspark.sql.functions as f 

def load_raw_data_with_schema_evolution(spark_session, source_path, file_format, additional_options={}):
    """
    Initializes a structured stream to ingest raw data using Databricks AutoLoader (cloudFiles).

    Args:
        spark_session (SparkSession): The active Spark session.
        source_path (str): The cloud storage path (ADLS/S3/GCS) where raw files are landing.
        file_format (str): The format of the source files (e.g., 'csv', 'json', 'parquet').
        additional_options (dict, optional): Extra Spark/AutoLoader configurations to 
            override defaults (e.g., 'cloudFiles.schemaLocation').

    Returns:
        DataFrame: A streaming DataFrame.
    """
    base_options = {
        "cloudFiles.format": file_format,
        "cloudFiles.schemaEvolutionMode": "addNewColumns",
        "multiline": "true"
    }

    file_format_options = {}
    if file_format == "csv": 
        file_format_options = {
            "cloudFiles.format": "csv",
            "quote": "\"",
            "escape": "\"",
            "header": "true"
        }
    elif file_format == "json": 
        file_format_options = {
            "cloudFiles.format": "json"
        }

    final_options = {**base_options, **file_format_options, **additional_options}

    return (
        spark_session.readStream
        .format("cloudFiles")
        .options(**final_options)
        .load(source_path)
    )