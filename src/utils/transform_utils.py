from pyspark.sql import DataFrame
import pyspark.sql.functions as f

def normalize_raw_schema(
    df: DataFrame
    ) -> DataFrame: 
    """
    Standardizes column names 
    1. Replaces spaces(_), hyphens(-) with underscores
    2. Converts all characters to lowercase
    """
    return df.select([
        f.col(f"`{c}`").alias(c.replace(' ', '_').replace('-', '_').lower()) 
        for c in df.columns
    ])