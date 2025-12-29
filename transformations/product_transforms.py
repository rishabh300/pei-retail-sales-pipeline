import pyspark.sql.functions as f
from delta.tables import DeltaTable

def enrich_products(df): 
    enriched_df = (
        df
        .withColumn("product_name", f.regexp_replace(f.col('product_name'), r"[Â\xc2\xa0]", ""))
        .withColumn("price_per_product", f.col("price_per_product").cast("Decimal(10,2)"))
        .withColumn("processing_timestamp", f.current_timestamp())
    )

    return enriched_df


def upsert(spark_session, df, target_table_name): 
    target_table = DeltaTable.forName(spark_session, target_table_name)

    (
        target_table.alias("t")
        .merge(df.alias("s"), "t.product_id = s.product_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )