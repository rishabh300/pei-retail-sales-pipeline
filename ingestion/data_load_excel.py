def load_raw_data(spark_session, file_path): 
    return (
        spark_session.read
        .format("excel")
        .option("header", "true")
        .option("inferSchema", "false") 
        .option("sheetName", "Sheet1") 
        .load(file_path)
    )
