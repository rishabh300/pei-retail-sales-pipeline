from src.transformations.transform_utils import normalize_raw_schema

def test_normalize_raw_schema_normalizes_headers(spark):

    messy_columns = ["Customer-ID", "First Name", "Contact_Number", "DATE-OF-BIRTH"]
    test_data = [("1", "Rishabh", "123", "1990-01-01")]
    df = spark.createDataFrame(test_data, messy_columns)


    normalized_df = normalize_raw_schema(df)
    result_columns = normalized_df.columns

    expected_columns = ["customer_id", "first_name", "contact_number", "date_of_birth"]
    assert result_columns == expected_columns