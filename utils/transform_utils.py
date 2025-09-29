from pyspark.sql.functions import trim
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

def clean_whitespace(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if field.dataType.typeName() == "string":
            df = df.withColumn(field.name, trim(df[field.name]))
    return df

def overwrite_table(
    spark_session: SparkSession,
    df: DataFrame, 
    full_table_name: str
) -> bool:
    """
    Truncate 1 bảng bất kỳ trong Nessie catalog trước, sau đó append data mới.
    Giúp đảm bảo dữ liệu luôn fresh (dùng cho Silver, Gold hoặc schema khác).

    full_table_name : str
        Tên đầy đủ của bảng trong Nessie, ví dụ:
        - "nessie.silver.customer"
        - "nessie.gold.fact_review"
        - "nessie.bronze.review"
    """
    spark_session.sql(f"TRUNCATE TABLE {full_table_name}")
    df.writeTo(full_table_name).append()
    return True
