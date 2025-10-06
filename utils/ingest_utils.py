from datetime import date, datetime
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from typing import Optional
import uuid
from pyspark.sql.functions import lit

def extract_from_pg(
    spark: SparkSession,
    table_name: str, 
    today: date
) -> Optional[DataFrame]:
    
    # Lấy thời gian gần nhất mà bronze layer được update
    max_updated_at = spark.sql(f"SELECT max(updated_at) as max_updated_at FROM nessie.bronze.{table_name}").collect()[0]["max_updated_at"]
    max_updated_at = "1999-01-01" if max_updated_at is None else str(max_updated_at)
    print(f"max_updated_at: {max_updated_at}")
    
    query = f"""
    (
        SELECT 
            min(updated_at) AS min_date, 
            max(updated_at) AS max_date 
        FROM {table_name} 
        WHERE updated_at > '{max_updated_at}' AND updated_at <= '{today}'
    ) tmp
    """
    bounds = spark.read \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://postgres:5432/oltp") \
        .option("dbtable", query) \
        .option("user", "postgres") \
        .option("password", "postgres") \
        .option("driver", "org.postgresql.Driver") \
        .load() \
        .collect()[0]
    
    lower, upper = bounds["min_date"], bounds["max_date"]
    
    if lower is None or upper is None:
        print(f"Không có bản ghi mới cho {table_name}, skip ingest.")
        return None
    
    print(f"lower: {lower}, upper: {upper}")
    
    # Chỉ lấy những record ở source có giá trị updated_at > giá trị max(updated_at) ở bronze
    query = f"""
    (
        SELECT * 
        FROM {table_name} 
        WHERE updated_at > '{max_updated_at}' AND updated_at <= '{today}'
    ) AS {table_name}
    """
    df = (spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/oltp")
        .option("dbtable", query)
        .option("user", "postgres")
        .option("password", "postgres")
        .option("driver", "org.postgresql.Driver")
        .option("partitionColumn", "id")
        .option("lowerBound", lower)
        .option("upperBound", upper)
        .option("numPartitions", "8")
        .option("partitionColumn", "updated_at")
        .option("lowerBound", lower)
        .option("upperBound", upper)
        .option("numPartitions", "8")
        .option("fetchsize", "10000")
        .load()
    )
    return df

def ingest(
    table_name: str, 
    df: DataFrame,
    ts: datetime
) -> bool:
    # Thêm các cột cần thiết để theo dõi
    batch_id = str(uuid.uuid4())
    df_bronze = (df
      .withColumn("_ingested_at", lit(ts))
      .withColumn("_batch_id", lit(batch_id))
      .withColumn("_is_deleted", lit(False))
    )
    
    # Ghi vào Iceberg
    df_bronze.writeTo(f"nessie.bronze.{table_name}").append()
    return True