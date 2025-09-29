import uuid
from pyspark.sql import functions as F
from datetime import datetime
from pyspark.sql import DataFrame

def ingest(
    table_name: str, 
    df: DataFrame,
    ts: datetime
) -> bool:
    # Thêm các cột cần thiết để theo dõi
    batch_id = str(uuid.uuid4())
    df_bronze = (df
      .withColumn("_ingested_at", F.lit(ts))
      .withColumn("_batch_id", F.lit(batch_id))
      .withColumn("_is_deleted", F.lit(False))
    )
    
    # Ghi vào Iceberg
    df_bronze.writeTo(f"nessie.bronze.{table_name}").append()
    return True