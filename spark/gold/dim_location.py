from utils.scd_utils import SCD1
from utils.spark_utils import get_spark_session
from pyspark.sql import SparkSession

def build_dim_location(spark: SparkSession):
    location = spark.table("nessie.silver.location")
    dim_location = spark.table("nessie.gold.dim_location")
    SCD1(
        spark=spark,
        source=location,
        target=dim_location,
        source_name="location",
        target_name="dim_location",
        bk_name="location_id",
        sk_name="location_key"
    )
    spark.stop()