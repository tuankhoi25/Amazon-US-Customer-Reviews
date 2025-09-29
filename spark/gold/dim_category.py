from utils.scd_utils import SCD1
from utils.spark_utils import get_spark_session
from pyspark.sql import SparkSession

def build_dim_category(spark: SparkSession):
    category = spark.table("nessie.silver.category")
    dim_category = spark.table("nessie.gold.dim_category")
    SCD1(
        spark=spark,
        source=category,
        target=dim_category,
        source_name="category",
        target_name="dim_category",
        bk_name="category_id",
        sk_name="category_key"
    )
    spark.stop()