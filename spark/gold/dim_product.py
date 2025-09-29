from utils.scd_utils import scd2
from utils.spark_utils import get_spark_session
from pyspark.sql import SparkSession

def build_dim_product(spark: SparkSession):
    product = spark.table("nessie.silver.product")
    dim_product = spark.table("nessie.gold.dim_product")
    scd2(
        spark=spark,
        source=product,
        source_name="product",
        target=dim_product,
        target_name="dim_product",
        bk_name="product_id",
        sk_name="product_key"
    )
    spark.stop()