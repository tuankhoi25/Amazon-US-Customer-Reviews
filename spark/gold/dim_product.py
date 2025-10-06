from utils.scd_utils import apply_scd2
from pyspark.sql import SparkSession

def build_dim_product(spark: SparkSession):
    product = spark.table("nessie.silver.product")
    dim_product = spark.table("nessie.gold.dim_product")

    apply_scd2(
        spark=spark,
        source=product,
        source_name="product",
        target=dim_product,
        target_name="dim_product",
        bk_name="product_id",
        sk_name="product_key"
    )