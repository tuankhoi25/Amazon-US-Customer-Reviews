from utils.scd_utils import apply_scd1
from pyspark.sql import SparkSession

def build_dim_customer(spark: SparkSession):
    customer = spark.table("nessie.silver.customer")
    dim_customer = spark.table("nessie.gold.dim_customer")

    apply_scd1(
        spark=spark,
        source=customer,
        target=dim_customer,
        source_name="customer",
        target_name="dim_customer",
        bk_name="customer_id",
        sk_name="customer_key"
    )