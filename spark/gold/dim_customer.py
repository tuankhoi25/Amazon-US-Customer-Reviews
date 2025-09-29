from utils.scd_utils import SCD1
from utils.spark_utils import get_spark_session
from pyspark.sql import SparkSession

def build_dim_customer(spark: SparkSession):
    customer = spark.table("nessie.silver.customer")
    dim_customer = spark.table("nessie.gold.dim_customer")
    SCD1(
        spark=spark,
        source=customer,
        target=dim_customer,
        source_name="customer",
        target_name="dim_customer",
        bk_name="customer_id",
        sk_name="customer_key"
    )
    spark.stop()