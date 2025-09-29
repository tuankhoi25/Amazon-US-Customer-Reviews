from utils.scd_utils import scd2, switch_bk_sk
from utils.spark_utils import get_spark_session
from pyspark.sql import SparkSession

def build_bridge_customer_location(spark: SparkSession):    
    customer_location = spark.table("nessie.silver.customer_location")
    dim_customer = spark.table("nessie.gold.dim_customer")
    dim_location = spark.table("nessie.gold.dim_location")
    bridge_customer_location = spark.table("nessie.gold.bridge_customer_location")
    
    customer_location = switch_bk_sk(
        bridge_table=customer_location, 
        dim_table=dim_customer, 
        bk_name="customer_id", 
        sk_name="customer_key"
    )

    customer_location = switch_bk_sk(
        bridge_table=customer_location, 
        dim_table=dim_location, 
        bk_name="location_id", 
        sk_name="location_key"
    )

    scd2(
        spark=spark,
        source=customer_location,
        target=bridge_customer_location,
        source_name="customer_location",
        target_name="bridge_customer_location",
        bk_name="customer_location_id",
        sk_name="customer_location_key"
    )

    spark.stop()