from utils.scd_utils import apply_scd1, switch_bk_sk
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

def build_bridge_product_category(spark: SparkSession):
    product_category = spark.table("nessie.silver.product_category")
    dim_category = spark.table("nessie.gold.dim_category")
    dim_product = spark.table("nessie.gold.dim_product").filter(col("is_current")==True)
    bridge_product_category = spark.table("nessie.gold.bridge_product_category")

    product_category = switch_bk_sk(
        bridge_table=product_category, 
        dim_table=dim_category, 
        bk_name="category_id", 
        sk_name="category_key"
    )
    product_category = switch_bk_sk(
        bridge_table=product_category, 
        dim_table=dim_product, 
        bk_name="product_id", 
        sk_name="product_key"
    )
    apply_scd1(
        spark=spark,
        source=product_category,
        target=bridge_product_category,
        source_name="product_category",
        target_name="bridge_product_category",
        bk_name="product_category_id",
        sk_name="product_category_key"
    )