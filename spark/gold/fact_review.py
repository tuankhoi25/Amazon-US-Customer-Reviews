from utils.scd_utils import switch_bk_sk, update_fact_table
from pyspark.sql.functions import col
from pyspark.sql import SparkSession

def build_fact_review(spark: SparkSession):
    review = spark.table("nessie.silver.review")
    review = review.withColumnRenamed("modified_date", "full_date")

    dim_customer = spark.table("nessie.gold.dim_customer")
    dim_product = spark.table("nessie.gold.dim_product").filter(col("is_current")==True)
    dim_date = spark.table("nessie.gold.dim_date")
    fact_review = spark.table("nessie.gold.fact_review")

    review = switch_bk_sk(
        bridge_table=review, 
        dim_table=dim_customer, 
        bk_name="customer_id", 
        sk_name="customer_key"
    )

    review = switch_bk_sk(
        bridge_table=review, 
        dim_table=dim_product, 
        bk_name="product_id", 
        sk_name="product_key"
    )

    review = switch_bk_sk(
        bridge_table=review, 
        dim_table=dim_date, 
        bk_name="full_date", 
        sk_name="date_key"
    )

    update_fact_table(
        spark=spark,
        source=review,
        target=fact_review,
        source_name="review",
        target_name="fact_review",
        bk_name="review_id",
        sk_name="review_key",
        has_cur_col=True
    )