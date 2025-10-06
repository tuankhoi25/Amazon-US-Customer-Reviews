from utils.branch_utils import create_branch, merge_branch, delete_branch, switch_to_branch
from utils.spark_utils import get_or_create_spark_session, stop_spark_session
from airflow.decorators import dag, task
from datetime import datetime
from utils.transform_utils import overwrite_table
from utils.ingest_utils import extract_from_pg, ingest
from spark.silver import transform_customer, transform_product, transform_review, transform_location, transform_product_category, transform_customer_location, transform_category
from spark.gold import build_fact_review, build_dim_customer, build_dim_product, build_dim_location, build_dim_category, build_bridge_customer_location, build_bridge_product_category
from typing import List


BRONZE_TABLES = [
    "category",
    "customer",
    "customer_location",
    "customer_phone",
    "location",
    "phone_number",
    "product_category",
    "review",
    "shadow_product",
]

SILVER_TRANSFORM_MAP = {
    "customer": (transform_customer, "nessie.silver.customer"),
    "shadow_product": (transform_product, "nessie.silver.product"),
    "review": (transform_review, "nessie.silver.review"),
    "location": (transform_location, "nessie.silver.location"),
    "product_category": (transform_product_category, "nessie.silver.product_category"),
    "customer_location": (transform_customer_location, "nessie.silver.customer_location"),
    "category": (transform_category, "nessie.silver.category"),
}

GOLD_MERGE_MAP = {
    "dim_customer": build_dim_customer,
    "dim_product": build_dim_product,
    "dim_location": build_dim_location,
    "dim_category": build_dim_category,
    "bridge_customer_location": build_bridge_customer_location,
    "bridge_product_category": build_bridge_product_category,
    "fact_review": build_fact_review,
}

SKIP_MAP_BRONZE_TO_SILVER = {
    "category": "category",
    "customer": "customer",
    "customer_location": "customer_location",
    "location": "location",
    "product_category": "product_category",
    "review": "review",
    "shadow_product": "shadow_product",
}

SKIP_MAP_SILVER_TO_GOLD = {
    "customer": "dim_customer",
    "shadow_product": "dim_product",
    "location": "dim_location",
    "category": "dim_category",
    "customer_location": "bridge_customer_location",
    "product_category": "bridge_product_category",
    "review": "fact_review",
}

@dag(
    dag_id="etl_pipeline_dag",
    start_date=datetime(2005, 1, 1),
    schedule=None,
    catchup=False,
)
def etl_pipeline_dag():
    

    @task()
    def create_elt_branch_task(logical_date: datetime):
        spark = get_or_create_spark_session(app_name="create_etl_branch")
        etl_processing_branch_name = f"feat/etl-processing-{logical_date.strftime('%Y-%m-%d-%H-%M-%S')}"
        create_branch(spark_session=spark, new_branch=etl_processing_branch_name, existing_branch="main")
        stop_spark_session()
        return etl_processing_branch_name

    @task()
    def bronze_processing(etl_pipeline_branch_name: str, logical_date: datetime):
        skipped_bronze_tables: List[str] = []
        spark = get_or_create_spark_session(app_name="bronze_ingest")
        switch_to_branch(spark_session=spark, branch=etl_pipeline_branch_name)

        for table_name in BRONZE_TABLES:
            df = extract_from_pg(spark=spark, table_name=table_name, today=logical_date.date())
            if df is None:
                skipped_bronze_tables.append(SKIP_MAP_BRONZE_TO_SILVER[table_name])
                continue
            ingest(table_name=table_name, df=df, ts=logical_date)
        
        stop_spark_session()

        return skipped_bronze_tables


    @task()
    def silver_processing(etl_pipeline_branch_name: str, skipped_bronze_tables: List[str], logical_date: datetime):
        skipped_silver_tables: List[str] = [SKIP_MAP_BRONZE_TO_SILVER[table] for table in skipped_bronze_tables]
        spark = get_or_create_spark_session("silver_transform")

        for source_name in SILVER_TRANSFORM_MAP.keys():
            if source_name in skipped_silver_tables:
                continue
            switch_to_branch(spark_session=spark, branch="main")
            source_df = extract_from_pg(spark=spark, table_name=source_name, today=logical_date.date())
            transform_func, target_name = SILVER_TRANSFORM_MAP[source_name]
            target_df = transform_func(source_df)
            switch_to_branch(spark_session=spark, branch=etl_pipeline_branch_name)
            overwrite_table(spark_session=spark, df=target_df, full_table_name=target_name)
        
        stop_spark_session()

        return skipped_silver_tables

    @task()
    def gold_processing(etl_pipeline_branch_name: str, skipped_silver_tables: List[str]):
        skipped_gold_tables: List[str] = [SKIP_MAP_SILVER_TO_GOLD[table] for table in skipped_silver_tables]
        spark = get_or_create_spark_session("gold_merge")
        switch_to_branch(spark_session=spark, branch=etl_pipeline_branch_name)

        for table_name in GOLD_MERGE_MAP.keys():
            if table_name in skipped_gold_tables:
                continue
            merge_func = GOLD_MERGE_MAP[table_name]
            merge_func(spark)

        stop_spark_session()


    @task()
    def etl_cleanup(etl_pipeline_branch_name: str):
        spark = get_or_create_spark_session("etl_cleanup")
        switch_to_branch(spark_session=spark, branch="main")
        merge_branch(spark_session=spark, source_branch=etl_pipeline_branch_name, target_branch="main")
        delete_branch(spark_session=spark, branch=etl_pipeline_branch_name)
        stop_spark_session()


    etl_pipeline_branch_name = create_elt_branch_task()
    skipped_bronze_tables = bronze_processing(etl_pipeline_branch_name=etl_pipeline_branch_name)
    skipped_silver_tables = silver_processing(etl_pipeline_branch_name=etl_pipeline_branch_name, skipped_bronze_tables=skipped_bronze_tables)
    gold_task = gold_processing(etl_pipeline_branch_name=etl_pipeline_branch_name, skipped_silver_tables=skipped_silver_tables)
    clean_task = etl_cleanup(etl_pipeline_branch_name=etl_pipeline_branch_name)

    gold_task >> clean_task

etl_pipeline_dag()