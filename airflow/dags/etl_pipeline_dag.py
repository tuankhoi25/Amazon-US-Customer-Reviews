from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.branch_utils import create_elt_branch, merge_elt_branch, delete_elt_branch
from utils.spark_utils import get_spark_session
from airflow.decorators import task
from datetime import datetime, date
from utils.transform_utils import overwrite_table
from spark.bronze import (
    read, 
    ingest
)
from spark.silver import (
    transform_customer,
    transform_product,
    transform_review,
    transform_location,
    transform_product_category,
    transform_customer_location,
    transform_category,
)
from spark.gold import (
    build_fact_review,
    build_dim_customer,
    build_dim_product,
    build_dim_location,
    build_dim_category,
    build_bridge_customer_location,
    build_bridge_product_category,
)


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

SILVER_TABLES = [
    "customer",
    "product",
    "review",
    "location",
    "product_category",
    "customer_location",
    "category",
]

SILVER_TRANSFORM_MAP = {
    "customer": (transform_customer, "nessie.silver.customer"),
    "product": (transform_product, "nessie.silver.product"),
    "review": (transform_review, "nessie.silver.review"),
    "location": (transform_location, "nessie.silver.location"),
    "product_category": (transform_product_category, "nessie.silver.product_category"),
    "customer_location": (transform_customer_location, "nessie.silver.customer_location"),
    "category": (transform_category, "nessie.silver.category"),
}

GOLD_TABLES = [
    "dim_customer",
    "dim_product",
    "dim_location",
    "dim_category",
    "bridge_customer_location",
    "bridge_product_category",
    "fact_review",
]

GOLD_MAP = {
    "dim_customer": build_dim_customer,
    "dim_product": build_dim_product,
    "dim_location": build_dim_location,
    "dim_category": build_dim_category,
    "bridge_customer_location": build_bridge_customer_location,
    "bridge_product_category": build_bridge_product_category,
    "fact_review": build_fact_review,
}

with DAG(
    dag_id="etl_pipeline_dag",
    start_date=datetime(2025, 9, 28),
    schedule_interval="@daily",
    catchup=False,
) as dag:
    
    create_elt_branch_task = PythonOperator(
        task_id="create_elt_branch",
        python_callable=create_elt_branch,
    )

    @task
    def bronze_ingest(table_name: str, **context):
        run_ts: datetime = context["logical_date"]
        today: date = run_ts.date()
        nessie_etl_branch = f"feat/etl-processing-{run_ts.strftime('%Y-%m-%d-%H-%M-%S')}"
        spark = get_spark_session(app_name="bronze_ingest")
        spark.sql(f"USE REFERENCE {nessie_etl_branch} IN nessie;")

        df = read(spark=spark, table_name=table_name, today=today)
        ingest(table_name=table_name, df=df, ts=run_ts)

        spark.stop()
        return f"{table_name} done"

    bronze_ingest_tasks = bronze_ingest.expand(table_name=BRONZE_TABLES)


    @task
    def run_transform(table_name: str, **context):
        run_ts: datetime = context["logical_date"]
        today: date = run_ts.date()
        nessie_etl_branch = f"feat/etl-processing-{run_ts.strftime('%Y-%m-%d-%H-%M-%S')}"
        spark = get_spark_session(f"silver_{table_name}")
        spark.sql(f"USE REFERENCE {nessie_etl_branch} IN nessie;")

        df = read(spark=spark, table_name=("shadow_product" if table_name == "product" else table_name), today=today)
        transform_func, target_table = SILVER_TRANSFORM_MAP[table_name]

        df = transform_func(df)
        overwrite_table(spark_session=spark, df=df, full_table_name=target_table)

        spark.stop()
        return f"{table_name} transformed"

    silver_transform_tasks = run_transform.expand(table_name=SILVER_TABLES)

    @task
    def run_gold(table_name: str, **context):
        run_ts: datetime = context["logical_date"]
        nessie_etl_branch = f"feat/etl-processing-{run_ts.strftime('%Y-%m-%d-%H-%M-%S')}"
        spark = get_spark_session(f"gold_{table_name}")
        spark.sql(f"USE REFERENCE {nessie_etl_branch} IN nessie;")

        GOLD_MAP[table_name](spark)
        spark.stop()
        return f"{table_name} built"

    gold_tasks = run_gold.expand(table_name=GOLD_TABLES)

    merge_elt_branch_task = PythonOperator(
        task_id="merge_elt_branch",
        python_callable=merge_elt_branch,
    )

    delete_elt_branch_task = PythonOperator(
        task_id="delete_elt_branch",
        python_callable=delete_elt_branch,
    )
    create_elt_branch_task >> bronze_ingest_tasks >> silver_transform_tasks >> gold_tasks >> merge_elt_branch_task >> delete_elt_branch_task