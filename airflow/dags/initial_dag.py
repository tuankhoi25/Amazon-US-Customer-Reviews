from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from init.init_schemas import init_schemas
from init.init_tables_in_bronze import init_tables_in_bronze
from init.init_tables_in_silver import init_tables_in_silver
from init.init_tables_in_gold import init_tables_in_gold

with DAG(
    dag_id="init_schemas_dag",
    start_date=datetime(2025, 9, 28),
    schedule_interval=None,
    catchup=False,
) as dag:

    init_schemas_task = PythonOperator(
        task_id="init_schemas",
        python_callable=init_schemas,
    )

    init_tables_in_bronze_task = PythonOperator(
        task_id="init_tables_in_bronze",
        python_callable=init_tables_in_bronze,
    )

    init_tables_in_silver_task = PythonOperator(
        task_id="init_tables_in_silver",
        python_callable=init_tables_in_silver,
    )

    init_tables_in_gold_task = PythonOperator(
        task_id="init_tables_in_gold",
        python_callable=init_tables_in_gold,
    )

    init_schemas_task >> [
        init_tables_in_bronze_task,
        init_tables_in_silver_task,
        init_tables_in_gold_task,
    ]
