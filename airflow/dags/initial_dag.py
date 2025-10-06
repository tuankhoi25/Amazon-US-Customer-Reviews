from datetime import datetime
from airflow.decorators import dag, task
from init.init_schemas import init_schemas
from init.init_tables_in_bronze import init_tables_in_bronze
from init.init_tables_in_silver import init_tables_in_silver
from init.init_tables_in_gold import init_tables_in_gold
from utils.spark_utils import get_or_create_spark_session, stop_spark_session


@dag(
    dag_id="init_schemas_dag",
    start_date=datetime(1999, 1, 1),
    schedule=None,
    catchup=False,
    tags=["init", "schemas", "tables"],
)
def init_schemas_dag():

    @task
    def init_schemas_tables():
        spark = get_or_create_spark_session(app_name="init_schemas_tables")
        init_schemas(spark=spark)
        init_tables_in_bronze(spark=spark)
        init_tables_in_silver(spark=spark)
        init_tables_in_gold(spark=spark)
        stop_spark_session()

    init_schemas_tables()

init_schemas_dag()