from airflow.models.param import Param
from airflow.decorators import dag, task
from datetime import datetime
from utils.spark_utils import get_or_create_spark_session
from spark.gold.dim_date import build_dim_date

@dag(
    dag_id="build_dim_date_dag",
    schedule=None,
    start_date=datetime(1999, 1, 1),
    catchup=False,
    params={
        "start": Param("2025-01-01", type="string", format="date", title="Start Date"),
        "end": Param("2025-01-31", type="string", format="date", title="End Date"),
    },
    tags=["gold", "dim_date"]
)
def build_dim_date_dag():

    @task
    def build_dim_date_task(params: dict):
        start_date = datetime.strptime(params["start"], "%Y-%m-%d")
        end_date = datetime.strptime(params["end"], "%Y-%m-%d")

        spark = get_or_create_spark_session("gold_dim_date")
        build_dim_date(spark, start_date, end_date)

    build_dim_date_task()

build_dim_date_dag()