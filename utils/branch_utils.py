from utils.spark_utils import get_spark_session

def create_elt_branch(**context):
    run_ts = context["logical_date"]
    spark = get_spark_session()
    etl_processing_branch_name = f"feat/etl-processing-{run_ts.strftime('%Y-%m-%d-%H-%M-%S')}"
    spark.sql(f"""
        CREATE BRANCH
        IF NOT EXISTS {etl_processing_branch_name}
        IN nessie
        FROM main
    """)

def merge_elt_branch(**context):
    run_ts = context["logical_date"]
    spark = get_spark_session()
    etl_processing_branch_name = f"feat/etl-processing-{run_ts.strftime('%Y-%m-%d-%H-%M-%S')}"
    spark.sql(f"""
        MERGE BRANCH
        IF EXISTS {etl_processing_branch_name}
        IN nessie
        INTO main
    """)

def delete_elt_branch(**context):
    run_ts = context["logical_date"]
    spark = get_spark_session()
    etl_processing_branch_name = f"feat/etl-processing-{run_ts.strftime('%Y-%m-%d-%H-%M-%S')}"
    spark.sql(f"""
        DELETE BRANCH
        IF EXISTS {etl_processing_branch_name}
        IN nessie
    """)
