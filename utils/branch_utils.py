from pyspark.sql import SparkSession

def create_branch(spark_session: SparkSession, new_branch: str, existing_branch: str) -> str:
    spark_session.sql(f"CREATE BRANCH IF NOT EXISTS {new_branch} IN nessie FROM {existing_branch}")

def merge_branch(spark_session: SparkSession, source_branch: str, target_branch: str):
    spark_session.sql(f"MERGE BRANCH {source_branch} INTO {target_branch} IN nessie")

def delete_branch(spark_session: SparkSession, branch: str):
    spark_session.sql(f"DROP BRANCH {branch} IN nessie")

def switch_to_branch(spark_session: SparkSession, branch: str):
    spark_session.sql(f"USE REFERENCE {branch} IN nessie;")