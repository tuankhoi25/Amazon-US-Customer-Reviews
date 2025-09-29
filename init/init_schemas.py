from utils.spark_utils import get_spark_session

def init_schemas():
    spark = get_spark_session(app_name="init_catalog")

    spark.sql("USE REFERENCE main IN nessie")
    spark.sql("CREATE DATABASE nessie.bronze")
    spark.sql("CREATE DATABASE nessie.silver")
    spark.sql("CREATE DATABASE nessie.gold")

    spark.stop()