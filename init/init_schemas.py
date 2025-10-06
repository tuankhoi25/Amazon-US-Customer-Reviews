from pyspark.sql import SparkSession

def init_schemas(spark: SparkSession):
    spark.sql("USE REFERENCE main IN nessie")
    spark.sql("CREATE DATABASE nessie.bronze")
    spark.sql("CREATE DATABASE nessie.silver")
    spark.sql("CREATE DATABASE nessie.gold")