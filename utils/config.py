from typing import List
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
load_dotenv(interpolate=True)

class Settings(BaseSettings):
    # Minio settings
    MINIO_ROOT_USER: str = "minioadmin"
    MINIO_ROOT_PASSWORD: str = "minioadmin"
    MINIO_REGION: str = "us-east-1"
    AWS_S3_ENDPOINT: str = "http://minio:9000"
    WAREHOUSE: str = "s3a://warehouse/"

    # Nessie settings
    NESSIE_URI: str = "http://nessie:19120/api/v2"
    NESSIE_VERSION_STORE_TYPE: str = "JDBC2"
    NESSIE_VERSION_STORE_PERSIST_JDBC_DATASOURCE: str = "postgresql"

    # PostgreSQL settings
    POSTGRES_USER: str = "postgres"
    POSTGRES_PASSWORD: str = "postgres"
    POSTGRES_DB: str = "oltp"
    POSTGRES_DB_FOR_NESSIE: str = "nessie"
    POSTGRES_DB_FOR_METABASE: str = "metabaseappdb"

    # Spark settings
    SPARK_MASTER: str = "spark://spark-master:7077"
    spark_jars_packages: List[str] = []
    spark_sql_extensions: List[str] = []
    spark_sql_catalog_nessie: str = "org.apache.iceberg.spark.SparkCatalog"
    spark_sql_catalog_nessie_uri: str = "http://nessie:19120/api/v2"
    spark_sql_catalog_nessie_client__api__version: str = "2"
    spark_sql_catalog_nessie_ref: str = "main"
    sql_sql_catalog_nessie_authentication_type: str = "NONE"
    spark_sql_catalog_nessie_catalog__impl: str = "org.apache.iceberg.nessie.NessieCatalog"
    spark_sql_catalog_nessie_s3_endpoint: str = "http://minio:9000"
    spark_sql_catalog_nessie_warehouse: str = "s3a://warehouse/"
    spark_sql_catalog_nessie_s3_path__style__access: str = "true"
    spark_sql_catalog_nessie_s3_access__key__id: str = "minioadmin"
    spark_sql_catalog_nessie_s3_secret__access__key: str = "minioadmin"
    spark_sql_catalog_nessie_io__impl: str = "org.apache.iceberg.aws.s3.S3FileIO"
    spark_driver_extraJavaOptions: str = "-Daws.region=us-east-1"
    spark_executor_extraJavaOptions: str = "-Daws.region=us-east-1"
    spark_sql_iceberg_merge__schema: str = "true"
    spark_executor_memory: str = "2g"
    spark_driver_memory: str = "2g"
    spark_executor_cores: str = "2"
    
    class Config:
        env_file = "./.env"
        case_sensitive = True

settings = Settings()