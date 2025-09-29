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
    SPARK_JARS_PACKAGES: List[str] = []
    SPARK_SQL_EXTENSIONS: List[str] = []
    SPARK_SQL_CATALOG_NESSIE: str = "org.apache.iceberg.spark.SparkCatalog"
    SPARK_SQL_CATALOG_NESSIE_URI: str = "http://nessie:19120/api/v2"
    SPARK_SQL_CATALOG_NESSIE_CLIENT__API__VERSION: str = "2"
    SPARK_SQL_CATALOG_NESSIE_REF: str = "main"
    SPARK_SQL_CATALOG_NESSIE_AUTHENTICATION_TYPE: str = "NONE"
    SPARK_SQL_CATALOG_NESSIE_CATALOG__IMPL: str = "org.apache.iceberg.nessie.NessieCatalog"
    SPARK_SQL_CATALOG_NESSIE_S3_ENDPOINT: str = "http://minio:9000"
    SPARK_SQL_CATALOG_NESSIE_WAREHOUSE: str = "s3a://warehouse/"
    SPARK_SQL_CATALOG_NESSIE_S3_PATH__STYLE__ACCESS: str = "true"
    SPARK_SQL_CATALOG_NESSIE_S3_ACCESS__KEY__ID: str = "minioadmin"
    SPARK_SQL_CATALOG_NESSIE_S3_SECRET__ACCESS__KEY: str = "minioadmin"
    SPARK_SQL_CATALOG_NESSIE_IO__IMPL: str = "org.apache.iceberg.aws.s3.S3FileIO"
    SPARK_SQL_ICEBERG_MERGE__SCHEMA: str = "true"
    SPARK_EXECUTOR_MEMORY: str = "2g"
    SPARK_DRIVER_MEMORY: str = "2g"
    SPARK_EXECUTOR_CORES: str = "2"
    
    class Config:
        env_file = "./.env"
        case_sensitive = True

settings = Settings()