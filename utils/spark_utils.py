import pyspark
from utils.config import settings
from pyspark.sql import SparkSession

def to_spark_key(env_key: str) -> str:
    """Chuyển ENV_KEY sang spark.key format"""
    key = env_key
    key = key.replace("__", "-")
    key = key.replace("_", ".")
    return key

def create_spark_session(app_name: str) -> SparkSession:
    """Khởi tạo và trả về SparkSession"""
    conf = pyspark.SparkConf()

    for key, value in settings.model_dump().items():
        if key.startswith("spark_") and value not in (None, "", []):
            spark_key = to_spark_key(key)
            if isinstance(value, list):
                if len(value) == 0:
                    continue
                conf.set(spark_key, ",".join(value))
            else:
                conf.set(spark_key, str(value))

    spark_session = (
        SparkSession.builder
            .master(settings.SPARK_MASTER)
            .appName(app_name)
            .config(conf=conf)
            .getOrCreate()
    )

    return spark_session

def get_or_create_spark_session(app_name: str) -> SparkSession:
    """Lấy SparkSession hiện tại hoặc tạo mới nếu chưa có"""
    current_spark_session = SparkSession.getActiveSession()
    if current_spark_session is not None:
        return current_spark_session
    return create_spark_session(app_name)

def stop_spark_session():
    """Dừng SparkSession"""
    current_spark_session = SparkSession.getActiveSession()

    if current_spark_session is not None:
        current_spark_session.stop()