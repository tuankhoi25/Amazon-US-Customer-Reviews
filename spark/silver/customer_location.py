from utils.transform_utils import clean_whitespace
from pyspark.sql import DataFrame

def transform_customer_location(customer_location: DataFrame) -> DataFrame:
    # Đổi tên cột, chỉ select những cột cần thiết
    customer_location = customer_location \
        .withColumnRenamed("created_at", "source_created_at") \
        .withColumnRenamed("updated_at", "source_updated_at") \
        .withColumnRenamed("id", "customer_location_id")

    # Handling of null and missing values
    customer_location = customer_location.na.drop()

    # Deduplicate
    customer_location = customer_location.drop_duplicates()

    # Loại bỏ khoảng trắng không cần thiết.
    customer_location = clean_whitespace(df=customer_location)

    return customer_location