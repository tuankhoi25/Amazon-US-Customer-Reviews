from utils.transform_utils import clean_whitespace
from pyspark.sql import DataFrame

def transform_product(product: DataFrame) -> DataFrame:
    # Đổi tên cột, chỉ select những cột cần thiết
    product = product.drop("id")
    product = product \
        .withColumnRenamed("created_at", "source_created_at") \
        .withColumnRenamed("updated_at", "source_updated_at")

    # Handling of null and missing values
    product = product.na.drop()

    # Deduplicate
    product = product.drop_duplicates()

    # Loại bỏ khoảng trắng không cần thiết.
    product = clean_whitespace(df=product)

    return product