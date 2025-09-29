from utils.transform_utils import clean_whitespace
from pyspark.sql import DataFrame

def transform_product_category(product_category: DataFrame) -> DataFrame:
    # Đổi tên cột, chỉ select những cột cần thiết
    product_category = product_category.drop("created_at", "updated_at")
    product_category = product_category.withColumnRenamed("id", "product_category_id")

    # Handling of null and missing values
    product_category = product_category.na.drop()

    # Deduplicate
    product_category = product_category.drop_duplicates()

    # Loại bỏ khoảng trắng không cần thiết.
    product_category = clean_whitespace(df=product_category)
    
    return product_category