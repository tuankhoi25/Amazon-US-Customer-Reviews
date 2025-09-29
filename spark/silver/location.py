from utils.transform_utils import clean_whitespace
from pyspark.sql.functions import col, lpad
from pyspark.sql import DataFrame

def transform_location(location: DataFrame) -> DataFrame:
    # Đổi tên cột, chỉ select những cột cần thiết
    location = location.drop("created_at", "updated_at")
    location = location.withColumnRenamed("id", "location_id")

    # Handling of null and missing values
    location = location.na.drop()

    # Deduplicate
    location = location.drop_duplicates()

    # Loại bỏ khoảng trắng không cần thiết.
    location = clean_whitespace(df=location)

    # Type Casting
    ## Đối datatype cột zipcode int->str. Dùng lpad cho thành 6 số.
    location = location.withColumn("zipcode", lpad(col("zipcode").cast("string"), 6, "0"))
    
    return location