from utils.transform_utils import clean_whitespace
from pyspark.sql.functions import col, when, lower
from pyspark.sql import DataFrame

def transform_customer(customer: DataFrame) -> DataFrame:
    # Đổi tên cột, chỉ select những cột cần thiết
    customer = customer.drop("login_username", "login_password", "updated_at")
    customer = customer.withColumnRenamed("created_at", "signup_date").withColumnRenamed("id", "customer_id")

    # Handling of null and missing values
    customer = customer.fillna(
        {
            "name": "Unknown",
            "sex": "Other",
            "mail": "unknown@gmail.com",
            "birthdate": "1900-01-01"
        }
    )
    customer = customer.na.drop(subset=["customer_id"], how="any")

    # Deduplicate
    customer = customer.drop_duplicates()

    # Loại bỏ khoảng trắng không cần thiết.
    customer = clean_whitespace(df=customer)

    # Mapping
    ## Chuẩn hoá cột "sex" cho trường hợp tổng hợp từ nhiều nguồn.
    customer = customer.withColumn("sex", lower(col("sex")))
    customer = customer.withColumn(
        "sex",
        when(col("sex").isin("1", "m", "male"), "1")
        .when(col("sex").isin("2", "f", "female"), "2")
        .when(col("sex").isin("3", "o", "other"), "3")
        .otherwise("3")
    )
    
    return customer