from utils.transform_utils import clean_whitespace
from pyspark.sql.functions import col, when, lower
from pyspark.sql import DataFrame

def transform_review(review: DataFrame) -> DataFrame:
    # Đổi tên cột, chỉ select những cột cần thiết
    review = review.drop("created_at")
    review = review.withColumnRenamed("updated_at", "modified_date").withColumnRenamed("id", "review_id")

    # Handling of null and missing values
    review = review.fillna(
        {
            "helpful_votes": 0,
            "total_votes": 0,
            "marketplace": "Unknown",
            "verified_purchase": "N",
            "review_headline": "",
            "review_body": ""
        }
    )
    review = review.na.drop(subset=["review_id", "customer_id", "product_id", "star_rating"], how="any")

    # Deduplicate
    review = review.drop_duplicates()

    # Loại bỏ khoảng trắng không cần thiết.
    review = clean_whitespace(df=review)

    # Mapping
    ## Chuẩn hoá cột "star_rating" cho trường hợp tổng hợp từ nhiều nguồn.
    review = review.withColumn("star_rating", lower(col("star_rating")))
    review = review.withColumn(
        "star_rating",
        when(col("star_rating").isin("one", "1", "1 star", "1*", "*"), "1")
        .when(col("star_rating").isin("two", "2", "2 stars", "2*", "**"), "2")
        .when(col("star_rating").isin("three", "3", "3 stars", "3*", "***"), "3")
        .when(col("star_rating").isin("four", "4", "4 stars", "4*", "****"), "4")
        .when(col("star_rating").isin("five", "5", "5 stars", "5*", "*****"), "5")
        .otherwise("9")
    )

    ## Chuẩn hoá cột "verified_purchase" cho trường hợp tổng hợp từ nhiều nguồn.
    review = review.withColumn("verified_purchase", lower(col("verified_purchase")))
    review = review.withColumn(
        "verified_purchase",
        when(col("verified_purchase").isin("yes", "y", "true", "t", "1", "verified", "purchased", "confirmed"), "True")
        .when(col("verified_purchase").isin("no", "n", "false", "f", "0", "not verified", "unverified", "not purchased"), "False")
        .otherwise("False")
    )
    
    # Filtering
    ## Loại bỏ những giá trị không xác định của cột star_rating
    review = review.filter(col("star_rating").isin(["1", "2", "3", "4", "5"]))

    # Type Casting
    ## Đổi kiểu dữ liệu "star_rating" thành int
    review = review.withColumn("star_rating", col("star_rating").cast("int"))

    ## Đổi giá trị Y/N của cột "verified_purchase" thành boolean
    review = review.withColumn("verified_purchase", col("verified_purchase").cast("boolean"))

    return review
