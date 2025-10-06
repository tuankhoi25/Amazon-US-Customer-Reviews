from pyspark.sql import SparkSession

def init_tables_in_silver(spark: SparkSession):

    spark.sql("""
        CREATE TABLE nessie.silver.customer (
            customer_id BIGINT,
            name STRING,
            sex STRING,
            mail STRING,
            birthdate DATE,
            signup_date DATE
        )
    """)

    spark.sql("""
        CREATE TABLE nessie.silver.location (
            location_id STRING,
            street_address STRING,
            city STRING,
            state STRING,
            country STRING,
            zipcode STRING
        )
    """)

    spark.sql("""
        CREATE TABLE nessie.silver.customer_location (
            customer_location_id BIGINT,
            customer_id BIGINT,
            location_id STRING,
            source_created_at DATE,
            source_updated_at DATE
        )
    """)

    spark.sql("""
        CREATE TABLE nessie.silver.product (
            product_id STRING,
            product_title STRING,
            currency STRING,
            price DECIMAL(10, 2),
            source_created_at DATE,
            source_updated_at DATE
        )
    """)

    spark.sql("""
        CREATE TABLE nessie.silver.category (
            category_id BIGINT,
            category_name STRING
        )
    """)

    spark.sql("""
        CREATE TABLE nessie.silver.product_category (
            product_category_id BIGINT,
            product_id STRING,
            category_id BIGINT
        )
    """)

    spark.sql("""
        CREATE TABLE nessie.silver.review (
            review_id STRING,
            customer_id BIGINT,
            product_id STRING,
            star_rating INT,
            helpful_votes INT,
            total_votes INT,
            marketplace STRING,
            verified_purchase BOOLEAN,
            review_headline STRING,
            review_body STRING,
            modified_date DATE
        )
    """)