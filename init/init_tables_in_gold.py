from pyspark.sql import SparkSession

def init_tables_in_gold(spark: SparkSession):

    spark.sql("""
        CREATE TABLE nessie.gold.dim_customer (
            customer_key BIGINT,
            customer_id BIGINT,
            name STRING,
            sex STRING,
            mail STRING,
            birthdate DATE,
            signup_date DATE
        )
    """)

    spark.sql("""
        CREATE TABLE nessie.gold.dim_date (
            date_key BIGINT,
            full_date DATE,
            day INT,
            month INT,
            month_name STRING,
            quarter INT,
            quarter_name STRING,
            year INT,
            day_of_week INT,
            day_name STRING,
            week_of_year INT,
            is_weekend BOOLEAN,
            is_holiday BOOLEAN
        )
    """)

    spark.sql("""
        CREATE TABLE nessie.gold.dim_location (
            location_key BIGINT,
            location_id STRING,
            street_address STRING,
            city STRING,
            state STRING,
            country STRING,
            zipcode STRING
        )
    """)

    spark.sql("""
        CREATE TABLE nessie.gold.bridge_customer_location (
            customer_location_key BIGINT,
            customer_location_id BIGINT,
            location_key INT,
            customer_key INT,
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN
        )
    """)

    spark.sql("""
        CREATE TABLE nessie.gold.dim_product (
            product_key BIGINT,
            product_id STRING,
            product_title STRING,
            currency STRING,
            price DECIMAL(10, 2),
            valid_from DATE,
            valid_to DATE,
            is_current BOOLEAN
        )
    """)

    spark.sql("""
        CREATE TABLE nessie.gold.dim_category (
            category_key BIGINT,
            category_id BIGINT,
            category_name STRING
        )
    """)

    spark.sql("""
        CREATE TABLE nessie.gold.bridge_product_category (
            product_category_key BIGINT,
            product_category_id BIGINT,
            product_key INT,
            category_key INT
        )
    """)

    spark.sql("""
        CREATE TABLE nessie.gold.fact_review (
            review_key BIGINT,
            review_id STRING,
            product_key INT,
            customer_key INT,
            date_key INT,
            star_rating INT,
            helpful_votes INT,
            total_votes INT,
            marketplace STRING,
            verified_purchase BOOLEAN,
            review_headline STRING,
            review_body STRING,
            is_current BOOLEAN
        )
    """)