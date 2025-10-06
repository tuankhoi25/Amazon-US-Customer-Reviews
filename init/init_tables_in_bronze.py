from pyspark.sql import SparkSession

def init_tables_in_bronze(spark: SparkSession):

    spark.sql("""
        CREATE TABLE nessie.bronze.customer (
            id BIGINT,
            name STRING,
            sex STRING,
            mail STRING,
            birthdate DATE,
            login_username STRING,
            login_password STRING,
            created_at DATE,
            updated_at DATE,
            _ingested_at TIMESTAMP,
            _batch_id STRING,
            _is_deleted BOOLEAN
        )
        USING iceberg
        TBLPROPERTIES ('write.spark.accept-any-schema'='true')
    """)

    spark.sql("""
        CREATE TABLE nessie.bronze.location (
            id STRING,
            street_address STRING,
            city STRING,
            state STRING,
            zipcode INT,
            country STRING,
            created_at DATE,
            updated_at DATE,
            _ingested_at TIMESTAMP,
            _batch_id STRING,
            _is_deleted BOOLEAN
        )
        USING iceberg
        TBLPROPERTIES ('write.spark.accept-any-schema'='true')
    """)

    spark.sql("""
        CREATE TABLE nessie.bronze.customer_location (
            id BIGINT,
            customer_id BIGINT,
            location_id STRING,
            created_at DATE,
            updated_at DATE,
            _ingested_at TIMESTAMP,
            _batch_id STRING,
            _is_deleted BOOLEAN
        )
        USING iceberg
        TBLPROPERTIES ('write.spark.accept-any-schema'='true')
    """)

    spark.sql("""
        CREATE TABLE nessie.bronze.phone_number (
            id STRING,
            phone_number STRING,
            created_at DATE,
            updated_at DATE,
            _ingested_at TIMESTAMP,
            _batch_id STRING,
            _is_deleted BOOLEAN
        )
        USING iceberg
        TBLPROPERTIES ('write.spark.accept-any-schema'='true')
    """)

    spark.sql("""
        CREATE TABLE nessie.bronze.customer_phone (
            id BIGINT,
            customer_id BIGINT,
            phone_id STRING,
            created_at DATE,
            updated_at DATE,
            _ingested_at TIMESTAMP,
            _batch_id STRING,
            _is_deleted BOOLEAN
        )
        USING iceberg
        TBLPROPERTIES ('write.spark.accept-any-schema'='true')
    """)

    spark.sql("""
        CREATE TABLE nessie.bronze.shadow_product (
            id STRING,
            product_id STRING,
            product_title STRING,
            currency STRING,
            price DECIMAL(10, 2),
            created_at DATE,
            updated_at DATE,
            _ingested_at TIMESTAMP,
            _batch_id STRING,
            _is_deleted BOOLEAN
        )
        USING iceberg
        TBLPROPERTIES ('write.spark.accept-any-schema'='true')
    """)

    spark.sql("""
        CREATE TABLE nessie.bronze.category (
            id BIGINT,
            category_name STRING,
            created_at DATE,
            updated_at DATE,
            _ingested_at TIMESTAMP,
            _batch_id STRING,
            _is_deleted BOOLEAN
        )
        USING iceberg
        TBLPROPERTIES ('write.spark.accept-any-schema'='true')
    """)

    spark.sql("""
        CREATE TABLE nessie.bronze.product_category (
            id BIGINT,
            product_id STRING,
            category_id BIGINT,
            created_at DATE,
            updated_at DATE,
            _ingested_at TIMESTAMP,
            _batch_id STRING,
            _is_deleted BOOLEAN
        )
        USING iceberg
        TBLPROPERTIES ('write.spark.accept-any-schema'='true')
    """)

    spark.sql("""
        CREATE TABLE nessie.bronze.review (
            id STRING,
            customer_id BIGINT,
            product_id STRING,
            star_rating STRING,
            helpful_votes INT,
            total_votes INT,
            marketplace STRING,
            verified_purchase STRING,
            review_headline STRING,
            review_body STRING,
            created_at DATE,
            updated_at DATE,
            _ingested_at TIMESTAMP,
            _batch_id STRING,
            _is_deleted BOOLEAN
        )
        USING iceberg
        TBLPROPERTIES ('write.spark.accept-any-schema'='true')
    """)