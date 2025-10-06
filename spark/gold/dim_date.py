import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, dayofmonth, month, quarter, concat, lit, year, dayofweek, weekofyear
from utils.spark_utils import get_or_create_spark_session

def build_dim_date(spark: SparkSession, start_date: datetime, end_date: datetime):
    start_date = start_date.strftime("%Y-%m-%d")
    end_date = end_date.strftime("%Y-%m-%d")
    
    df_date = spark.sql(f"SELECT explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as full_date") \
        .withColumn("date_key", date_format("full_date", "yyyyMMdd").cast("long")) \
        .withColumn("day", dayofmonth("full_date")) \
        .withColumn("month", month("full_date")) \
        .withColumn("month_name", date_format("full_date", "MMMM")) \
        .withColumn("quarter", quarter("full_date")) \
        .withColumn("quarter_name", concat(lit("Q"), quarter("full_date"))) \
        .withColumn("year", year("full_date")) \
        .withColumn("day_of_week", ((dayofweek("full_date")+5)%7)+1) \
        .withColumn("day_name", date_format("full_date", "EEEE")) \
        .withColumn("week_of_year", weekofyear("full_date")) \
        .withColumn("is_weekend", col("day_of_week").isin(1,7)) \
        .withColumn("is_holiday", lit(False))

    df_date.writeTo(f"nessie.gold.dim_date").append()

def main():
    parser = argparse.ArgumentParser(description="Generate dim_date table")
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    args = parser.parse_args()

    spark = get_or_create_spark_session("gold_dim_date")
    start_date = datetime.strptime(args.start, "%Y-%m-%d")
    end_date = datetime.strptime(args.end, "%Y-%m-%d")

    build_dim_date(spark, start_date, end_date)

if __name__ == "__main__":
    main()