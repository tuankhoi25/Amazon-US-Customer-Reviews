from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col, max
from typing import List
import textwrap

def generate_surrogate_key(
    source: DataFrame,
    target: DataFrame,
    sk_name: str,
    sort_cols: List[str]
) -> DataFrame:
    # Get max surrogate key in source table
    max_sk = target.select(max(sk_name)).collect()[0][0]
    max_sk = max_sk if max_sk is not None else 0

    # Generate sk in silver table
    source = source \
        .withColumn(sk_name, row_number().over(Window().orderBy(*sort_cols)) + max_sk) \
        .withColumn(sk_name, col(sk_name).cast("long"))
    
    return source

def switch_bk_sk(
    bridge_table: DataFrame,
    dim_table: DataFrame,
    bk_name: str,
    sk_name: str
) -> DataFrame:
    bridge_table = bridge_table.join(
        other=dim_table.select(bk_name, sk_name),
        on=bk_name,
        how="inner"
    )
    bridge_table = bridge_table.drop(bk_name)
    return bridge_table


# Ở trường hợp SCD1 (nghĩa là dữ liệu mới sẽ ghi đè lên dữ liệu cũ) thì ở OLTP cũng ghi đè thôi chứ không có bật CDC lên
# -> Do vậy thì mỗi lần incremental load dựa trên cột updated_at thì mỗi dim_ID sẽ chỉ xuất hiện 1 lần
# -> Xài MERGE INTO vô tư mà không cần phải xử lý deduplicate
# Còn nếu SCD1 nhưng ở OLTP lỡ bật CDC thì chỉ cần xử lý deduplicate bằng cách chỉ lấy dim_ID có updated_at lớn nhất
# Lưu ý: schema của Dim này chỉ hơn cái schema ở silver mỗi cái key thôi
def SCD1(
    spark: SparkSession,
    source: DataFrame,
    source_name: str,
    target: DataFrame,
    target_name: str,
    sk_name: str,
    bk_name: str
):
    # Generate surrogate key base on max(surrogate_key) in source table
    source = generate_surrogate_key(source=source, target=target, sk_name=sk_name, sort_cols=[bk_name])

    # Tiến hành SCD1, match thì overwrite, miss match thì insert
    merge_expr = textwrap.dedent(f"""
        MERGE INTO nessie.gold.{target_name} g
        USING {source_name} s
        ON s.{bk_name} = g.{bk_name}
        WHEN MATCHED THEN 
            UPDATE SET {", ".join([f"g.{column} = s.{column}" for column in target.columns if column != sk_name])}
        WHEN NOT MATCHED THEN 
            INSERT ({", ".join(sorted(target.columns))})
            VALUES ({", ".join(f"s.{column}" for column in sorted(target.columns))})
    """)
    print(merge_expr)
    
    # Exec expr
    source.createOrReplaceTempView(source_name)
    spark.sql(merge_expr)
    spark.catalog.dropTempView(source_name)
    
    return True


def scd2(
    spark: SparkSession,
    source: DataFrame,
    source_name: str,
    target: DataFrame,
    target_name: str,
    bk_name: str,
    sk_name: str,
) -> bool:
    # Generate surrogate key base on max(surrogate_key) in source table
    source = generate_surrogate_key(source=source, target=target, sk_name=sk_name, sort_cols=[bk_name])

    # Tạo df chỉ chứa phiên bản cũ nhất của ID trong source table và Cập nhật bản mới nhất ở dim thành bản cũ
    w = Window.partitionBy(bk_name).orderBy(F.col("source_updated_at").asc())
    earliest_record_in_source = source.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")
    earliest_record_in_source.createOrReplaceTempView(source_name)

    expr = textwrap.dedent(f"""
        MERGE INTO nessie.gold.{target_name} g
        USING {source_name} s
        ON s.{bk_name} = g.{bk_name}
        WHEN MATCHED AND g.is_current = True THEN 
            UPDATE SET g.is_current = False, g.valid_to = s.source_updated_at
    """)
    df = spark.sql(expr)
    spark.catalog.dropTempView(source_name)

    # Insert toàn bộ source vào target
    w = Window.partitionBy(bk_name).orderBy(F.col("source_updated_at").desc())
    source = source \
                    .withColumn("valid_from", col("source_updated_at")) \
                    .withColumn("valid_to", F.lag("valid_from").over(w)) \
                    .drop("source_created_at", "source_updated_at") \
                    .withColumn("is_current", when(col("valid_to").isNull(), True).otherwise(False))
    source.writeTo(f"nessie.gold.{target_name}").append()
    return True


def fact(
    spark: SparkSession,
    source: DataFrame,
    target: DataFrame,
    source_name: str,
    target_name: str,
    bk_name: str,
    sk_name: str,
    has_cur_col: bool
):
    # Generate surrogate key base on max(surrogate_key) in source table
    source = generate_surrogate_key(source=source, target=target, sk_name=sk_name, sort_cols=[bk_name])

    # Chỉnh is_current = False nếu có
    source.createOrReplaceTempView(source_name)
    expr = textwrap.dedent(f"""
        MERGE INTO nessie.gold.{target_name} g
        USING {source_name} s
        ON s.{bk_name} = g.{bk_name}
        WHEN MATCHED AND g.is_current = True AND {has_cur_col} = True THEN 
            UPDATE SET g.is_current = False
    """)
    spark.sql(expr)
    spark.catalog.dropTempView(source_name)
    print(expr)
    
    ## Append
    if "is_current" in target.columns:
        source = source.withColumn("is_current", F.lit(True))
    source.writeTo(f"nessie.gold.{target_name}").append()
    
    return True