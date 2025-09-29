from utils.transform_utils import clean_whitespace
from pyspark.sql.functions import col, when
from pyspark.sql import DataFrame

def transform_category(category: DataFrame) -> DataFrame:
    # Đổi tên cột, chỉ select những cột cần thiết
    category = category.drop("created_at", "updated_at")
    category = category.withColumnRenamed("id", "category_id")

    # Handling of null and missing values
    category = category.na.drop()

    # Deduplicate
    category = category.drop_duplicates()

    # Loại bỏ khoảng trắng không cần thiết.
    category = clean_whitespace(df=category)

    # Mapping
    ## Chuẩn hoá cột "sex" cho trường hợp tổng hợp từ nhiều nguồn.
    valid_category_name = [
        'Personal_Care_Appliances',
        'Toys',
        'Beauty',
        'Video Games',
        'Digital_Ebook_Purchase',
        'Watches',
        'Pet Products',
        'Grocery',
        'Other',
        'Mobile_Apps',
        'Office Products',
        'Camera',
        'Wireless',
        'Apparel',
        'Automotive',
        'Outdoors',
        'Major Appliances',
        'Furniture',
        'Tools',
        'Books',
        'Musical Instruments',
        'Baby',
        'Health & Personal Care',
        'Sports',
        'Electronics',
        'Mobile_Electronics',
        'Shoes'
    ]

    category = category.withColumn(
        colName="category_name",
        col=when(
                condition=col("category_name").isin(valid_category_name), 
                value=col("category_name")
            ).otherwise(value="Other")
    )
    
    return category