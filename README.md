# Fighting Review Manipulation on Your E-Commerce Platform

# Bộ dữ liệu
Bộ dữ liệu về các đánh giá sản phẩm được cung cấp bởi Amazon tại [Kaggle](https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset?select=amazon_reviews_us_Grocery_v1_00.tsv). 

Lưu ý: vì vấn đề bản quyền của bộ dữ liệu của Amazon nên mình chỉ có thể public những file data được generate ra.

Sau khi giảm độ lớn của dữ liệu xuống mức phù hợp với máy tính cá nhân và generate thêm thông tin về khách hàng và giá sản phẩm bằng [thư viện Faker](https://faker.readthedocs.io/en/master/index.html) và tiến hành chuẩn hoá cơ sở dữ liệu cho phù hợp với OLTP thì ta có như sau:

![Ảnh màn hình 2025-07-19 lúc 16.33.02.png](https://github.com/user-attachments/assets/8f23d828-910e-4239-8b98-43895d368bcf)