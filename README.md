# Fighting Review Manipulation on Your E-Commerce Platform

# Bộ dữ liệu
Bộ dữ liệu về các đánh giá sản phẩm được cung cấp bởi Amazon tại [Kaggle](https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset?select=amazon_reviews_us_Grocery_v1_00.tsv). 

Lưu ý: vì vấn đề bản quyền của bộ dữ liệu của Amazon nên mình chỉ có thể public những file data được generate ra.

Sau khi giảm độ lớn của dữ liệu xuống mức phù hợp với máy tính cá nhân và generate thêm thông tin về khách hàng và giá sản phẩm bằng [thư viện Faker](https://faker.readthedocs.io/en/master/index.html) và tiến hành chuẩn hoá cơ sở dữ liệu cho phù hợp với OLTP thì ta có như sau:

![OLTP Data Model](https://github.com/user-attachments/assets/8f23d828-910e-4239-8b98-43895d368bcf)
----

# Lakehouse Architecture

![Lakehouse Architecture](https://github.com/user-attachments/assets/bc8b8545-d4fa-4e14-aaa5-d8a64690b349)
----

# Cách kết nối Trino với DBeaver
1. Cài đặt DBeaver
2. Tạo kết nối mới
   - Mở DBeaver → Database → New Database Connection
   - Tìm và chọn Trino
   - Cấu hình tab Main
   - Host: localhost
   - Port: 8085
   - Database/Schema: (để trống hoặc nhập iceberg.test_schema nếu muốn)
   - Username: nhập gì cũng được (ví dụ: admin)
   - Password: để trống
3. Test kết nối
   - Bấm Test Connection
   - Nếu DBeaver hỏi tải driver thì chọn Yes để tải
   - Lưu và kết nối
   - Bấm OK để lưu cấu hình và bắt đầu kết nối

----
# Tài liệu tham khảo

[Config spark with local mode](https://www.linkedin.com/pulse/creating-local-data-lakehouse-using-alex-merced/)

[Check Dockerfile's content](https://hub.docker.com/layers/alexmerced/spark33-notebook/latest/images/sha256-3c83b5963a633430a48628bd7695893d167de661b371dc15356474f4ec878e1f)

[Tham khảo triển khai trino](https://trino.io/docs/current/installation/containers.html)

[Tham khảo config trino](https://projectnessie.org/iceberg/trino.html)

[Tham khảo config trino với iceberg](https://trino.io/docs/current/connector/iceberg.html)

[Tham khảo config minio](https://trino.io/docs/current/object-storage/file-system-s3.html)

[Tham khảo triển khai cluster trino](https://trino.io/docs/current/installation/deployment.html)