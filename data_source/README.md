customer_kaggle_datasets là bộ dữ liệu từ kaggle đã được lược bớt để phù hợp với máy tính cá nhân và đã được generate thêm thông tin về khách hàng và giá sản phẩm bằng thư viện Faker. Bộ dữ liệu này được chuẩn hoá để phù hợp với mô hình OLTP.

postgres_data là thư mục chứa dữ liệu đã được chuẩn hoá và lưu trữ trong PostgreSQL. Thư mục này bao gồm các bảng dữ liệu như customers, products, reviews, v.v., được sử dụng để phân tích và truy vấn dữ liệu đánh giá sản phẩm.

posgres_data là dữ liệu của hai thư mục trên, được load và bind mount sẵn vào máy tính để không phải tải lại khi chạy Docker.