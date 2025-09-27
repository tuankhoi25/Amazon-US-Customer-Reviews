# Date Lakehouse and Medallion Architecture

# Bộ dữ liệu
Bộ dữ liệu sẽ là sự kết hợp của nhiều nguồn khác nhau, bao gồm:
- Bộ dữ liệu về các đánh giá sản phẩm được cung cấp bởi Amazon tại [Kaggle](https://www.kaggle.com/datasets/cynthiarempel/amazon-us-customer-reviews-dataset). 
- Kết hợp với các thông tin của người dùng được tạo ra bằng thư viện [Faker](https://faker.readthedocs.io/en/master/index.html#).
- Bộ dữ liệu về thành phố, tiểu bảng, mã zip code của Mỹ được cung cấp bởi [SimpleMaps](https://simplemaps.com/data/us-zips)

**Lưu ý**: vì vấn đề bản quyền của bộ dữ liệu của Amazon trên Kaggle nên không public hầu hết các bảng dữ liệu.

### Đây là bộ dữ liệu sau khi được chuẩn hoá (mô phỏng OLTP):

![OLTP Data Model](https://github.com/user-attachments/assets/86d32816-37fc-47d5-bdcb-242d6c6eb896)
----

# System Architecture

![Lakehouse Architecture](https://github.com/user-attachments/assets/28c8f8e1-ac20-45bb-b3cf-b462e370642e)

----

# Medallion Architecture

## 1.Bronze Layer
Bronze Layer đóng vai trò là nơi lưu trữ dữ liệu thô, chứa toàn bộ lịch sử dữ liệu từ mọi nguồn khác nhau của doanh nghiệp. Bronze Layer có các đặc điểm hính như sau:
  - Dữ liệu được lưu ở Bronze Layer là dữ liệu thô chưa qua xử lý, giữ nguyên định dạng và nội dung từ các nguồn khác nhau (ERP, CRM, IoT, API, file, streaming, v.v.), nhằm bảo toàn toàn bộ lịch sử và tính toàn vẹn của dữ liệu.
  - Bronze Layer giúp giảm tải cho hệ thống nguồn (Data Source) bằng cách chuyển các dữ liệu ít được truy cập hoặc không còn cần thiết cho hoạt động hàng ngày sang Bronze Layer.
  - Đối với những dữ liệu hiện tại doanh nghiệp chưa có chiến lược sử dụng, việc lưu trữ chúng ở Bronze Layer sẽ giúp đỡ phải tiến hành tìm cách xử lý ngay lập tức, mà có thể để dành cho các phân tích trong tương lai. Đồng thời giúp giảm tải cho hệ thống phân tích (OLAP) bằng cách lược bỏ các dữ liệu không cần thiết.
  - Trong Lakehouse Architecture, Bronze Layer thường được triển khai bằng Storage Object hỗ trợ nhiều định dạng như CSV, JSON, Parquet, Avro, ORC, v.v. Nhờ đó, vừa tối ưu chi phí lưu trữ, vừa nâng cao hiệu suất truy xuất.
  - Việc lưu dữ liệu thô tại Bronze Layer giúp doanh nghiệp luôn có khả năng truy xuất lại dữ liệu gốc khi cần kiểm tra, phân tích chuyên sâu hoặc tái xử lý cho các pipeline nâng cao hơn.

### 1.1.Ingestion
Dữ liệu được lưu ở Bronze Layer là kết quả của quá trình ingestion, có 2 loại ingestion khác nhau tuỳ thuộc vào nhu cầu sử dụng, bao gồm:
  - Batch ingestion: nạp dữ liệu theo lô (ví dụ: file CSV, Excel, database dump) theo lịch định kỳ hàng ngày/tuần/tháng.
  - Streaming ingestion: nạp dữ liệu liên tục theo thời gian thực (real-time) từ các nguồn như Kafka, Kinesis, IoT sensors, hoặc log hệ thống.

*Nâng cao hơn sẽ là Lambda Architecture và Kappa Architecture.*

### 1.2.Load Strategy
Tùy thuộc vào đặc điểm dữ liệu và nhu cầu sử dụng, quá trình ingestion vào Bronze Layer có thể được thực hiện theo hai chiến lược:
  - Full Load:
    - Full Load là phương pháp phù hợp cho các bảng dữ liệu có kích thước nhỏ hoặc ít thay đổi. Toàn bộ dữ liệu từ nguồn sẽ được nạp lại vào Bronze Layer trong mỗi lần ingest, thường theo batch định kỳ. 
    - Ưu điểm: đơn giản, dễ triển khai, đảm bảo tính toàn vẹn dữ liệu. 
    - Nhược điểm: tốn nhiều tài nguyên và thời gian nếu áp dụng cho bảng lớn hoặc dữ liệu có khối lượng tăng nhanh.
  - Incremental Load:
    - Incremental Load là phương pháp tối ưu cho các dữ liệu lớn hoặc thay đổi thường xuyên. Chỉ những bản ghi mới hoặc đã được cập nhật kể từ lần ingest trước sẽ được nạp vào Bronze Layer, giúp giảm thiểu khối lượng dữ liệu cần xử lý.
    - Ưu điểm: tiết kiệm tài nguyên, thời gian và chi phí lưu trữ. Phù hợp cho việc lưu trữ dữ liệu lịch sử lâu dài.
    - Nhược điểm: phức tạp hơn trong việc triển khai, cần cơ chế theo dõi thay đổi (Change Data Capture, timestamp, log-based CDC) tại nguồn để đảm bảo tính toàn vẹn và tránh trùng lặp.

### 1.3 Metadata Management
Dữ liệu thô được giữ nguyên trạng, nhưng được bổ sung thêm metadata để hỗ trợ việc quản lý dữ liệu hiệu quả hơn. Các metadata thường bao gồm:
  - Ingestion Timestamp: ghi nhận chính xác thời điểm dữ liệu được nạp vào hệ thống.  
  - Source Identifier: cho biết nguồn gốc của dữ liệu.  
  - Batch Identifier: mã định danh duy nhất gắn cho mỗi lần nạp dữ liệu theo batch.  

Việc bổ sung metadata này đóng vai trò quan trọng trong việc theo dõi dòng chảy dữ liệu (data lineage) và quản lý vòng đời dữ liệu (data lifecycle).

### 1.4 Schema Enforcement
Schema Enforcement là quá trình áp đặt và kiểm soát chặt chẽ cấu trúc dữ liệu (schema) khi nạp vào hệ thống, nhằm đảm bảo dữ liệu tuân thủ các định nghĩa về số lượng cột, tên cột và kiểu dữ liệu. Tại Bronze Layer, có hai hướng tiếp cận phổ biến:
  - Trường hợp 1: Không áp dụng Schema Enforcement
    - Khi định nghĩa các bảng ban đầu bằng DDL script, ta sẽ xem các bảng gốc ở nguồn (Data Source) rồi tiến hành viết DDL script với cùng tên bảng, cùng số lượng cột, cùng tên cột, nhưng kiểu dữ liệu (Data Type) là STRING.
    - Nếu schema thay đổi ở các lần ingestion sau thì nó vẫn sẽ tự thêm columns với data type là STRING nhờ các chức năng sau:
        - Bật chức năng Auto Merge Schema của Iceberg để nó tự động thêm cột nếu như có thêm cột thì nó tự động tạo thêm trong table tương ứng ở bronze layer (nếu không mở thì ingestion sẽ bị lỗi do không khớp schema)
        - Đối với đọc các file như csv thì tắt chức năng Infer Schema của Spark để mọi dữ liệu được đọc đều có data type là STRING (nếu không thì khi thêm các cột mới không có trong DDL script ban đầu thì Spark sẽ tự suy luận kiểu dữ liệu cho cột đó)
        - Đối với các nguồn đã có schema sẵn như JDBC, ORC, Parquet, Avro, Delta, Hive…, Spark sẽ tự lấy schema định nghĩa, do đó cần đọc ra xong rồi cast lại kiểu dữ liệu về STRING.
  - Trường hợp 2: Có áp dụng Schema Enforcement
    - Khi định nghĩa các bảng ban đầu bằng DDL script, ta xem các table gốc ở data source rồi tiến hành viết DDL script với cùng tên bảng, cùng số lượng cột, cùng tên cột, cùng kiểu dữ liệu.
    - Nếu schema thay đổi ở các lần ingestion sau: các team quản lý data source cần báo trước cho team data để học thực hiện câu lệnh tạo/sửa/xoá cột tương ứng ở bronze layer trước khi ingestion.

#### So sánh & Đánh giá:
  - Trường hợp 1:
    - Ưu điểm: Linh hoạt, dễ dàng tiếp nhận dữ liệu từ nguồn không ổn định (CSV, JSON log, Excel thủ công). Các định dạng dữ liệu phức tạp (như Oracle NUMBER không cố định precision/scale) có thể giữ nguyên dưới dạng STRING, tránh mất mát thông tin trước khi chuẩn hoá ở Silver.
    - Nhược điểm: Tốn tài nguyên tính toán (mọi giá trị phải cast sang STRING trước khi convert lại về kiểu chuẩn ở Silver). Tốn dung lượng lưu trữ (STRING chiếm nhiều byte hơn kiểu native như INT, FLOAT, DATE, BOOLEAN), làm tăng kích thước file Parquet/Delta.
  - Trường hợp 2:
    - Ưu điểm: Dữ liệu lưu trữ tối ưu, giữ đúng kiểu dữ liệu từ đầu giúp tiết kiệm tài nguyên và lưu trữ.
    - Nhược điểm: tốn thời gian để bàn bạc giữa các team khi có thay đổi schema, đặc biệt là đối với các công ty lớn có nhiều team hoặc tần suất thay đổi schema cao và logic phức tạp. Nhưng đối với những thay đổi schema đơn giản thì có thể chìa endpoint ra để các team khác tự động hoá việc này.

#### Lưu ý đặc biệt
Trong một số trường hợp, kiểu dữ liệu của cùng một cột ở hệ thống nguồn có thể thay đổi theo thời gian. Ví dụ: cột A ban đầu được định nghĩa là INT, nhưng sau một thời gian do thay đổi logic kinh doanh, nó lại chuyển sang BOOLEAN.
  - Nếu áp dụng TH2 (Schema Enforcement nghiêm ngặt), ingestion sẽ không thể lưu được vì không cho phép một cột vừa mang kiểu INT vừa mang kiểu BOOLEAN.
  - Nếu áp dụng TH1 (toàn bộ lưu dạng STRING), dữ liệu từ năm 2010 đến 2025 vẫn được giữ nguyên, nhưng khi xử lý ETL về Silver Layer, hệ thống phải liên tục cast từ STRING sang BOOLEAN, gây lãng phí tài nguyên.

Giải pháp: tạo thêm một cột mới để phản ánh sự thay đổi
  - Giữ lại A_int (INT) để lưu dữ liệu lịch sử.
  - Cột A (BOOLEAN) từ giờ sẽ dùng để lưu dữ liệu mới sau khi chuyển đổi.

Cách xử lý này đảm bảo vừa bảo toàn dữ liệu gốc, vừa tối ưu hiệu năng khi khai thác. Nếu các cột khác cũng gặp tình huống thay đổi định dạng/kiểu dữ liệu, ta áp dụng tương tự.

## 2.Silver Layer
Silver Layer là nơi lưu trữ dữ liệu đã được làm sạch, chuẩn hoá và tích hợp từ Bronze Layer. Đây là tầng xử lý trung gian, giúp biến dữ liệu thô thành dữ liệu có cấu trúc ổn định, đáng tin cậy để phục vụ phân tích hoặc chuyển tiếp sang Gold Layer.

Tùy thuộc vào logic kinh doanh và nhu cầu của từng tổ chức, một số bước có thể được lược bỏ hoặc bổ sung thêm. Các công việc thường gặp ở Silver Layer bao gồm:
  - Schema evolution
  - Schema enforcement
    - Schema Validation
    - Data Type Validation
  - Handling of null and missing values
  - Data deduplication
  - Resolution of out-of-order and late-arriving data issues
  - Data quality checks and enforcement
    - Data Standardization
      - Invalid values handling
      - Type casting
    - Business Rules Validation
      - Range and Constraint Validation
      - Cross-field Validation
      - Uniqueness Validation
    - Referential Integrity
    - Conformance
  - Data Enrichment: thường là import thêm từ nhiều nguồn khác thôi (API, crm, erp) chứ không phải là tạo derived column (làm ở Gold Layer)
  - Data Quality Checks: kiểm tra chất lượng dữ liệu (completeness, accuracy, consistency, timeliness, validity). Đây là concept advanced dùng để đảm bảo dữ liệu đạt chuẩn trước khi chuyển sang Gold Layer.
  - Slowly Changing Dimensions (SCD): khá tuỳ, nếu các team khác như là Data Scientist, Machine Learning có nhu cầu dùng dữ liệu lịch sử ở dạng chưa phi chuẩn hoá (Denormalize) thì mới áp dụng SCD ở Silver Layer. Nếu không thì nên giữ nguyên chuẩn 3NF để dễ dàng bảo trì, mở rộng và tránh dư thừa dữ liệu, áp dụng SCD ở Gold Layer.

*Best Practice: Ở Silver Layer, nên bóc tách và chia nhỏ các logic phức tạp của doanh nghiệp thành từng bước xử lý riêng biệt. Cách này giúp dễ dàng bảo trì, tái sử dụng và kiểm thử, đồng thời giảm rủi ro khi thay đổi hệ thống.*

## 3.Gold Layer
Gold Layer là nơi lưu trữ dữ liệu đã được tinh chỉnh và tối ưu hóa nhằm phục vụ trực tiếp cho nhu cầu phân tích, báo cáo, dashboard cũng như các ứng dụng phân tích nâng cao (ML, AI). Ở lớp này, dữ liệu không còn giữ nguyên trạng như Raw hay Staging, mà được tái cấu trúc theo ngữ nghĩa kinh doanh để người dùng cuối dễ dàng khai thác.

Dữ liệu trong Gold Layer thường được tổ chức theo mô hình sao (Star Schema) hoặc mô hình bông tuyết (Snowflake Schema). Đây là các mô hình phổ biến trong kho dữ liệu, cho phép:
  - Tách rõ bảng Fact (lưu trữ các số liệu, giao dịch, sự kiện) và bảng Dimension (lưu trữ ngữ cảnh, danh mục, thuộc tính).
  - Tối ưu hóa cho các phép tổng hợp (aggregation) và truy vấn phân tích.
  - Hỗ trợ tốt cho công cụ BI (Power BI, SSAS, Tableau…) trong việc định nghĩa measure, KPI, và drill-down theo nhiều chiều dữ liệu.

Các công việc chính ở Gold Layer:
  - Aggregated Data: Dữ liệu thường được tổng hợp (SUM, AVG, COUNT…) theo các chiều phân tích phổ biến (theo tuần, theo tháng, theo khu vực…) và có thể được triển khai dưới dạng view hoặc fact aggregate table để phục vụ nhanh cho báo cáo.
  - Enriched Data: Dữ liệu được xử lý, làm sạch và bổ sung thêm các thông tin giá trị gia tăng (ví dụ: phân loại sản phẩm ABC, phân nhóm khách hàng, phân vùng thị trường).
  - Business-Level Aggregation: Dữ liệu được tổng hợp và trình bày theo đúng logic nghiệp vụ (ví dụ: Doanh thu theo kênh phân phối, P&L theo công ty CNX/HST, chi phí sản xuất theo dây chuyền).
  - Denormalized Structure: Cấu trúc dữ liệu thường được phi chuẩn hóa (denormalized) để giảm số lượng join, giúp truy vấn nhanh hơn và đơn giản hơn cho người dùng BI.
  - Query Optimization: Dữ liệu được thiết kế, phân vùng (partitioning), lập chỉ mục (indexing) hoặc tạo materialized view để đảm bảo hiệu năng tốt nhất cho các báo cáo có tần suất truy cập cao.
  - Slowly Changing Dimensions (SCD): Các dimension được quản lý theo cơ chế SCD (Type 1, Type 2…) để theo dõi thay đổi lịch sử (ví dụ: khách hàng đổi tên công ty, sản phẩm đổi bao bì).
----

# BI & Reporting
![BI & Reporting](https://github.com/user-attachments/assets/28269e14-542b-417c-9076-836c1dfac463)