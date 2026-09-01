# Giai đoạn C: Spark Structured Streaming — Kafka → Bronze (4 topic)

## Đã làm
- [x] Docker image Spark (apache/spark:3.5.1-python3) + 6 JAR connector Kafka/S3A
- [x] Cluster Spark standalone: 1 master + 1 worker (6 core, 4GB)
- [x] spark_session.py, schemas.py dùng chung cho mọi job
- [x] 4 streaming job: orders/payments/inventory/customers_to_bronze.py
- [x] Mỗi job là 1 service Docker riêng, restart: unless-stopped
- [x] Verify: schema đúng, dữ liệu đúng logic nghiệp vụ, cả 4 bucket con có file .parquet

## Việc cần làm tiếp (Giai đoạn D)
- Đọc lại Bronze (không phải Kafka nữa) -> làm sạch -> Silver
- Tính window aggregation (doanh thu mỗi 5 phút...) -> Gold
