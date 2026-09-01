# Giai đoạn A: Hạ tầng (Kafka + MinIO)

## Đã làm
- [x] docker-compose.yml: Kafka (KRaft mode, 1 broker) + Kafka UI + MinIO + minio-init
- [x] .env chứa toàn bộ config port/credentials
- [x] Script init-buckets.sh tự tạo bronze/silver/gold khi MinIO khởi động
- [x] Script check_kafka_topics.sh, check_minio_buckets.sh để verify nhanh

## Kiến trúc network
- Container nội bộ dùng hostname: `kafka:29092`, `minio:9000`
- Máy host (terminal, browser) dùng: `localhost:9092`, `localhost:9001` (console), `localhost:9000` (API)

## Endpoint truy cập
- Kafka bootstrap (host): localhost:9092
- Kafka bootstrap (internal): kafka:29092
- Kafka UI: http://localhost:8080
- MinIO API: http://localhost:9000
- MinIO Console: http://localhost:9001 (user: minioadmin)

## Bucket đã tạo
- bronze, silver, gold

## Việc cần làm tiếp (Giai đoạn B)
- Viết Event Generator Python, cấu hình producer trỏ tới kafka:29092
- Tạo 4 topic thật: orders, payments, inventory, customers
