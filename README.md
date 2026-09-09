# dataeng-pipeline

End-to-end data engineering pipeline mô phỏng một hệ thống thương mại điện
tử: sinh sự kiện business real-time → streaming qua Kafka → Medallion data
lake (Bronze/Silver/Gold trên MinIO) → nạp vào Snowflake → (sắp tới) dbt +
BI dashboard.

## Kiến trúc

```
Event Generator (Python) → Kafka (4 topics) → Spark Structured Streaming
   → Bronze (raw) → Silver (cleaned) → Gold (aggregated)  [MinIO]
   → Airflow (hourly) → Snowflake RAW
   → dbt staging → star schema → BI dashboard   [sắp tới]
```

Kết hợp **Medallion architecture** (Bronze/Silver/Gold) với mô hình
**Lambda**: Gold trên MinIO là speed layer phục vụ dashboard gần-real-time;
Snowflake + dbt là serving layer phục vụ báo cáo đã chuẩn hóa (star schema).

## Trạng thái

| Giai đoạn | Nội dung | Trạng thái |
|---|---|---|
| A | Hạ tầng Docker: Kafka (KRaft) + MinIO | ✅ |
| B | Event Generator (Python) → Kafka, 4 topic | ✅ |
| C | Spark Streaming: Kafka → Bronze | ✅ |
| D | Spark: Bronze → Silver (clean) → Gold (aggregate) | ✅ |
| E | Airflow: Gold (MinIO) → Snowflake RAW | ✅ |
| F | dbt: staging models + test + lineage | ⬜ |
| G | dbt: star schema (fact/dim, SCD Type 2) | ⬜ |
| H | BI dashboard + Grafana/alerting | ⬜ |

Ghi chú học tập chi tiết từng giai đoạn: [`docs/`](./docs).

## Cách chạy

```bash
# Hạ tầng
docker compose up -d kafka kafka-ui minio minio-init
bash scripts/check_kafka_topics.sh
bash scripts/check_minio_buckets.sh

# Spark cluster + streaming jobs (Bronze, Silver, Gold)
docker compose up -d spark-master spark-worker \
  orders-to-bronze payments-to-bronze inventory-to-bronze customers-to-bronze \
  orders-cleaning payments-cleaning inventory-cleaning \
  revenue-5min-window

# Event generator
cd event_generator && python main.py

# Airflow (Gold → Snowflake): xem airflow/
```

## Tech stack

Kafka (KRaft) · Spark Structured Streaming · Airflow · MinIO · Snowflake ·
AWS EC2 + Terraform · Python/PySpark
Sắp tới: dbt · Power BI/Looker · Grafana

## Cấu trúc thư mục

```
dataeng-pipeline/
├── docker-compose.yml
├── docker/            # Dockerfile riêng từng service
├── event_generator/   # Giai đoạn B
├── spark_jobs/         # Giai đoạn C, D (bronze/, silver/, gold/)
├── airflow/             # Giai đoạn E
├── snowflake/             # Giai đoạn E, F (setup SQL)
├── dbt_project/             # Giai đoạn F, G (sắp tới)
├── bi/                        # Giai đoạn H (sắp tới)
├── monitoring/                  # Giai đoạn H (sắp tới)
├── scripts/                       # Tiện ích kiểm tra nhanh
└── docs/                            # Ghi chú học tập theo giai đoạn
```
