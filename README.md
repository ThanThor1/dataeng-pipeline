# Data Engineering Pipeline — Tiến độ

| Giai đoạn | Nội dung | Trạng thái |
|---|---|---|
| A | Hạ tầng Docker: Kafka + MinIO | ✅ Hoàn thành |
| B | Event Generator → Kafka (4 topic) | ⬜ Chưa bắt đầu |
| C | Spark Streaming → Bronze | ⬜ |
| D | Spark → Silver + Gold | ⬜ |
| E | Airflow: Gold → Snowflake | ⬜ |
| F | dbt staging + test | ⬜ |
| G | dbt star schema | ⬜ |
| H | BI + Grafana alerting | ⬜ |

## Cách khởi động hạ tầng hiện tại
\`\`\`bash
docker compose up -d
docker compose ps
bash scripts/check_kafka_topics.sh
bash scripts/check_minio_buckets.sh
\`\`\`

Chi tiết từng giai đoạn xem trong `docs/stage_*.md`
