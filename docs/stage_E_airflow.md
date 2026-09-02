# Giai đoạn E: Airflow điều phối Gold -> Snowflake raw

## Đã làm
- Airflow 2.9.3 CeleryExecutor (Postgres + Redis + webserver + scheduler + worker + triggerer)
- Custom image có apache-airflow-providers-snowflake, apache-airflow-providers-amazon
- Connections: minio_default (S3-compatible), snowflake_default
- DAG gold_to_snowflake_dag: 3 task song song, full load MinIO Parquet -> Snowflake raw tables
- Schedule: @hourly, retries: 3, retry_delay: 2 phút

## Sự cố đã giải quyết (đáng nhớ)
- postgres:13 vs :15 version mismatch -> phải xoá volume cũ khi đổi version
- Dockerfile: USER phải đặt TRƯỚC RUN pip install, không dùng --user trong virtualenv
- Airflow Connection UI: field "region" ẩn gây sai hostname SSL -> phải sửa qua CLI
  (airflow connections delete/add) để chắc chắn, và RESTART worker để nhận Connection mới
- Snowflake Account Identifier: dùng <organization_name>-<account_name>
  (VD: KVWDIDW-OP86178), KHÔNG dùng <account_locator>.<region> (cách cũ)
- pandas Timestamp không bind được trực tiếp vào Snowflake executemany()
  -> phải .dt.strftime() sang string trước khi insert

## Việc cần làm tiếp (Giai đoạn F)
- dbt: staging models (stg_orders.sql...) đọc từ RAW schema
- dbt test: not_null, unique, referential integrity
