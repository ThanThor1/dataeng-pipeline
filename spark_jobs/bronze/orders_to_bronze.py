# spark_jobs/bronze/orders_to_bronze.py
"""
Đọc stream từ topic Kafka 'orders', parse JSON theo order_schema,
ghi thẳng ra Bronze layer trên MinIO dưới dạng Parquet.
Bronze = dữ liệu thô, KHÔNG transform, KHÔNG lọc, KHÔNG làm sạch —
chỉ parse JSON để có cấu trúc bảng thay vì chuỗi bytes.
"""
import os
import sys

# Cho phép import từ spark_jobs/common khi chạy trong container
sys.path.insert(0, "/opt/spark_apps")

from pyspark.sql.functions import from_json, col, current_timestamp
from common.spark_session import get_spark_session
from common.schemas import order_schema

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL", "kafka:29092")
TOPIC_ORDERS = os.getenv("TOPIC_ORDERS", "orders")
BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "bronze")


def main():
    spark = get_spark_session(app_name="orders_to_bronze")

    # BƯỚC 1: Đọc raw stream từ Kafka (mỗi message là 1 row có key, value dạng bytes)
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_ORDERS)
        .option("startingOffsets", "earliest")   # lần đầu chạy, đọc từ đầu topic
        .load()
    )

    # BƯỚC 2: value đang là bytes -> cast sang String -> parse JSON theo order_schema
    parsed_stream = (
        raw_stream
        .selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as json_value",
                     "topic", "partition", "offset", "timestamp as kafka_timestamp")
        .withColumn("data", from_json(col("json_value"), order_schema))
        .select(
            col("kafka_key"),
            col("data.*"),              # tách hết field trong order_schema ra thành cột riêng
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),     # thời điểm Kafka NHẬN message (khác created_at là thời điểm nghiệp vụ)
        )
        .withColumn("ingested_at", current_timestamp())  # thời điểm Spark GHI vào Bronze
    )

    # BƯỚC 3: Ghi ra Bronze layer trên MinIO, dạng Parquet, partition theo ngày để dễ query/quản lý sau này
    query = (
        parsed_stream.writeStream
        .format("parquet")
        .option("path", f"s3a://{BUCKET_BRONZE}/orders/")
        .option("checkpointLocation", f"s3a://{BUCKET_BRONZE}/_checkpoints/orders/")
        .outputMode("append")            # Bronze chỉ thêm mới, không update/overwrite
        .trigger(processingTime="10 seconds")  # cứ 10 giây gom 1 lần ghi file, tránh sinh quá nhiều file nhỏ
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
