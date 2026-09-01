# spark_jobs/bronze/payments_to_bronze.py
"""
Đọc stream từ topic Kafka 'payments', parse JSON theo payment_schema,
ghi thẳng ra Bronze layer trên MinIO dưới dạng Parquet.
"""
import os
import sys

sys.path.insert(0, "/opt/spark_apps")

from pyspark.sql.functions import from_json, col, current_timestamp
from common.spark_session import get_spark_session
from common.schemas import payment_schema

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS_INTERNAL", "kafka:29092")
TOPIC_PAYMENTS = os.getenv("TOPIC_PAYMENTS", "payments")
BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "bronze")


def main():
    spark = get_spark_session(app_name="payments_to_bronze")

    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC_PAYMENTS)
        .option("startingOffsets", "earliest")
        .load()
    )

    parsed_stream = (
        raw_stream
        .selectExpr("CAST(key AS STRING) as kafka_key", "CAST(value AS STRING) as json_value",
                     "topic", "partition", "offset", "timestamp as kafka_timestamp")
        .withColumn("data", from_json(col("json_value"), payment_schema))
        .select(
            col("kafka_key"),
            col("data.*"),
            col("topic"),
            col("partition"),
            col("offset"),
            col("kafka_timestamp"),
        )
        .withColumn("ingested_at", current_timestamp())
    )

    query = (
        parsed_stream.writeStream
        .format("parquet")
        .option("path", f"s3a://{BUCKET_BRONZE}/payments/")
        .option("checkpointLocation", f"s3a://{BUCKET_BRONZE}/_checkpoints/payments/")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
