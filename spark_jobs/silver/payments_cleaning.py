# spark_jobs/silver/payments_cleaning.py
"""
Đọc Bronze payments, làm sạch, ghi ra Silver.
Quy tắc làm sạch:
  1. Ép created_at String -> Timestamp thật
  2. Loại dòng thiếu payment_id/order_id
  3. Loại dòng amount <= 0
  4. CHỈ GIỮ payment_status = 'success' -> chỉ payment thành công mới
     được tính là doanh thu thật ở tầng Gold (quyết định nghiệp vụ quan trọng)
  5. Dedup theo payment_id trong phạm vi watermark
"""
import os
import sys

sys.path.insert(0, "/opt/spark_apps")

from pyspark.sql.functions import col, to_timestamp, current_timestamp
from common.spark_session import get_spark_session

BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")


def main():
    spark = get_spark_session(app_name="payments_cleaning")

    bronze_stream = spark.readStream.schema(
        spark.read.parquet(f"s3a://{BUCKET_BRONZE}/payments/").schema
    ).parquet(f"s3a://{BUCKET_BRONZE}/payments/")

    typed_stream = bronze_stream.withColumn(
        "created_at_ts", to_timestamp(col("created_at"))
    )

    cleaned_stream = typed_stream.filter(
        col("payment_id").isNotNull()
        & col("order_id").isNotNull()
        & (col("amount") > 0)
        & col("created_at_ts").isNotNull()
        & (col("payment_status") == "success")   # chỉ giữ payment thành công
    )

    final_stream = (
        cleaned_stream
        .withWatermark("created_at_ts", "10 minutes")
        .dropDuplicatesWithinWatermark(["payment_id"])
        .withColumn("cleaned_at", current_timestamp())
    )

    query = (
        final_stream.writeStream
        .format("parquet")
        .option("path", f"s3a://{BUCKET_SILVER}/payments/")
        .option("checkpointLocation", f"s3a://{BUCKET_SILVER}/_checkpoints/payments/")
        .outputMode("append")
        .trigger(processingTime="15 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
