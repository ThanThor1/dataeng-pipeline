# spark_jobs/silver/orders_cleaning.py
"""
Đọc Bronze orders (Parquet), làm sạch, ghi ra Silver.
Quy tắc làm sạch:
  1. Ép created_at String -> Timestamp thật
  2. Loại dòng thiếu order_id/customer_id
  3. Loại dòng total_amount <= 0 (dữ liệu hỏng)
  4. Dedup theo order_id (giữ bản ghi có ingested_at mới nhất)

Lưu ý: items GIỮ NGUYÊN dạng array lồng - không explode ở Silver
(xem docs/stage_D_silver_gold.md để hiểu lý do thiết kế).
"""
import os
import sys

sys.path.insert(0, "/opt/spark_apps")

from pyspark.sql.functions import col, to_timestamp, row_number
from pyspark.sql.window import Window
from common.spark_session import get_spark_session

BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")


def main():
    spark = get_spark_session(app_name="orders_cleaning")

    # BƯỚC 1: Đọc Bronze dưới dạng STREAM (không phải batch)
    # vì đây vẫn là pipeline liên tục: Bronze mới -> Silver mới liên tục
    bronze_stream = spark.readStream.schema(
        spark.read.parquet(f"s3a://{BUCKET_BRONZE}/orders/").schema
    ).parquet(f"s3a://{BUCKET_BRONZE}/orders/")

    # BƯỚC 2: Ép kiểu created_at String -> Timestamp thật
    typed_stream = bronze_stream.withColumn(
        "created_at_ts", to_timestamp(col("created_at"))
    )

    # BƯỚC 3: Lọc bỏ dòng hỏng
    cleaned_stream = typed_stream.filter(
        col("order_id").isNotNull()
        & col("customer_id").isNotNull()
        & (col("total_amount") > 0)
        & col("created_at_ts").isNotNull()   # created_at parse thất bại -> null -> loại luôn
    )

    # BƯỚC 4: Ghi ra Silver
    # LƯU Ý QUAN TRỌNG: dedup thật sự (loại trùng order_id) sẽ làm ở bước riêng
    # bằng dropDuplicatesWithinWatermark (Spark 3.5+), cần watermark trước.
    from pyspark.sql.functions import current_timestamp

    final_stream = (
        cleaned_stream
        .withWatermark("created_at_ts", "10 minutes")   # cho phép dữ liệu trễ tối đa 10 phút
        .dropDuplicatesWithinWatermark(["order_id"])     # loại trùng order_id trong cửa sổ watermark
        .withColumn("cleaned_at", current_timestamp())
    )

    query = (
        final_stream.writeStream
        .format("parquet")
        .option("path", f"s3a://{BUCKET_SILVER}/orders/")
        .option("checkpointLocation", f"s3a://{BUCKET_SILVER}/_checkpoints/orders/")
        .outputMode("append")
        .trigger(processingTime="15 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
