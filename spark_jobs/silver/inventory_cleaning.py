# spark_jobs/silver/inventory_cleaning.py
"""
Đọc Bronze inventory, làm sạch, ghi ra Silver.
Quy tắc làm sạch:
  1. Ép created_at String -> Timestamp thật
  2. Loại dòng thiếu inventory_id/product_id
  3. Loại dòng quantity_on_hand < 0 (tồn kho âm là dữ liệu bất thường)
  4. Dedup theo inventory_id trong phạm vi watermark
"""
import os
import sys

sys.path.insert(0, "/opt/spark_apps")

from pyspark.sql.functions import col, to_timestamp, current_timestamp
from common.spark_session import get_spark_session

BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")


def main():
    spark = get_spark_session(app_name="inventory_cleaning")

    bronze_stream = spark.readStream.schema(
        spark.read.parquet(f"s3a://{BUCKET_BRONZE}/inventory/").schema
    ).parquet(f"s3a://{BUCKET_BRONZE}/inventory/")

    typed_stream = bronze_stream.withColumn(
        "created_at_ts", to_timestamp(col("created_at"))
    )

    cleaned_stream = typed_stream.filter(
        col("inventory_id").isNotNull()
        & col("product_id").isNotNull()
        & (col("quantity_on_hand") >= 0)
        & col("created_at_ts").isNotNull()
    )

    final_stream = (
        cleaned_stream
        .withWatermark("created_at_ts", "10 minutes")
        .dropDuplicatesWithinWatermark(["inventory_id"])
        .withColumn("cleaned_at", current_timestamp())
    )

    query = (
        final_stream.writeStream
        .format("parquet")
        .option("path", f"s3a://{BUCKET_SILVER}/inventory/")
        .option("checkpointLocation", f"s3a://{BUCKET_SILVER}/_checkpoints/inventory/")
        .outputMode("append")
        .trigger(processingTime="15 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
