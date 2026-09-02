# spark_jobs/gold/revenue_5min_window.py
"""
Đọc Silver payments (đã lọc success), tính doanh thu theo tumbling window 5 phút,
ghi ra Gold bằng foreachBatch (vì Parquet sink không hỗ trợ outputMode "update" trực tiếp).
"""
import os
import sys

sys.path.insert(0, "/opt/spark_apps")

from pyspark.sql.functions import window, sum as _sum, count, col
from common.spark_session import get_spark_session

BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")
BUCKET_GOLD = os.getenv("MINIO_BUCKET_GOLD", "gold")


def main():
    spark = get_spark_session(app_name="revenue_5min_window")

    silver_payments = spark.readStream.schema(
        spark.read.parquet(f"s3a://{BUCKET_SILVER}/payments/").schema
    ).parquet(f"s3a://{BUCKET_SILVER}/payments/")

    watermarked = silver_payments.withWatermark("created_at_ts", "10 minutes")

    revenue_agg = (
        watermarked
        .groupBy(window(col("created_at_ts"), "1 minute"))
        .agg(
            _sum("amount").alias("total_revenue"),
            count("*").alias("transaction_count"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("total_revenue"),
            col("transaction_count"),
        )
    )

    def write_batch(batch_df, batch_id):
        batch_df.write.mode("append").parquet(f"s3a://{BUCKET_GOLD}/revenue_5min/")

    query = (
        revenue_agg.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"s3a://{BUCKET_GOLD}/_checkpoints/revenue_5min/")
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
