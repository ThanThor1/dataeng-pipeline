# spark_jobs/gold/customer_churn_signal.py
"""
Đọc Silver orders, tính số đơn hàng + tổng chi tiêu mỗi khách hàng
theo tumbling window 1 giờ, ghi ra Gold bằng foreachBatch.
"""
import os
import sys

sys.path.insert(0, "/opt/spark_apps")

from pyspark.sql.functions import window, count, sum as _sum, max as _max, col
from common.spark_session import get_spark_session

BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")
BUCKET_GOLD = os.getenv("MINIO_BUCKET_GOLD", "gold")


def main():
    spark = get_spark_session(app_name="customer_churn_signal")

    silver_orders = spark.readStream.schema(
        spark.read.parquet(f"s3a://{BUCKET_SILVER}/orders/").schema
    ).parquet(f"s3a://{BUCKET_SILVER}/orders/")

    watermarked = silver_orders.withWatermark("created_at_ts", "10 minutes")

    customer_activity = (
        watermarked
        .groupBy(
            window(col("created_at_ts"), "1 minute"),
            col("customer_id"),
        )
        .agg(
            count("*").alias("order_count"),
            _sum("total_amount").alias("total_spent"),
            _max("created_at_ts").alias("last_order_at"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("customer_id"),
            col("order_count"),
            col("total_spent"),
            col("last_order_at"),
        )
    )

    def write_batch(batch_df, batch_id):
        batch_df.write.mode("append").parquet(f"s3a://{BUCKET_GOLD}/customer_activity_hourly/")

    query = (
        customer_activity.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"s3a://{BUCKET_GOLD}/_checkpoints/customer_activity_hourly/")
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
