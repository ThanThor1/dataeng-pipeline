# spark_jobs/gold/top_products_hourly.py
"""
Đọc Silver orders, EXPLODE mảng items, tính tổng số lượng bán theo
tumbling window 1 giờ, ghi ra Gold bằng foreachBatch.
"""
import os
import sys

sys.path.insert(0, "/opt/spark_apps")

from pyspark.sql.functions import window, explode, sum as _sum, col
from common.spark_session import get_spark_session

BUCKET_SILVER = os.getenv("MINIO_BUCKET_SILVER", "silver")
BUCKET_GOLD = os.getenv("MINIO_BUCKET_GOLD", "gold")


def main():
    spark = get_spark_session(app_name="top_products_hourly")

    silver_orders = spark.readStream.schema(
        spark.read.parquet(f"s3a://{BUCKET_SILVER}/orders/").schema
    ).parquet(f"s3a://{BUCKET_SILVER}/orders/")

    exploded = silver_orders.select(
        col("created_at_ts"),
        explode(col("items")).alias("item"),
    ).select(
        col("created_at_ts"),
        col("item.product_id").alias("product_id"),
        col("item.product_name").alias("product_name"),
        col("item.quantity").alias("quantity"),
    )

    watermarked = exploded.withWatermark("created_at_ts", "10 minutes")

    product_agg = (
        watermarked
        .groupBy(
            window(col("created_at_ts"), "1 minute"),
            col("product_id"),
            col("product_name"),
        )
        .agg(_sum("quantity").alias("total_quantity_sold"))
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("product_id"),
            col("product_name"),
            col("total_quantity_sold"),
        )
    )

    def write_batch(batch_df, batch_id):
        batch_df.write.mode("append").parquet(f"s3a://{BUCKET_GOLD}/top_products_hourly/")

    query = (
        product_agg.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", f"s3a://{BUCKET_GOLD}/_checkpoints/top_products_hourly/")
        .outputMode("update")
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
