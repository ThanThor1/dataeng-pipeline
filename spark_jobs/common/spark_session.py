# spark_jobs/common/spark_session.py
"""
Hàm khởi tạo SparkSession dùng chung cho mọi job (bronze, silver, gold).
Tập trung cấu hình S3A (MinIO) tại đây để không lặp lại ở từng job.
"""
import os
from pyspark.sql import SparkSession


def get_spark_session(app_name: str) -> SparkSession:
    """
    Tạo SparkSession đã cấu hình sẵn kết nối tới MinIO qua S3A connector.
    app_name: tên hiển thị trên Spark UI, nên đặt trùng tên job (VD: "orders_to_bronze")
    để dễ phân biệt khi nhìn Spark Master UI lúc có nhiều job chạy song song.
    """
    minio_endpoint = os.getenv("MINIO_ENDPOINT_INTERNAL", "http://minio:9000")
    minio_access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    minio_secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        # --- Cấu hình S3A để Spark coi MinIO như S3 thật ---
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")   # MinIO cần path-style, không phải virtual-hosted-style
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")  # MinIO nội bộ chạy HTTP, không cần SSL
        # --- Giảm log rác, chỉ hiện WARN trở lên ---
        .config("spark.sql.streaming.schemaInference", "false")  # BẮT BUỘC định nghĩa schema tay, không tự đoán
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark
