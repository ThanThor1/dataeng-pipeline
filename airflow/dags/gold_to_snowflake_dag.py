# airflow/dags/gold_to_snowflake_dag.py
"""
DAG điều phối việc load dữ liệu Gold (MinIO, Parquet) vào Snowflake raw tables.
Chạy theo lịch mỗi giờ, có retry và cảnh báo khi thất bại.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

import pandas as pd
import io

# ============================================
# Cấu hình chung cho DAG
# ============================================
default_args = {
    "owner": "dataeng",
    "retries": 3,                          # thử lại tối đa 3 lần nếu task thất bại
    "retry_delay": timedelta(minutes=2),   # cách nhau 2 phút giữa mỗi lần thử
    "email_on_failure": False,             # Giai đoạn này chưa cấu hình SMTP, để False
}

GOLD_BUCKET = "gold"

# Mapping: (thư mục Gold trên MinIO) -> (bảng đích trên Snowflake)
TABLE_MAPPING = {
    "revenue_5min": {
        "table": "RAW_REVENUE_5MIN",
        "columns": ["window_start", "window_end", "total_revenue", "transaction_count"],
    },
    "top_products_hourly": {
        "table": "RAW_TOP_PRODUCTS_HOURLY",
        "columns": ["window_start", "window_end", "product_id", "product_name", "total_quantity_sold"],
    },
    "customer_activity_hourly": {
        "table": "RAW_CUSTOMER_ACTIVITY_HOURLY",
        "columns": ["window_start", "window_end", "customer_id", "order_count", "total_spent", "last_order_at"],
    },
}


def load_gold_to_snowflake(gold_folder: str, table_name: str, columns: list, **context):
    """
    Đọc TOÀN BỘ file Parquet trong 1 thư mục Gold, gộp lại, nạp vào Snowflake.
    Đơn giản hoá cho Giai đoạn E: nạp lại toàn bộ (full load), không tính incremental.
    Incremental loading sẽ là điểm cải tiến ở giai đoạn sau.
    """
    s3_hook = S3Hook(aws_conn_id="minio_default")
    snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    # Liệt kê toàn bộ file .parquet trong thư mục Gold tương ứng
    keys = s3_hook.list_keys(bucket_name=GOLD_BUCKET, prefix=f"{gold_folder}/")
    parquet_keys = [k for k in keys if k.endswith(".parquet")]

    if not parquet_keys:
        print(f"Không tìm thấy file Parquet nào trong gold/{gold_folder}/, bỏ qua.")
        return

    # Đọc từng file, gộp thành 1 DataFrame duy nhất
    dfs = []
    for key in parquet_keys:
        obj = s3_hook.get_key(key, bucket_name=GOLD_BUCKET)
        buffer = io.BytesIO(obj.get()["Body"].read())
        df = pd.read_parquet(buffer)
        dfs.append(df)

    full_df = pd.concat(dfs, ignore_index=True)
    full_df = full_df[columns]  # đảm bảo đúng thứ tự cột khớp bảng Snowflake

    # Chuyển mọi cột kiểu datetime sang string ISO format,
    # vì Snowflake connector executemany() không tự nhận pandas Timestamp
    for col in full_df.columns:
        if pd.api.types.is_datetime64_any_dtype(full_df[col]):
            full_df[col] = full_df[col].dt.strftime("%Y-%m-%d %H:%M:%S.%f")

    print(f"Đọc được {len(full_df)} dòng từ {len(parquet_keys)} file Parquet trong gold/{gold_folder}/")

    # Ghi đè bảng Snowflake (TRUNCATE + INSERT, đơn giản cho full load)
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()
    try:
        cursor.execute(f"TRUNCATE TABLE {table_name}")

        col_list = ", ".join(columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO {table_name} ({col_list}) VALUES ({placeholders})"

        records = [tuple(row) for row in full_df.itertuples(index=False)]
        cursor.executemany(insert_sql, records)
        conn.commit()

        print(f"Đã nạp {len(records)} dòng vào Snowflake bảng {table_name}")
    finally:
        cursor.close()
        conn.close()


# ============================================
# Định nghĩa DAG
# ============================================
with DAG(
    dag_id="gold_to_snowflake_dag",
    default_args=default_args,
    description="Load du lieu Gold layer (MinIO) vao Snowflake raw tables",
    schedule_interval="@hourly",           # chạy mỗi giờ 1 lần
    start_date=datetime(2026, 9, 1),
    catchup=False,                         # không chạy bù các lần lỡ trong quá khứ
    tags=["dataeng-pipeline", "gold", "snowflake"],
) as dag:

    tasks = []
    for gold_folder, config in TABLE_MAPPING.items():
        task = PythonOperator(
            task_id=f"load_{gold_folder}_to_snowflake",
            python_callable=load_gold_to_snowflake,
            op_kwargs={
                "gold_folder": gold_folder,
                "table_name": config["table"],
                "columns": config["columns"],
            },
        )
        tasks.append(task)

    # 3 task chạy ĐỘC LẬP, song song (không phụ thuộc lẫn nhau)
    # vì mỗi bảng raw hoàn toàn tách biệt, lỗi 1 bảng không nên chặn 2 bảng kia
