# spark_jobs/common/schemas.py
"""
StructType schema cho từng topic Kafka, PHẢI khớp chính xác với
dataclass tương ứng trong event_generator/schemas/*.py (Giai đoạn B).

Lưu ý quan trọng: Structured Streaming KHÔNG tự suy schema từ JSON.
Nếu field nào trong JSON thực tế không khớp tên/kiểu ở đây, Spark
sẽ trả về NULL cho field đó thay vì báo lỗi -> luôn đối chiếu lại
dataclass gốc mỗi khi sửa schema này.
"""
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, ArrayType,
)

# --- Order schema (khớp event_generator/schemas/order_schema.py) ---
order_item_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("unit_price", DoubleType(), True),
])

order_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("items", ArrayType(order_item_schema), True),
    StructField("total_amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("created_at", StringType(), True),   # giữ String, parse sang Timestamp ở bước sau (silver)
    StructField("event_type", StringType(), True),
])

# --- Payment schema (khớp event_generator/schemas/payment_schema.py) ---
payment_schema = StructType([
    StructField("payment_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("payment_status", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("event_type", StringType(), True),
])

# --- Inventory schema (khớp event_generator/schemas/inventory_schema.py) ---
inventory_schema = StructType([
    StructField("inventory_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("warehouse_id", StringType(), True),
    StructField("quantity_change", IntegerType(), True),
    StructField("quantity_on_hand", IntegerType(), True),
    StructField("change_reason", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("event_type", StringType(), True),
])

# --- Customer schema (khớp event_generator/schemas/customer_schema.py) ---
customer_schema = StructType([
    StructField("customer_id", StringType(), True),
    StructField("full_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("segment", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("updated_at", StringType(), True),
    StructField("event_type", StringType(), True),
])
