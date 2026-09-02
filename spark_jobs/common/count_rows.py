import sys
sys.path.insert(0, "/opt/spark_apps")
from common.spark_session import get_spark_session

spark = get_spark_session(app_name="count_rows")

for topic in ["orders", "payments", "inventory"]:
    b = spark.read.parquet(f"s3a://bronze/{topic}/").count()
    s = spark.read.parquet(f"s3a://silver/{topic}/").count()
    print(f"{topic}: Bronze={b} rows, Silver={s} rows")

spark.stop()
