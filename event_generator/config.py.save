# event_generator/config.py
"""
Tập trung mọi cấu hình đọc từ .env vào một nơi duy nhất.
Mọi module khác trong event_generator import từ đây, KHÔNG tự đọc os.environ rải rác.
"""
import os
from pathlib import Path
from dotenv import load_dotenv

# .env nằm ở thư mục gốc dự án (cấp trên của event_generator/)
ROOT_DIR = Path(__file__).resolve().parent.parent
load_dotenv(ROOT_DIR / ".env")

# --- Kafka ---
# Chạy Python từ MÁY HOST (ngoài Docker) nên dùng bootstrap server "host" (localhost:9092)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# --- Topics ---
TOPIC_ORDERS = os.getenv("TOPIC_ORDERS", "orders")
TOPIC_PAYMENTS = os.getenv("TOPIC_PAYMENTS", "payments")
TOPIC_INVENTORY = os.getenv("TOPIC_INVENTORY", "inventory")
TOPIC_CUSTOMERS = os.getenv("TOPIC_CUSTOMERS", "customers")

# --- Tốc độ sinh dữ liệu ---
EVENT_INTERVAL_SECONDS = float(os.getenv("EVENT_INTERVAL_SECONDS", "1.0"))
