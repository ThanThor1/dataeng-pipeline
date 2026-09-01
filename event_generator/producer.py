# event_generator/producer.py
"""
Wrapper quanh confluent_kafka.Producer, xử lý serialize JSON, log kết quả gửi
(thành công/lỗi) qua delivery callback, và chọn key hợp lý cho từng loại message.
"""
import json
import logging

from confluent_kafka import Producer

from event_generator.config import KAFKA_BOOTSTRAP_SERVERS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("producer")


class EventProducer:
    def __init__(self):
        self.producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "acks": "all",          # chờ tất cả replica xác nhận -> an toàn hơn
            "retries": 5,
            "linger.ms": 50,        # gom message trong 50ms để gửi theo batch, tăng throughput
        })
        logger.info(f"Đã khởi tạo Kafka producer, kết nối tới {KAFKA_BOOTSTRAP_SERVERS}")

    def _delivery_report(self, err, msg):
        """Callback được gọi khi broker xác nhận (hoặc từ chối) message."""
        if err is not None:
            logger.error(f"[FAIL] topic={msg.topic()} lỗi: {err}")
        else:
            logger.info(
                f"[OK] topic={msg.topic()} partition={msg.partition()} "
                f"offset={msg.offset()}"
            )

    def send(self, topic: str, key: str, value: dict):
        payload = json.dumps(value, ensure_ascii=False).encode("utf-8")
        key_bytes = key.encode("utf-8") if key else None

        self.producer.produce(
            topic=topic,
            key=key_bytes,
            value=payload,
            callback=self._delivery_report,
        )
        # poll(0) xử lý các delivery callback đang chờ trong hàng đợi nội bộ,
        # không block. Cần gọi định kỳ để callback thực sự được kích hoạt.
        self.producer.poll(0)

    def flush(self):
        self.producer.flush()

    def close(self):
        logger.info("Đang flush hết message còn lại trước khi đóng...")
        self.producer.flush(timeout=10)
        logger.info("Đã đóng Kafka producer.")
