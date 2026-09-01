# event_generator/main.py
"""
Entry point V2: sinh đồng thời cả 4 loại event, mô phỏng đúng thứ tự nghiệp vụ:
1. Customer event (thỉnh thoảng — không phải lúc nào cũng có khách mới)
2. Order event (khách đặt hàng)
3. Payment event (thanh toán CHO đơn hàng vừa tạo)
4. Inventory event (trừ kho CHO từng sản phẩm trong đơn)

Việc sinh theo đúng chuỗi nhân-quả này (thay vì 4 luồng độc lập ngẫu nhiên)
giúp dữ liệu ở Bronze/Silver layer có thể JOIN lại được một cách có ý nghĩa.
"""
import time
import random
import logging

from event_generator.config import (
    TOPIC_ORDERS,
    TOPIC_PAYMENTS,
    TOPIC_INVENTORY,
    TOPIC_CUSTOMERS,
    EVENT_INTERVAL_SECONDS,
)
from event_generator.producer import EventProducer
from event_generator.generators.order_generator import generate_order_event
from event_generator.generators.payment_generator import generate_payment_event
from event_generator.generators.inventory_generator import generate_inventory_event_for_sale
from event_generator.generators.customer_generator import generate_customer_event

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("main")


def emit_customer_maybe(producer: EventProducer, probability: float = 0.2):
    """20% mỗi vòng lặp sẽ có 1 customer event (mới hoặc update)."""
    if random.random() < probability:
        customer = generate_customer_event()
        producer.send(topic=TOPIC_CUSTOMERS, key=customer.customer_id, value=customer.to_dict())


def emit_order_with_downstream(producer: EventProducer):
    """Sinh 1 order, rồi sinh payment tương ứng, rồi sinh inventory cho từng item."""
    order = generate_order_event()
    producer.send(topic=TOPIC_ORDERS, key=order.customer_id, value=order.to_dict())

    # Chỉ tạo payment nếu đơn không bị hủy ngay từ đầu
    if order.order_status != "cancelled":
        payment = generate_payment_event(
            order_id=order.order_id,
            customer_id=order.customer_id,
            amount=order.total_amount,
        )
        producer.send(topic=TOPIC_PAYMENTS, key=payment.order_id, value=payment.to_dict())

        # Trừ kho cho từng sản phẩm trong đơn, ở 1 kho ngẫu nhiên
        from event_generator.generators.inventory_generator import WAREHOUSES
        for item in order.items:
            warehouse = random.choice(WAREHOUSES)
            inv_event = generate_inventory_event_for_sale(
                product_id=item.product_id,
                warehouse_id=warehouse,
                quantity_sold=item.quantity,
            )
            producer.send(topic=TOPIC_INVENTORY, key=inv_event.product_id, value=inv_event.to_dict())


def run():
    producer = EventProducer()
    logger.info("Bắt đầu sinh events cho 4 topic: orders, payments, inventory, customers... (Ctrl+C để dừng)")

    try:
        while True:
            emit_customer_maybe(producer)
            emit_order_with_downstream(producer)
            time.sleep(EVENT_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Nhận Ctrl+C, đang dừng...")
    finally:
        producer.close()


if __name__ == "__main__":
    run()
