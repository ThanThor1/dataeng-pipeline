# event_generator/generators/order_generator.py
"""
Sinh 1 OrderEvent giả lập, dùng Faker cho các giá trị mang tính người dùng thật (tên SP...).
customer_id được sinh trong một khoảng cố định để mô phỏng việc "khách hàng cũ quay lại mua".
"""
import random
import uuid
from datetime import datetime, timezone

from faker import Faker

from event_generator.schemas.order_schema import OrderEvent, OrderItem

fake = Faker()

# Danh sách sản phẩm giả cố định — dùng lại nhất quán giữa các generator
# (order_generator và inventory_generator đều cần trỏ tới cùng 1 danh sách product)
PRODUCT_CATALOG = [
    {"product_id": "P001", "product_name": "Áo thun basic", "unit_price": 150000},
    {"product_id": "P002", "product_name": "Quần jeans slimfit", "unit_price": 450000},
    {"product_id": "P003", "product_name": "Giày sneaker", "unit_price": 890000},
    {"product_id": "P004", "product_name": "Balo laptop", "unit_price": 350000},
    {"product_id": "P005", "product_name": "Mũ lưỡi trai", "unit_price": 99000},
    {"product_id": "P006", "product_name": "Áo khoác gió", "unit_price": 590000},
]

# Pool 200 customer_id cố định để mô phỏng khách cũ mua lại nhiều lần
CUSTOMER_POOL_SIZE = 200
CUSTOMER_ID_POOL = [f"CUST{str(i).zfill(5)}" for i in range(1, CUSTOMER_POOL_SIZE + 1)]

ORDER_STATUSES = ["created", "confirmed", "shipped", "cancelled"]
ORDER_STATUS_WEIGHTS = [0.5, 0.3, 0.15, 0.05]  # phần lớn đơn "created", ít đơn "cancelled"


def generate_order_event() -> OrderEvent:
    order_id = f"ORD{uuid.uuid4().hex[:12].upper()}"
    customer_id = random.choice(CUSTOMER_ID_POOL)

    num_items = random.randint(1, 4)
    chosen_products = random.sample(PRODUCT_CATALOG, k=num_items)

    items = []
    total_amount = 0.0
    for p in chosen_products:
        qty = random.randint(1, 3)
        item = OrderItem(
            product_id=p["product_id"],
            product_name=p["product_name"],
            quantity=qty,
            unit_price=p["unit_price"],
        )
        items.append(item)
        total_amount += qty * p["unit_price"]

    status = random.choices(ORDER_STATUSES, weights=ORDER_STATUS_WEIGHTS, k=1)[0]

    return OrderEvent(
        order_id=order_id,
        customer_id=customer_id,
        order_status=status,
        items=items,
        total_amount=round(total_amount, 2),
        created_at=datetime.now(timezone.utc).isoformat(),
    )
