# event_generator/schemas/order_schema.py
"""
Schema cho 1 event Order.
Mỗi đơn hàng có nhiều dòng sản phẩm (order_items) -> dùng nested list.
"""
from dataclasses import dataclass, asdict, field
from typing import List


@dataclass
class OrderItem:
    product_id: str
    product_name: str
    quantity: int
    unit_price: float


@dataclass
class OrderEvent:
    order_id: str
    customer_id: str
    order_status: str          # created | confirmed | shipped | cancelled
    items: List[OrderItem] = field(default_factory=list)
    total_amount: float = 0.0
    currency: str = "VND"
    created_at: str = ""       # ISO 8601 timestamp, ví dụ 2026-08-29T10:00:00Z
    event_type: str = "order_created"  # dùng để phân biệt loại sự kiện trong cùng topic

    def to_dict(self) -> dict:
        """Convert sang dict thuần để json.dumps() gửi Kafka."""
        return asdict(self)
