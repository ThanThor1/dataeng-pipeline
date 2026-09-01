# event_generator/generators/customer_generator.py
"""
Sinh CustomerEvent. Khác với order/payment (luôn tạo mới), customer có 2 loại event:
- customer_created: khách hàng mới hoàn toàn
- customer_updated: khách hàng cũ đổi thông tin (địa chỉ, segment...)
  -> đây chính là dữ liệu nuôi SCD Type 2 ở Giai đoạn G.
"""
import random
from datetime import datetime, timezone

from faker import Faker
from event_generator.schemas.customer_schema import CustomerEvent
from event_generator.generators.order_generator import CUSTOMER_ID_POOL

fake = Faker("vi_VN")

SEGMENTS = ["regular", "vip", "new"]
CITIES = ["Hà Nội", "Hồ Chí Minh", "Đà Nẵng", "Ninh Bình", "Hải Phòng"]

# Lưu thông tin khách hàng đã "tồn tại" để mô phỏng update lại đúng người đó
_known_customers = {}


def generate_customer_event() -> CustomerEvent:
    customer_id = random.choice(CUSTOMER_ID_POOL)
    now = datetime.now(timezone.utc).isoformat()

    if customer_id in _known_customers:
        # Khách đã tồn tại -> sinh event UPDATE (đổi city hoặc segment)
        base = _known_customers[customer_id]
        event = CustomerEvent(
            customer_id=customer_id,
            full_name=base["full_name"],
            email=base["email"],
            phone=base["phone"],
            address=fake.address().replace("\n", ", "),
            city=random.choice(CITIES),
            segment=random.choice(SEGMENTS),
            created_at=base["created_at"],
            updated_at=now,
            event_type="customer_updated",
        )
    else:
        # Khách mới hoàn toàn -> sinh event CREATE
        event = CustomerEvent(
            customer_id=customer_id,
            full_name=fake.name(),
            email=fake.email(),
            phone=fake.phone_number(),
            address=fake.address().replace("\n", ", "),
            city=random.choice(CITIES),
            segment="new",
            created_at=now,
            updated_at=now,
            event_type="customer_created",
        )
        _known_customers[customer_id] = {
            "full_name": event.full_name,
            "email": event.email,
            "phone": event.phone,
            "created_at": event.created_at,
        }

    return event
