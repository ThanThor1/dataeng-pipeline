# event_generator/generators/payment_generator.py
"""
Sinh PaymentEvent, LIÊN KẾT với 1 order_id có thật (giả lập việc thanh toán
xảy ra SAU khi có đơn hàng — đúng thứ tự nghiệp vụ thật).
"""
import random
import uuid
from datetime import datetime, timezone

from event_generator.schemas.payment_schema import PaymentEvent

PAYMENT_METHODS = ["credit_card", "momo", "bank_transfer", "cod"]
PAYMENT_STATUSES = ["success", "failed", "pending"]
PAYMENT_STATUS_WEIGHTS = [0.85, 0.10, 0.05]


def generate_payment_event(order_id: str, customer_id: str, amount: float) -> PaymentEvent:
    payment_id = f"PAY{uuid.uuid4().hex[:12].upper()}"
    status = random.choices(PAYMENT_STATUSES, weights=PAYMENT_STATUS_WEIGHTS, k=1)[0]

    return PaymentEvent(
        payment_id=payment_id,
        order_id=order_id,
        customer_id=customer_id,
        amount=amount,
        payment_method=random.choice(PAYMENT_METHODS),
        payment_status=status,
        created_at=datetime.now(timezone.utc).isoformat(),
    )
