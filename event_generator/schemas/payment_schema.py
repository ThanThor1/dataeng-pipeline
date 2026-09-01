# event_generator/schemas/payment_schema.py
from dataclasses import dataclass, asdict


@dataclass
class PaymentEvent:
    payment_id: str
    order_id: str               # liên kết ngược tới OrderEvent -> dùng để JOIN sau này ở Spark/dbt
    customer_id: str
    amount: float
    currency: str = "VND"
    payment_method: str = "credit_card"   # credit_card | momo | bank_transfer | cod
    payment_status: str = "success"       # success | failed | pending
    created_at: str = ""
    event_type: str = "payment_processed"

    def to_dict(self) -> dict:
        return asdict(self)
