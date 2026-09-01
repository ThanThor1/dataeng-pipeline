# event_generator/schemas/customer_schema.py
from dataclasses import dataclass, asdict


@dataclass
class CustomerEvent:
    customer_id: str
    full_name: str
    email: str
    phone: str
    address: str
    city: str
    segment: str = "regular"     # regular | vip | new
    created_at: str = ""
    updated_at: str = ""
    event_type: str = "customer_created"  # customer_created | customer_updated

    def to_dict(self) -> dict:
        return asdict(self)
