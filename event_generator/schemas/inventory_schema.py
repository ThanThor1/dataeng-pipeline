# event_generator/schemas/inventory_schema.py
from dataclasses import dataclass, asdict


@dataclass
class InventoryEvent:
    inventory_id: str
    product_id: str
    product_name: str
    warehouse_id: str
    quantity_change: int        # âm = xuất kho (bán), dương = nhập kho
    quantity_on_hand: int        # số lượng tồn SAU khi thay đổi
    change_reason: str = "sale"  # sale | restock | adjustment | return
    created_at: str = ""
    event_type: str = "inventory_updated"

    def to_dict(self) -> dict:
        return asdict(self)
