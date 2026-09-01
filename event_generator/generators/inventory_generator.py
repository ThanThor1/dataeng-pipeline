# event_generator/generators/inventory_generator.py
"""
Sinh InventoryEvent. Dùng chung PRODUCT_CATALOG với order_generator để product_id nhất quán.
Duy trì 1 dict in-memory làm "kho ảo" để quantity_on_hand có tính liên tục hợp lý,
không random vô căn cứ.
"""
import uuid
from datetime import datetime, timezone

from event_generator.schemas.inventory_schema import InventoryEvent
from event_generator.generators.order_generator import PRODUCT_CATALOG

WAREHOUSES = ["WH_HN", "WH_HCM", "WH_DN"]

# Kho ảo in-memory: {(product_id, warehouse_id): quantity_on_hand}
_stock_state = {
    (p["product_id"], wh): 500  # mỗi sản phẩm khởi điểm 500 đơn vị mỗi kho
    for p in PRODUCT_CATALOG
    for wh in WAREHOUSES
}


def generate_inventory_event_for_sale(product_id: str, warehouse_id: str, quantity_sold: int) -> InventoryEvent:
    """Gọi khi có 1 order_item được bán -> trừ kho tương ứng."""
    key = (product_id, warehouse_id)
    current_qty = _stock_state.get(key, 500)
    new_qty = max(current_qty - quantity_sold, 0)
    _stock_state[key] = new_qty

    product_name = next(p["product_name"] for p in PRODUCT_CATALOG if p["product_id"] == product_id)

    return InventoryEvent(
        inventory_id=f"INV{uuid.uuid4().hex[:12].upper()}",
        product_id=product_id,
        product_name=product_name,
        warehouse_id=warehouse_id,
        quantity_change=-quantity_sold,
        quantity_on_hand=new_qty,
        change_reason="sale",
        created_at=datetime.now(timezone.utc).isoformat(),
    )
