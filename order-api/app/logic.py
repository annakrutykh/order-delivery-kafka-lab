from datetime import datetime, timezone

ORDER_STATUSES = ("NEW", "IN_PROGRESS", "READY_FOR_DELIVERY", "DELIVERED")

_ALLOWED_TRANSITIONS = {
    "NEW": {"IN_PROGRESS", "READY_FOR_DELIVERY"},
    "IN_PROGRESS": {"READY_FOR_DELIVERY"},
    "READY_FOR_DELIVERY": {"DELIVERED"},
    "DELIVERED": set(),
}


def is_valid_transition(current: str, target: str) -> bool:
    if target not in ORDER_STATUSES:
        return False
    return target in _ALLOWED_TRANSITIONS.get(current, set())


def build_status_changed_event(order_id: int, status: str) -> dict:
    return {
        "correlationId": str(order_id),
        "orderId": order_id,
        "eventType": "order.status_changed",
        "status": status,
        "occurredAt": datetime.now(timezone.utc).isoformat(),
    }


def build_dispatch_requested_event(order_id: int) -> dict:
    return {
        "correlationId": str(order_id),
        "orderId": order_id,
        "eventType": "order.dispatch_requested",
        "occurredAt": datetime.now(timezone.utc).isoformat(),
    }
