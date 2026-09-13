import logging

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app import db, kafka_producer
from app.logging_config import configure_logging
from app.logic import (
    build_dispatch_requested_event,
    build_status_changed_event,
    is_valid_transition,
)

configure_logging()
logger = logging.getLogger("order-api")

app = FastAPI(title="order-api")


class CreateOrderRequest(BaseModel):
    customer_name: str
    customer_address: str | None = None


class StatusUpdateRequest(BaseModel):
    status: str


@app.on_event("startup")
def on_startup() -> None:
    db.seed()


@app.post("/orders", status_code=201)
def create_order(body: CreateOrderRequest):
    order_id = db.create_order(body.customer_name, body.customer_address)
    logger.info("order created", extra={"correlationId": str(order_id), "orderId": order_id})
    return {"id": order_id, "status": "NEW"}


@app.patch("/orders/{order_id}/status")
def update_status(order_id: int, body: StatusUpdateRequest):
    order = db.get_order(order_id)
    if order is None:
        raise HTTPException(status_code=404, detail="order not found")
    if not is_valid_transition(order["status"], body.status):
        raise HTTPException(
            status_code=409,
            detail=f"cannot transition from {order['status']} to {body.status}",
        )
    db.update_order_status(order_id, body.status)
    logger.info(
        "order status updated",
        extra={"correlationId": str(order_id), "orderId": order_id, "status": body.status},
    )
    if body.status == "READY_FOR_DELIVERY":
        event = build_status_changed_event(order_id, body.status)
        kafka_producer.publish("order.status_changed", key=str(order_id), value=event)
    return {"id": order_id, "status": body.status}


@app.get("/orders/{order_id}/delivery")
def get_delivery(order_id: int):
    delivery = db.get_delivery_by_order(order_id)
    if delivery is None:
        raise HTTPException(status_code=404, detail="delivery not found")
    return delivery


@app.post("/orders/dispatch-batch")
def dispatch_batch():
    orders = db.get_orders_by_status("READY_FOR_DELIVERY")
    dispatched = 0
    for order in orders:
        event = build_dispatch_requested_event(order["id"])
        kafka_producer.publish("order.dispatch_requested", key=str(order["id"]), value=event)
        dispatched += 1
    logger.info("dispatch batch completed", extra={"dispatchedCount": dispatched})
    return {"dispatchedCount": dispatched}


@app.post("/admin/reset")
def admin_reset():
    db.reset()
    logger.info("admin reset performed")
    return {"status": "reset"}
