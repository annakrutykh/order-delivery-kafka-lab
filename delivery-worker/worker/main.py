import json
import logging
import os
from datetime import datetime, timezone

from confluent_kafka import Consumer

from worker import db
from worker.logging_config import configure_logging
from worker.logic import compute_scheduled_at

configure_logging()
logger = logging.getLogger("delivery-worker")

TOPICS = ["order.status_changed", "order.dispatch_requested"]


def _safe_correlation_id(raw_value: bytes | None) -> str:
    try:
        return json.loads(raw_value)["correlationId"]
    except Exception:
        return "unknown"


def handle_message(raw_value: bytes) -> None:
    event = json.loads(raw_value)
    correlation_id = event.get("correlationId", "unknown")
    order_id = event["orderId"]
    scheduled_at = compute_scheduled_at(datetime.now(timezone.utc))
    db.insert_delivery(order_id, scheduled_at)
    logger.info(
        "delivery scheduled",
        extra={"correlationId": correlation_id, "orderId": order_id, "scheduledAt": scheduled_at.isoformat()},
    )


def run() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": "delivery-worker",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 120000,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(TOPICS)
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            try:
                handle_message(msg.value())
            except Exception:
                logger.exception(
                    "failed to process message",
                    extra={"correlationId": _safe_correlation_id(msg.value())},
                )
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
