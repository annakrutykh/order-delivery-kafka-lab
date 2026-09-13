import json
import os

from confluent_kafka import Producer

_producer: Producer | None = None


def _get_producer() -> Producer:
    global _producer
    if _producer is None:
        _producer = Producer({"bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"]})
    return _producer


def publish(topic: str, key: str, value: dict) -> None:
    producer = _get_producer()
    producer.produce(topic, key=key.encode(), value=json.dumps(value).encode())
    producer.flush(10)
