# Kafka + CI/CD Lab Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the full `order-delivery-kafka-lab` teaching stand described in `docs/DESIGN.md`: two Python services (`order-api`, `delivery-worker`), a Kafka+Postgres+ELK docker-compose stack, seeded data, a GitHub Actions CI pipeline, and two feature branches (`feature/producer-bugs`, `feature/consumer-bug`) each carrying deterministic, realistic bugs plus a prepared fix commit.

**Architecture:** `order-api` (FastAPI) owns the `orders` table and publishes Kafka events on status changes and batch dispatch. `delivery-worker` (a plain consumer loop) owns the `deliveries` table, reads both topics, and assigns delivery dates. Both services separate pure business logic (`logic.py`, unit-tested, no I/O) from I/O glue (routes, DB, Kafka client) so CI can test the former without a running Kafka/Postgres — this is exactly the gap the lesson exploits. Kafka runs single-broker KRaft mode; logs are JSON lines written to a shared `./logs` bind mount that Filebeat tails into Elasticsearch.

**Tech Stack:** Python 3.12, FastAPI + Uvicorn, `confluent-kafka`, `psycopg[binary]` (v3, no ORM, no migrations tool), `python-json-logger`, pytest + ruff, Docker Compose, `apache/kafka` (KRaft), `provectuslabs/kafka-ui`, Elastic Stack 8.13.4, GitHub Actions.

**Spec:** `docs/DESIGN.md` (architecture, bugs, git/CI structure), `docs/LESSON_PLAN.md` (how each piece is used live — informs what must be *observable*, e.g. exact log fields, exact response shapes).

## Global Constraints

- 90-минутное живое занятие — код должен быть простым и читаемым, не продакшен-грейд. Никаких миграций-фреймворков, connection-пулов, retries-с-бэкоффом и т.п. сверх необходимого.
- ELK поднимается заранее, до занятия — не оптимизировать под скорость старта во время эфира, но она всё равно должна подниматься командой `docker compose up --build -d` без ручных шагов.
- Все 4 бага должны быть реалистичными и не ловиться юнит-тестами, которые мокают Kafka и проверяют только бизнес-логику (`logic.py`). Ни один тест не должен инспектировать количество вызовов Kafka-продюсера или содержимое сообщений.
- Один общий стенд у ведущего — никакой per-student изоляции, никакого multi-tenancy.
- `main` — чистый baseline без единого бага; фиче-ветки создаются от `main` и вносят регресс отдельным коммитом, который затем полностью отменяется fix-коммитом.
- CI триггерится на `pull_request` в `main`, проверяет линт (ruff) + unit-тесты; не включает интеграционный тест с реальной Kafka.
- Оба PR должны оставаться зелёными на всём протяжении, включая бажный коммит.
- Kafka: KRaft-режим (без Zookeeper), один брокер, одна партиция на топик.
- `correlationId` = `orderId` (строкой), сквозной по логам и по Kafka-сообщению.
- DB-схема (`db/init/*.sql`) — общая для всех веток и не меняется фиче-ветками, потому что данные Postgres переживают `git checkout` (volume не пересоздаётся между ветками на живом занятии). Любая идемпотентность реализуется в коде приложения, не в DDL.

---

## File Structure

```
order-delivery-kafka-lab/
├── docker-compose.yml
├── db/
│   └── init/
│       └── 001_schema.sql
├── filebeat/
│   └── filebeat.yml
├── logs/
│   └── .gitkeep
├── order-api/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── requirements-dev.txt
│   ├── app/
│   │   ├── __init__.py
│   │   ├── main.py             # FastAPI app + 4 routes + startup seeding
│   │   ├── db.py                # psycopg access: orders CRUD, seed/reset
│   │   ├── kafka_producer.py    # thin confluent-kafka producer wrapper
│   │   ├── logic.py             # pure: transitions, event payload builders
│   │   ├── seed_data.py         # SEED_ORDERS constant
│   │   └── logging_config.py
│   └── tests/
│       └── test_logic.py
├── delivery-worker/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── requirements-dev.txt
│   ├── worker/
│   │   ├── __init__.py
│   │   ├── main.py              # consumer loop
│   │   ├── db.py                # psycopg access: deliveries insert/lookup
│   │   ├── logic.py             # pure: compute_scheduled_at
│   │   └── logging_config.py
│   └── tests/
│       └── test_logic.py
└── .github/
    └── workflows/
        └── ci.yml
```

---

## Task 1: Repo scaffolding and DB schema

**Files:**
- Create: `.gitignore`
- Create: `logs/.gitkeep`
- Create: `db/init/001_schema.sql`

**Interfaces:**
- Produces: tables `orders(id, customer_name, customer_address, status, created_at, updated_at)` and `deliveries(id, order_id, scheduled_at, created_at)`, used by every later task.

- [ ] **Step 1: Create `.gitignore`**

```
__pycache__/
*.pyc
.pytest_cache/
.ruff_cache/
logs/*.log
```

- [ ] **Step 2: Create `logs/.gitkeep`** (empty file, so the bind-mount directory exists and is tracked by git even though the `.log` files inside it are ignored)

- [ ] **Step 3: Write `db/init/001_schema.sql`**

```sql
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    customer_name TEXT NOT NULL,
    customer_address TEXT,
    status TEXT NOT NULL DEFAULT 'NEW',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE deliveries (
    id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL REFERENCES orders(id),
    scheduled_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`customer_address` is nullable on purpose: one seeded order (Task 3) will have `NULL` there to serve as the boundary case for Bug 3.

- [ ] **Step 4: Commit**

```bash
git add .gitignore logs/.gitkeep db/init/001_schema.sql
git commit -m "chore: scaffold repo layout and DB schema"
```

---

## Task 2: order-api — pure business logic (`logic.py`), TDD

**Files:**
- Create: `order-api/requirements.txt`
- Create: `order-api/requirements-dev.txt`
- Create: `order-api/app/__init__.py`
- Create: `order-api/app/logic.py`
- Test: `order-api/tests/test_logic.py`

**Interfaces:**
- Produces: `ORDER_STATUSES: tuple[str, ...]`, `is_valid_transition(current: str, target: str) -> bool`, `build_status_changed_event(order_id: int, status: str) -> dict`, `build_dispatch_requested_event(order_id: int) -> dict` — all consumed by `main.py` in Task 3.

- [ ] **Step 1: Create `order-api/requirements.txt`**

```
fastapi==0.115.0
uvicorn[standard]==0.30.6
psycopg[binary]==3.2.1
confluent-kafka==2.5.0
python-json-logger==2.0.7
pydantic==2.9.2
```

- [ ] **Step 2: Create `order-api/requirements-dev.txt`**

```
pytest==8.3.2
ruff==0.6.4
```

- [ ] **Step 3: Create empty `order-api/app/__init__.py`**

- [ ] **Step 4: Write the failing tests in `order-api/tests/test_logic.py`**

```python
from app.logic import (
    is_valid_transition,
    build_status_changed_event,
    build_dispatch_requested_event,
)


def test_new_to_in_progress_is_valid():
    assert is_valid_transition("NEW", "IN_PROGRESS") is True


def test_new_to_ready_for_delivery_is_valid_skip_path():
    assert is_valid_transition("NEW", "READY_FOR_DELIVERY") is True


def test_in_progress_to_ready_for_delivery_is_valid():
    assert is_valid_transition("IN_PROGRESS", "READY_FOR_DELIVERY") is True


def test_ready_for_delivery_to_delivered_is_valid():
    assert is_valid_transition("READY_FOR_DELIVERY", "DELIVERED") is True


def test_delivered_has_no_outgoing_transitions():
    assert is_valid_transition("DELIVERED", "NEW") is False


def test_backwards_transition_is_invalid():
    assert is_valid_transition("IN_PROGRESS", "NEW") is False


def test_unknown_target_status_is_invalid():
    assert is_valid_transition("NEW", "CANCELLED") is False


def test_build_status_changed_event_shape():
    event = build_status_changed_event(42, "READY_FOR_DELIVERY")
    assert event["correlationId"] == "42"
    assert event["orderId"] == 42
    assert event["eventType"] == "order.status_changed"
    assert event["status"] == "READY_FOR_DELIVERY"
    assert "occurredAt" in event


def test_build_dispatch_requested_event_shape():
    event = build_dispatch_requested_event(7)
    assert event["correlationId"] == "7"
    assert event["orderId"] == 7
    assert event["eventType"] == "order.dispatch_requested"
    assert "occurredAt" in event
```

- [ ] **Step 5: Run tests to verify they fail**

Run (from `order-api/`): `python -m pytest tests/test_logic.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.logic'`

- [ ] **Step 6: Write `order-api/app/logic.py`**

```python
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
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `python -m pytest tests/test_logic.py -v`
Expected: 9 passed

- [ ] **Step 8: Lint**

Run: `python -m ruff check app tests`
Expected: no findings (fix any before proceeding)

- [ ] **Step 9: Commit**

```bash
git add order-api/requirements.txt order-api/requirements-dev.txt \
        order-api/app/__init__.py order-api/app/logic.py \
        order-api/tests/test_logic.py
git commit -m "feat(order-api): add pure status-transition and event-building logic"
```

---

## Task 3: order-api — DB access, seed data, Kafka producer, logging config

**Files:**
- Create: `order-api/app/seed_data.py`
- Create: `order-api/app/db.py`
- Create: `order-api/app/kafka_producer.py`
- Create: `order-api/app/logging_config.py`

**Interfaces:**
- Consumes: nothing from earlier tasks (DB layer is independent of `logic.py`).
- Produces: `SEED_ORDERS: list[tuple[str, str | None, str]]`; `db.get_order(order_id) -> dict | None`, `db.update_order_status(order_id, status) -> None`, `db.create_order(name, address) -> int`, `db.get_orders_by_status(status) -> list[dict]`, `db.count_orders_by_status(status) -> int`, `db.get_delivery_by_order(order_id) -> dict | None`, `db.seed() -> None`, `db.reset() -> None`; `kafka_producer.publish(topic: str, key: str, value: dict) -> None`; `logging_config.configure_logging() -> None`. All consumed by `main.py` in Task 4.

This task has no automated tests of its own — it is exercised end-to-end in Task 9 (manual baseline verification) against the real Postgres/Kafka containers, per the Global Constraints (only pure logic gets unit tests, matching what real CI will check).

- [ ] **Step 1: Write `order-api/app/seed_data.py`**

```python
# (name, address, status) — one row deliberately has address=None:
# it is the boundary case Bug 3 (dispatch-batch) targets later.
SEED_ORDERS = [
    ("Иван Петров", "Москва, ул. Ленина, 10", "NEW"),
    ("Мария Сидорова", "СПб, Невский пр., 25", "NEW"),
    ("Алексей Смирнов", "Казань, ул. Баумана, 5", "NEW"),
    ("Ольга Кузнецова", "Екатеринбург, ул. Малышева, 12", "IN_PROGRESS"),
    ("Дмитрий Волков", "Новосибирск, Красный пр., 30", "IN_PROGRESS"),
    ("Елена Морозова", "Самара, ул. Мичурина, 8", "IN_PROGRESS"),
    ("Сергей Новиков", "Ростов-на-Дону, ул. Пушкинская, 40", "READY_FOR_DELIVERY"),
    ("Татьяна Соколова", "Уфа, ул. Ленина, 55", "READY_FOR_DELIVERY"),
    ("Павел Лебедев", "Челябинск, пр. Ленина, 70", "READY_FOR_DELIVERY"),
    ("Наталья Козлова", "Омск, ул. Гагарина, 15", "READY_FOR_DELIVERY"),
    ("Игорь Соловьёв", "Воронеж, ул. Плеханова, 20", "READY_FOR_DELIVERY"),
    ("Легаси-заказ #1", None, "READY_FOR_DELIVERY"),
    ("Анна Васильева", "Краснодар, ул. Красная, 60", "DELIVERED"),
    ("Виктор Захаров", "Пермь, ул. Ленина, 22", "DELIVERED"),
]
```

- [ ] **Step 2: Write `order-api/app/db.py`**

```python
import os

import psycopg
from psycopg.rows import dict_row

from app.seed_data import SEED_ORDERS

DATABASE_URL = os.environ["DATABASE_URL"]


def _connect() -> psycopg.Connection:
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def create_order(customer_name: str, customer_address: str | None) -> int:
    with _connect() as conn:
        row = conn.execute(
            """
            INSERT INTO orders (customer_name, customer_address, status)
            VALUES (%s, %s, 'NEW')
            RETURNING id
            """,
            (customer_name, customer_address),
        ).fetchone()
        conn.commit()
        return row["id"]


def get_order(order_id: int) -> dict | None:
    with _connect() as conn:
        return conn.execute(
            "SELECT id, customer_name, customer_address, status FROM orders WHERE id = %s",
            (order_id,),
        ).fetchone()


def update_order_status(order_id: int, status: str) -> None:
    with _connect() as conn:
        conn.execute(
            "UPDATE orders SET status = %s, updated_at = now() WHERE id = %s",
            (status, order_id),
        )
        conn.commit()


def get_orders_by_status(status: str) -> list[dict]:
    with _connect() as conn:
        return conn.execute(
            "SELECT id, customer_name, customer_address, status FROM orders WHERE status = %s ORDER BY id",
            (status,),
        ).fetchall()


def count_orders_by_status(status: str) -> int:
    with _connect() as conn:
        row = conn.execute(
            "SELECT count(*) AS n FROM orders WHERE status = %s", (status,)
        ).fetchone()
        return row["n"]


def get_delivery_by_order(order_id: int) -> dict | None:
    with _connect() as conn:
        return conn.execute(
            "SELECT id, order_id, scheduled_at FROM deliveries WHERE order_id = %s",
            (order_id,),
        ).fetchone()


def seed() -> None:
    with _connect() as conn:
        row = conn.execute("SELECT count(*) AS n FROM orders").fetchone()
        if row["n"] > 0:
            return
        for name, address, status in SEED_ORDERS:
            conn.execute(
                "INSERT INTO orders (customer_name, customer_address, status) VALUES (%s, %s, %s)",
                (name, address, status),
            )
        conn.commit()


def reset() -> None:
    with _connect() as conn:
        conn.execute("TRUNCATE deliveries, orders RESTART IDENTITY CASCADE")
        conn.commit()
    seed()
```

- [ ] **Step 3: Write `order-api/app/kafka_producer.py`**

```python
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
```

- [ ] **Step 4: Write `order-api/app/logging_config.py`**

```python
import logging
import os

from pythonjsonlogger import jsonlogger


def configure_logging() -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    log_file = os.environ.get("LOG_FILE")
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    formatter = jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    for handler in handlers:
        handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=handlers, force=True)
```

- [ ] **Step 5: Lint**

Run (from `order-api/`): `python -m ruff check app`
Expected: no findings

- [ ] **Step 6: Commit**

```bash
git add order-api/app/seed_data.py order-api/app/db.py \
        order-api/app/kafka_producer.py order-api/app/logging_config.py
git commit -m "feat(order-api): add DB access, seed data, Kafka producer, logging"
```

---

## Task 4: order-api — FastAPI app and routes (baseline, no bugs)

**Files:**
- Create: `order-api/app/main.py`

**Interfaces:**
- Consumes: everything produced in Task 2 (`logic.py`) and Task 3 (`db.py`, `kafka_producer.py`, `logging_config.py`).
- Produces: FastAPI `app` object with routes `POST /orders`, `PATCH /orders/{id}/status`, `GET /orders/{id}/delivery`, `POST /orders/dispatch-batch`, `POST /admin/reset` — this is the file the bug/fix commits in Tasks 11 and 13 will modify.

No new automated tests here (route wiring needs live Postgres/Kafka; covered by Task 9's manual verification per Global Constraints).

- [ ] **Step 1: Write `order-api/app/main.py`**

```python
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
```

- [ ] **Step 2: Lint**

Run: `python -m ruff check app`
Expected: no findings

- [ ] **Step 3: Commit**

```bash
git add order-api/app/main.py
git commit -m "feat(order-api): wire up FastAPI routes (baseline, no bugs)"
```

---

## Task 5: delivery-worker — pure business logic (`logic.py`), TDD

**Files:**
- Create: `delivery-worker/requirements.txt`
- Create: `delivery-worker/requirements-dev.txt`
- Create: `delivery-worker/worker/__init__.py`
- Create: `delivery-worker/worker/logic.py`
- Test: `delivery-worker/tests/test_logic.py`

**Interfaces:**
- Produces: `DELIVERY_LEAD_TIME: timedelta`, `compute_scheduled_at(now: datetime) -> datetime` — consumed by `worker/main.py` in Task 7.

- [ ] **Step 1: Create `delivery-worker/requirements.txt`**

```
confluent-kafka==2.5.0
psycopg[binary]==3.2.1
python-json-logger==2.0.7
```

- [ ] **Step 2: Create `delivery-worker/requirements-dev.txt`**

```
pytest==8.3.2
ruff==0.6.4
```

- [ ] **Step 3: Create empty `delivery-worker/worker/__init__.py`**

- [ ] **Step 4: Write the failing test in `delivery-worker/tests/test_logic.py`**

```python
from datetime import datetime, timedelta, timezone

from worker.logic import DELIVERY_LEAD_TIME, compute_scheduled_at


def test_compute_scheduled_at_adds_lead_time():
    now = datetime(2026, 9, 13, 10, 0, 0, tzinfo=timezone.utc)
    assert compute_scheduled_at(now) == now + DELIVERY_LEAD_TIME


def test_lead_time_is_two_days():
    assert DELIVERY_LEAD_TIME == timedelta(days=2)
```

- [ ] **Step 5: Run test to verify it fails**

Run (from `delivery-worker/`): `python -m pytest tests/test_logic.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'worker.logic'`

- [ ] **Step 6: Write `delivery-worker/worker/logic.py`**

```python
from datetime import datetime, timedelta

DELIVERY_LEAD_TIME = timedelta(days=2)


def compute_scheduled_at(now: datetime) -> datetime:
    return now + DELIVERY_LEAD_TIME
```

- [ ] **Step 7: Run test to verify it passes**

Run: `python -m pytest tests/test_logic.py -v`
Expected: 2 passed

- [ ] **Step 8: Lint**

Run: `python -m ruff check worker tests`
Expected: no findings

- [ ] **Step 9: Commit**

```bash
git add delivery-worker/requirements.txt delivery-worker/requirements-dev.txt \
        delivery-worker/worker/__init__.py delivery-worker/worker/logic.py \
        delivery-worker/tests/test_logic.py
git commit -m "feat(delivery-worker): add pure scheduled-at calculation logic"
```

---

## Task 6: delivery-worker — DB access and logging config

**Files:**
- Create: `delivery-worker/worker/db.py`
- Create: `delivery-worker/worker/logging_config.py`

**Interfaces:**
- Produces: `db.insert_delivery(order_id: int, scheduled_at: datetime) -> None` (idempotent: no-op if a delivery already exists for `order_id` — this exact function body is what the consumer-bug regression in Task 14 strips out), `logging_config.configure_logging() -> None`.

No automated tests (DB I/O, covered manually in Task 9).

- [ ] **Step 1: Write `delivery-worker/worker/db.py`**

```python
import os
from datetime import datetime

import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.environ["DATABASE_URL"]


def _connect() -> psycopg.Connection:
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def insert_delivery(order_id: int, scheduled_at: datetime) -> None:
    with _connect() as conn:
        existing = conn.execute(
            "SELECT 1 FROM deliveries WHERE order_id = %s", (order_id,)
        ).fetchone()
        if existing is not None:
            return
        conn.execute(
            "INSERT INTO deliveries (order_id, scheduled_at) VALUES (%s, %s)",
            (order_id, scheduled_at),
        )
        conn.commit()
```

- [ ] **Step 2: Write `delivery-worker/worker/logging_config.py`**

```python
import logging
import os

from pythonjsonlogger import jsonlogger


def configure_logging() -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    log_file = os.environ.get("LOG_FILE")
    if log_file:
        handlers.append(logging.FileHandler(log_file))
    formatter = jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    for handler in handlers:
        handler.setFormatter(formatter)
    logging.basicConfig(level=logging.INFO, handlers=handlers, force=True)
```

- [ ] **Step 3: Lint**

Run: `python -m ruff check worker`
Expected: no findings

- [ ] **Step 4: Commit**

```bash
git add delivery-worker/worker/db.py delivery-worker/worker/logging_config.py
git commit -m "feat(delivery-worker): add idempotent delivery insert and logging config"
```

---

## Task 7: delivery-worker — consumer loop (baseline, no bugs)

**Files:**
- Create: `delivery-worker/worker/main.py`

**Interfaces:**
- Consumes: `worker.logic.compute_scheduled_at`, `worker.db.insert_delivery`, `worker.logging_config.configure_logging`.
- Produces: `if __name__ == "__main__"` entrypoint that runs the consume loop forever — this is the file the bug/fix commits in Tasks 14 and 16 will modify.

- [ ] **Step 1: Write `delivery-worker/worker/main.py`**

```python
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
            "enable.auto.commit": False,
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
            consumer.commit(msg)
    finally:
        consumer.close()


if __name__ == "__main__":
    run()
```

Note on the poison-message path: the offset is committed unconditionally after the try/except, whether or not `handle_message` raised. That is intentional baseline behavior (a message with a permanently broken payload must not stall the whole consumer group), not a bug — Bug 2 (Task 11) exploits exactly this to make the caller see 200 OK while no delivery is ever created.

- [ ] **Step 2: Lint**

Run: `python -m ruff check worker`
Expected: no findings

- [ ] **Step 3: Commit**

```bash
git add delivery-worker/worker/main.py
git commit -m "feat(delivery-worker): add Kafka consumer loop (baseline, no bugs)"
```

---

## Task 8: Dockerfiles for both services

**Files:**
- Create: `order-api/Dockerfile`
- Create: `delivery-worker/Dockerfile`

**Interfaces:**
- Consumes: `requirements.txt` and the `app`/`worker` packages from Tasks 2–7.
- Produces: buildable images referenced by `docker-compose.yml` in Task 9.

- [ ] **Step 1: Write `order-api/Dockerfile`**

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 2: Write `delivery-worker/Dockerfile`**

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY worker ./worker
CMD ["python", "-m", "worker.main"]
```

- [ ] **Step 3: Commit**

```bash
git add order-api/Dockerfile delivery-worker/Dockerfile
git commit -m "chore: add Dockerfiles for order-api and delivery-worker"
```

---

## Task 9: docker-compose stack (Kafka, Kafka UI, Postgres, ELK, both services) + baseline verification

**Files:**
- Create: `filebeat/filebeat.yml`
- Create: `docker-compose.yml`

**Interfaces:**
- Consumes: `db/init/001_schema.sql` (Task 1), both Dockerfiles (Task 8), env vars `DATABASE_URL`, `KAFKA_BOOTSTRAP_SERVERS`, `LOG_FILE` read by `app/db.py`, `app/kafka_producer.py`, `worker/db.py`, `*/logging_config.py`.
- Produces: a fully running stand — the last verification step in this task is the acceptance test for every task so far (Scenario 0 from `docs/DESIGN.md`).

- [ ] **Step 1: Write `filebeat/filebeat.yml`**

```yaml
filebeat.inputs:
  - type: filestream
    id: app-logs
    paths:
      - /logs/*.log
    parsers:
      - ndjson:
          target: ""
          overwrite_keys: true
          add_error_key: true

output.elasticsearch:
  hosts: ["http://elasticsearch:9200"]
  index: "app-logs-%{+yyyy.MM.dd}"

setup.template.enabled: false
setup.ilm.enabled: false
```

- [ ] **Step 2: Write `docker-compose.yml`**

```yaml
services:
  kafka:
    image: apache/kafka:3.7.0
    container_name: kafka
    ports:
      - "9094:9094"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,PLAINTEXT_HOST://0.0.0.0:9094
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9094
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_NUM_PARTITIONS: 1
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"
    volumes:
      - kafka-data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list"]
      interval: 10s
      timeout: 10s
      retries: 15

  kafka-ui:
    image: provectuslabs/kafka-ui:v0.7.2
    container_name: kafka-ui
    ports:
      - "8081:8080"
    environment:
      KAFKA_CLUSTERS_0_NAME: local
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092
    depends_on:
      kafka:
        condition: service_healthy

  postgres:
    image: postgres:16-alpine
    container_name: postgres
    environment:
      POSTGRES_DB: orders_db
      POSTGRES_USER: orders_user
      POSTGRES_PASSWORD: orders_password
    ports:
      - "5432:5432"
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./db/init:/docker-entrypoint-initdb.d
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U orders_user -d orders_db"]
      interval: 5s
      timeout: 5s
      retries: 10

  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.13.4
    container_name: elasticsearch
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false
      - ES_JAVA_OPTS=-Xms512m -Xmx512m
    ports:
      - "9200:9200"
    volumes:
      - es-data:/usr/share/elasticsearch/data

  kibana:
    image: docker.elastic.co/kibana/kibana:8.13.4
    container_name: kibana
    environment:
      ELASTICSEARCH_HOSTS: http://elasticsearch:9200
    ports:
      - "5601:5601"
    depends_on:
      - elasticsearch

  filebeat:
    image: docker.elastic.co/beats/filebeat:8.13.4
    container_name: filebeat
    user: root
    command: ["--strict.perms=false"]
    volumes:
      - ./filebeat/filebeat.yml:/usr/share/filebeat/filebeat.yml:ro
      - ./logs:/logs:ro
    depends_on:
      - elasticsearch

  order-api:
    build: ./order-api
    container_name: order-api
    environment:
      DATABASE_URL: postgresql://orders_user:orders_password@postgres:5432/orders_db
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      LOG_FILE: /logs/order-api.log
    ports:
      - "8000:8000"
    volumes:
      - ./logs:/logs
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_healthy

  delivery-worker:
    build: ./delivery-worker
    container_name: delivery-worker
    environment:
      DATABASE_URL: postgresql://orders_user:orders_password@postgres:5432/orders_db
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
      LOG_FILE: /logs/delivery-worker.log
    volumes:
      - ./logs:/logs
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_healthy

volumes:
  kafka-data:
  postgres-data:
  es-data:
```

- [ ] **Step 3: Bring up the stack**

Run: `docker compose up --build -d`
Expected: all services reach `Up`/`healthy` within a few minutes. Check with `docker compose ps`. If `kafka` doesn't pass its healthcheck, run `docker compose logs kafka` — KRaft env vars are version-sensitive; the recipe above is verified for `apache/kafka:3.7.0`.

- [ ] **Step 4: Verify Swagger comes up and orders were seeded**

Run: `curl -s http://localhost:8000/orders/1/delivery` (expect `404 delivery not found` — order 1 is seeded as `NEW`, no delivery yet). Open `http://localhost:8000/docs` in a browser and confirm the 5 endpoints are listed.

- [ ] **Step 5: Verify Scenario 0 end-to-end (create → status change → 1 Kafka message → 1 delivery row)**

```bash
curl -s -X POST http://localhost:8000/orders \
  -H "Content-Type: application/json" \
  -d '{"customer_name": "Test User", "customer_address": "Test City"}'
# note the returned "id", call it $ID

curl -s -X PATCH http://localhost:8000/orders/$ID/status \
  -H "Content-Type: application/json" -d '{"status": "IN_PROGRESS"}'

curl -s -X PATCH http://localhost:8000/orders/$ID/status \
  -H "Content-Type: application/json" -d '{"status": "READY_FOR_DELIVERY"}'

curl -s http://localhost:8000/orders/$ID/delivery
```

Expected: the last call returns one delivery row with `scheduled_at` ~2 days in the future. Open `http://localhost:8081` (Kafka UI) → topic `order.status_changed` → confirm exactly 1 message with that order's `correlationId`. Open `http://localhost:5601` (Kibana) → Discover → index `app-logs-*` → confirm log lines from both `order-api` and `delivery-worker` carrying that `correlationId`.

- [ ] **Step 6: Verify `dispatch-batch` against seeded data**

```bash
curl -s -X POST http://localhost:8000/orders/dispatch-batch
```

Expected: `{"dispatchedCount": 6}` (5 fully-seeded `READY_FOR_DELIVERY` orders + the boundary "Легаси-заказ #1" — baseline code has no bug, so the `NULL`-address order dispatches fine too). Confirm 6 messages in Kafka UI under `order.dispatch_requested`, and 6 new rows in `deliveries` (verify via `docker compose exec postgres psql -U orders_user -d orders_db -c "SELECT count(*) FROM deliveries;"`).

- [ ] **Step 7: Commit**

```bash
git add filebeat/filebeat.yml docker-compose.yml
git commit -m "feat: add docker-compose stack (Kafka KRaft, Kafka UI, Postgres, ELK)"
```

---

## Task 10: GitHub Actions CI workflow

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `order-api/requirements*.txt`, `order-api/tests/test_logic.py`, `delivery-worker/requirements*.txt`, `delivery-worker/tests/test_logic.py` (Tasks 2, 5).
- Produces: the `pull_request` check both feature branches in Tasks 11–17 rely on staying green.

- [ ] **Step 1: Write `.github/workflows/ci.yml`**

```yaml
name: CI

on:
  pull_request:
    branches: [main]

jobs:
  order-api:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: order-api
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -r requirements.txt -r requirements-dev.txt
      - run: python -m ruff check app tests
      - run: python -m pytest tests -v

  delivery-worker:
    runs-on: ubuntu-latest
    defaults:
      run:
        working-directory: delivery-worker
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: "3.12"
      - run: pip install -r requirements.txt -r requirements-dev.txt
      - run: python -m ruff check worker tests
      - run: python -m pytest tests -v
```

- [ ] **Step 2: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add GitHub Actions lint+test workflow for both services"
```

---

## Task 11: `feature/producer-bugs` — introduce bugs 1–3

**Files:**
- Modify: `order-api/app/main.py`

**Interfaces:**
- Consumes: `logic.build_status_changed_event`, `logic.build_dispatch_requested_event` (Task 2); `kafka_producer.publish`, `db.get_order`, `db.update_order_status`, `db.get_orders_by_status` (Task 3).
- Produces: no new interfaces — this task only changes the *behavior* of `update_status` and `dispatch_batch`. `delivery-worker` is untouched on this branch.

- [ ] **Step 1: Create and switch to the branch**

```bash
git checkout main
git checkout -b feature/producer-bugs
```

- [ ] **Step 2: Edit `update_status` in `order-api/app/main.py`** to introduce Bug 1 (unconditional duplicate publish on every "normal" transition into `READY_FOR_DELIVERY`) and Bug 2 (corrupted, field-missing payload specifically on the `NEW → READY_FOR_DELIVERY` skip-transition):

Replace:

```python
    db.update_order_status(order_id, body.status)
    logger.info(
        "order status updated",
        extra={"correlationId": str(order_id), "orderId": order_id, "status": body.status},
    )
    if body.status == "READY_FOR_DELIVERY":
        event = build_status_changed_event(order_id, body.status)
        kafka_producer.publish("order.status_changed", key=str(order_id), value=event)
    return {"id": order_id, "status": body.status}
```

with:

```python
    previous_status = order["status"]
    db.update_order_status(order_id, body.status)
    logger.info(
        "order status updated",
        extra={"correlationId": str(order_id), "orderId": order_id, "status": body.status},
    )
    if body.status == "READY_FOR_DELIVERY":
        if previous_status == "NEW":
            # Fast path for orders pre-packed before intake — legacy, rarely hit.
            event = {
                "correlationId": str(order_id),
                "eventType": "order.status_changed",
                "status": body.status,
            }
            kafka_producer.publish("order.status_changed", key=str(order_id), value=event)
        else:
            event = build_status_changed_event(order_id, body.status)
            kafka_producer.publish("order.status_changed", key=str(order_id), value=event)
            _notify_legacy_listeners(order_id, body.status)
    return {"id": order_id, "status": body.status}


def _notify_legacy_listeners(order_id: int, status: str) -> None:
    event = build_status_changed_event(order_id, status)
    kafka_producer.publish("order.status_changed", key=str(order_id), value=event)
```

- [ ] **Step 3: Edit `dispatch_batch` in `order-api/app/main.py`** to introduce Bug 3 (silently skips one order via a bare `except`, but reports a count computed independently of what actually sent):

Replace:

```python
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
```

with:

```python
@app.post("/orders/dispatch-batch")
def dispatch_batch():
    orders = db.get_orders_by_status("READY_FOR_DELIVERY")
    for order in orders:
        try:
            logger.info("dispatching order to %s", order["customer_address"].upper())
            event = build_dispatch_requested_event(order["id"])
            kafka_producer.publish("order.dispatch_requested", key=str(order["id"]), value=event)
        except Exception:
            continue
    dispatched = db.count_orders_by_status("READY_FOR_DELIVERY")
    logger.info("dispatch batch completed", extra={"dispatchedCount": dispatched})
    return {"dispatchedCount": dispatched}
```

- [ ] **Step 4: Run the existing unit tests to confirm CI stays green despite the bugs**

Run (from `order-api/`): `python -m pytest tests -v && python -m ruff check app tests`
Expected: all pass — these tests only exercise `logic.py`, which was not touched, so they cannot see either bug. This is the concrete demonstration of the CI gap from `docs/LESSON_PLAN.md` section 3.

- [ ] **Step 5: Commit the bug**

```bash
git add order-api/app/main.py
git commit -m "feat(order-api): add fast-path status transitions and dispatch-batch logging"
```

(Commit message deliberately reads like a plausible, innocuous feature commit — matches the real-world framing that CI is green and nothing looks alarming in the diff summary.)

---

## Task 12: `feature/producer-bugs` — manual bug verification

**Files:** none (verification only, against the running stand from Task 9)

**Interfaces:** none

- [ ] **Step 1: Rebuild `order-api` from the branch**

```bash
docker compose up --build -d order-api
```

- [ ] **Step 2: Reproduce Bug 1 (duplicate message)**

```bash
curl -s -X POST http://localhost:8000/orders -H "Content-Type: application/json" \
  -d '{"customer_name": "Bug1 Test", "customer_address": "X"}'
# note $ID, then:
curl -s -X PATCH http://localhost:8000/orders/$ID/status -d '{"status":"IN_PROGRESS"}' -H "Content-Type: application/json"
curl -s -X PATCH http://localhost:8000/orders/$ID/status -d '{"status":"READY_FOR_DELIVERY"}' -H "Content-Type: application/json"
```

Expected: Kafka UI shows **2** messages on `order.status_changed` for this `correlationId` (both well-formed).

- [ ] **Step 3: Reproduce Bug 2 (corrupted message, skip-transition)**

```bash
curl -s -X POST http://localhost:8000/orders -H "Content-Type: application/json" \
  -d '{"customer_name": "Bug2 Test", "customer_address": "Y"}'
# note $ID2, then:
curl -s -X PATCH http://localhost:8000/orders/$ID2/status -d '{"status":"READY_FOR_DELIVERY"}' -H "Content-Type: application/json"
curl -s http://localhost:8000/orders/$ID2/delivery
```

Expected: Kafka UI shows exactly **1** message for `$ID2`, missing `orderId`/`occurredAt`. The `GET .../delivery` call returns 404 (no delivery created). Kibana shows a `failed to process message` log line from `delivery-worker` with this `correlationId`.

- [ ] **Step 4: Reproduce Bug 3 (batch undercount)**

```bash
docker compose exec postgres psql -U orders_user -d orders_db \
  -c "SELECT count(*) FROM orders WHERE status = 'READY_FOR_DELIVERY';"
curl -s -X POST http://localhost:8000/orders/dispatch-batch
```

Expected: `dispatchedCount` in the response equals the `SELECT count(*)` above (both count DB rows, per the bug), but Kafka UI's `order.dispatch_requested` topic has one fewer message than that count — the `NULL`-address seed order ("Легаси-заказ #1") is silently skipped.

- [ ] **Step 5: No commit** (this task only verifies the branch matches `docs/LESSON_PLAN.md` §2's table)

---

## Task 13: `feature/producer-bugs` — fix commit and re-verification

**Files:**
- Modify: `order-api/app/main.py`

**Interfaces:** none new — restores the Task 4 baseline behavior exactly.

- [ ] **Step 1: Revert the bug commit**

```bash
git log --oneline -3   # confirm the bug commit's hash is HEAD
git revert --no-edit HEAD
```

This restores `order-api/app/main.py` to byte-for-byte the Task 4 baseline, guaranteeing the fix is exactly the previously-tested-correct code — no new bugs can be introduced by hand-writing a "different" fix.

- [ ] **Step 2: Run unit tests**

Run (from `order-api/`): `python -m pytest tests -v && python -m ruff check app tests`
Expected: all pass (unchanged from before — this is the point: CI was green before and after, only real behavior changed)

- [ ] **Step 3: Rebuild and re-verify Scenario 0 style checks**

```bash
docker compose up --build -d order-api
```

Repeat Task 12 Steps 2–4 against fresh orders: expect exactly 1 message for the duplicate-publish case, a well-formed message + created delivery for the skip-transition case, and `dispatchedCount` matching the actual message count in `order.dispatch_requested` for the batch case.

- [ ] **Step 4: No further commit needed** (the revert in Step 1 is the fix commit)

---

## Task 14: `feature/consumer-bug` — introduce bug 4

**Files:**
- Modify: `delivery-worker/worker/main.py`
- Modify: `delivery-worker/worker/db.py`

**Interfaces:**
- Consumes: same as Task 7 and Task 6.
- Produces: no new interfaces — behavior-only regression. `order-api` is untouched on this branch (branched from `main`, not from `feature/producer-bugs`).

- [ ] **Step 1: Create the branch from `main`**

```bash
git checkout main
git checkout -b feature/consumer-bug
```

- [ ] **Step 2: Edit `delivery-worker/worker/db.py`** — remove the idempotency check:

Replace:

```python
def insert_delivery(order_id: int, scheduled_at: datetime) -> None:
    with _connect() as conn:
        existing = conn.execute(
            "SELECT 1 FROM deliveries WHERE order_id = %s", (order_id,)
        ).fetchone()
        if existing is not None:
            return
        conn.execute(
            "INSERT INTO deliveries (order_id, scheduled_at) VALUES (%s, %s)",
            (order_id, scheduled_at),
        )
        conn.commit()
```

with:

```python
def insert_delivery(order_id: int, scheduled_at: datetime) -> None:
    with _connect() as conn:
        conn.execute(
            "INSERT INTO deliveries (order_id, scheduled_at) VALUES (%s, %s)",
            (order_id, scheduled_at),
        )
        conn.commit()
```

- [ ] **Step 3: Edit `delivery-worker/worker/main.py`** — switch to auto-commit with a long interval and drop the manual commit, so a restart shortly after processing reliably replays the last message:

Replace the `Consumer(...)` construction:

```python
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": "delivery-worker",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
```

with:

```python
    consumer = Consumer(
        {
            "bootstrap.servers": os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            "group.id": "delivery-worker",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 60000,
            "auto.offset.reset": "earliest",
        }
    )
```

and remove the now-redundant manual commit call at the end of the loop body:

Replace:

```python
            try:
                handle_message(msg.value())
            except Exception:
                logger.exception(
                    "failed to process message",
                    extra={"correlationId": _safe_correlation_id(msg.value())},
                )
            consumer.commit(msg)
```

with:

```python
            try:
                handle_message(msg.value())
            except Exception:
                logger.exception(
                    "failed to process message",
                    extra={"correlationId": _safe_correlation_id(msg.value())},
                )
```

- [ ] **Step 4: Run unit tests to confirm CI stays green**

Run (from `delivery-worker/`): `python -m pytest tests -v && python -m ruff check worker tests`
Expected: all pass — `compute_scheduled_at` is untouched; the offset/idempotency regression lives entirely in I/O code the unit tests don't exercise.

- [ ] **Step 5: Commit the bug**

```bash
git add delivery-worker/worker/main.py delivery-worker/worker/db.py
git commit -m "perf(delivery-worker): reduce commit overhead with batched auto-commit"
```

---

## Task 15: `feature/consumer-bug` — manual bug verification

**Files:** none (verification only)

**Interfaces:** none

- [ ] **Step 1: Rebuild `delivery-worker` from the branch**

```bash
docker compose up --build -d delivery-worker
```

- [ ] **Step 2: Trigger and reproduce Bug 4**

```bash
curl -s -X POST http://localhost:8000/orders -H "Content-Type: application/json" \
  -d '{"customer_name": "Bug4 Test", "customer_address": "Z"}'
# note $ID3, then:
curl -s -X PATCH http://localhost:8000/orders/$ID3/status -d '{"status":"IN_PROGRESS"}' -H "Content-Type: application/json"
curl -s -X PATCH http://localhost:8000/orders/$ID3/status -d '{"status":"READY_FOR_DELIVERY"}' -H "Content-Type: application/json"
sleep 2
docker compose restart delivery-worker
sleep 5
curl -s http://localhost:8000/orders/$ID3/delivery
```

Expected: Kafka UI shows exactly **1** message on `order.status_changed` for `$ID3`. The `deliveries` table has **2** rows for `order_id = $ID3` (check via `docker compose exec postgres psql -U orders_user -d orders_db -c "SELECT * FROM deliveries WHERE order_id = $ID3;"`) — because the restart happened well inside the 60s auto-commit window, so the offset was never persisted and the worker replayed the message on restart.

- [ ] **Step 3: No commit**

---

## Task 16: `feature/consumer-bug` — fix commit and re-verification

**Files:**
- Modify: `delivery-worker/worker/main.py`
- Modify: `delivery-worker/worker/db.py`

**Interfaces:** none new — restores the Task 6/7 baseline exactly.

- [ ] **Step 1: Revert the bug commit**

```bash
git log --oneline -3   # confirm the bug commit's hash is HEAD
git revert --no-edit HEAD
```

- [ ] **Step 2: Run unit tests**

Run (from `delivery-worker/`): `python -m pytest tests -v && python -m ruff check worker tests`
Expected: all pass

- [ ] **Step 3: Rebuild and re-verify**

```bash
docker compose up --build -d delivery-worker
```

Repeat Task 15 Step 2 against a fresh order: expect exactly **1** row in `deliveries` for the new order even after an immediate `docker compose restart delivery-worker`.

- [ ] **Step 4: No further commit needed** (the revert in Step 1 is the fix commit)

---

## Task 17: Push `main` and both feature branches, open PRs

This is the only task that touches the shared remote (GitHub) rather than the local working tree — confirm with the user before running it, and confirm the target remote/repo name first (`git remote -v`).

**Files:** none

**Interfaces:** none

- [ ] **Step 1: Push `main`**

```bash
git checkout main
git push origin main
```

- [ ] **Step 2: Push `feature/producer-bugs` and open its PR**

```bash
git push origin feature/producer-bugs
gh pr create --base main --head feature/producer-bugs \
  --title "Producer-side fast paths and batch dispatch logging" \
  --body "Adds a fast path for pre-packed order intake and structured logging for dispatch-batch."
```

- [ ] **Step 3: Push `feature/consumer-bug` and open its PR**

```bash
git push origin feature/consumer-bug
gh pr create --base main --head feature/consumer-bug \
  --title "Reduce delivery-worker commit overhead" \
  --body "Batches offset commits to cut Kafka round-trips under load."
```

- [ ] **Step 4: Confirm both PRs show green checks**

Open each PR's **Checks** tab on GitHub and confirm both jobs (`order-api`, `delivery-worker`) pass — this is the final acceptance check for the whole plan: two green PRs that each hide a real, reproducible bug, exactly as specified in `docs/DESIGN.md` §5.

---

## Self-Review Notes

- **Spec coverage:** all 4 endpoints (Task 4), all 4 bugs + their single fix commits each (Tasks 11/13, 14/16), KRaft/single-partition Kafka + Kafka UI + Postgres + ELK (Task 9), seed data with the boundary NULL-address row (Task 3), `POST /admin/reset` (Task 4), CI on `pull_request` to `main` with mocked-Kafka unit tests only (Task 10), correlationId = orderId threaded through logs and events (Tasks 2, 4, 7) are all covered.
- **Out of scope confirmed absent:** no real CD/deploy step, no consumer-lag scenario, no per-student stand — none of these appear anywhere above, matching `docs/DESIGN.md` §7.
- **Type/name consistency checked:** `db.py`, `kafka_producer.py`, `logic.py` function names and signatures are identical between their defining task and every later task that calls them (`main.py` in Tasks 4/11/13 for order-api; `main.py` in Tasks 7/14/16 for delivery-worker).
