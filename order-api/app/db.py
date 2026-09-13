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
