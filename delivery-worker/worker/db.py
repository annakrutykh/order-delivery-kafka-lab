import os
from datetime import datetime

import psycopg
from psycopg.rows import dict_row

DATABASE_URL = os.environ["DATABASE_URL"]


def _connect() -> psycopg.Connection:
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def insert_delivery(order_id: int, scheduled_at: datetime) -> None:
    with _connect() as conn:
        conn.execute(
            "INSERT INTO deliveries (order_id, scheduled_at) VALUES (%s, %s)",
            (order_id, scheduled_at),
        )
        conn.commit()
