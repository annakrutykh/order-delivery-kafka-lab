from datetime import datetime, timedelta, timezone

from worker.logic import DELIVERY_LEAD_TIME, compute_scheduled_at


def test_compute_scheduled_at_adds_lead_time():
    now = datetime(2026, 9, 13, 10, 0, 0, tzinfo=timezone.utc)
    assert compute_scheduled_at(now) == now + DELIVERY_LEAD_TIME


def test_lead_time_is_two_days():
    assert DELIVERY_LEAD_TIME == timedelta(days=2)
