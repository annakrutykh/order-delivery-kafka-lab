from datetime import datetime, timedelta

DELIVERY_LEAD_TIME = timedelta(days=2)


def compute_scheduled_at(now: datetime) -> datetime:
    return now + DELIVERY_LEAD_TIME
