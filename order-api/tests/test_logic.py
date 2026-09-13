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


def test_same_status_reset_is_invalid():
    assert is_valid_transition("READY_FOR_DELIVERY", "READY_FOR_DELIVERY") is False


def test_new_to_delivered_is_invalid_illegal_jump():
    assert is_valid_transition("NEW", "DELIVERED") is False


def test_unknown_current_status_is_invalid():
    assert is_valid_transition("BOGUS", "NEW") is False


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
