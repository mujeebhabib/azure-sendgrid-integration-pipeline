import json
import logging
from datetime import datetime

def transform_event(row):
    """
    Transform a staging row into a clean, validated event.
    Any invalid or unexpected data raises an exception so the
    processor can route the event to DeadLetterEvents.
    """

    # Decode JSON
    try:
        payload = json.loads(row.payload)
    except Exception as e:
        logging.error(f"Row {row.id}: Failed to decode JSON payload: {e}")
        raise ValueError(f"Invalid JSON payload: {e}")

    # -----------------------------
    # VALIDATION SECTION
    # -----------------------------

    # Validate event type
    event_type = payload.get("event")
    if not isinstance(event_type, str):
        raise ValueError(f"Invalid event type: {event_type}")

    # Validate email
    email = payload.get("email")
    if not isinstance(email, str) or "@" not in email:
        raise ValueError(f"Invalid email: {email}")

    # Validate timestamp
    timestamp = payload.get("timestamp")
    if not isinstance(timestamp, int):
        raise ValueError(f"Invalid timestamp: {timestamp}")

    # -----------------------------
    # TRANSFORMATION SECTION
    # -----------------------------

    transformed = {
        "source_event_id": row.id,
        "event_type": event_type,
        "email": email,
        "timestamp": timestamp,
        "received_at": (
            row.received_at.isoformat()
            if hasattr(row.received_at, "isoformat")
            else str(row.received_at)
        ),
        "raw_payload": payload,
    }

    # Event-specific logic
    if event_type == "processed":
        transformed["status"] = "processed"
    elif event_type == "delivered":
        transformed["status"] = "delivered"
        transformed["smtp_id"] = payload.get("smtp-id")
    elif event_type == "bounce":
        transformed["status"] = "bounced"
        transformed["reason"] = payload.get("reason")
    elif event_type == "spamreport":
        transformed["status"] = "spam_reported"
    else:
        raise ValueError(f"Unknown event type: {event_type}")

    return transformed
