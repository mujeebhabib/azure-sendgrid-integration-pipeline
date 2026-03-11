import json

def transform_event(row):
    payload = json.loads(row.payload)

    return {
        "id": row.id,
        "event_type": row.event_type,
        "email": payload.get("email"),
        "timestamp": payload.get("timestamp"),
        "metadata": payload.get("sg_event_id")
    }
