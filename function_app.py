import logging
import json
import time
from datetime import datetime
import azure.functions as func

# Local modules
from sql_client import (
    fetch_unprocessed_rows,
    mark_row_processed,
    insert_processed_event,
    dead_letter,
    increment_metric,
    insert_event,
)
from transform_event import transform_event
from logging_config import setup_logging

# ------------------------------------------------------------
# Configure logging
# ------------------------------------------------------------
setup_logging()

# Create the Function App
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


# ------------------------------------------------------------
# Retry helper — exponential backoff
# ------------------------------------------------------------
def retry(operation, retries=3, base_delay=0.5, backoff=2):
    delay = base_delay

    for attempt in range(1, retries + 1):
        try:
            return operation()
        except Exception as e:
            logging.error(f"Attempt {attempt} failed: {e}")

            if attempt == retries:
                raise

            time.sleep(delay)
            delay *= backoff


# ------------------------------------------------------------
# HTTP TRIGGER — simple test endpoint
# ------------------------------------------------------------
@app.route(route="http_trigger")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    name = req.params.get("name")
    if not name:
        try:
            req_body = req.get_json()
            name = req_body.get("name")
        except ValueError:
            pass

    if name:
        return func.HttpResponse(f"Hello, {name}. This HTTP triggered function executed successfully.")
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. "
            "Pass a name in the query string or in the request body for a personalized response.",
            status_code=200,
        )


# ------------------------------------------------------------
# HTTP TRIGGER — SendGrid webhook ingestion
# ------------------------------------------------------------
@app.function_name(name="receive_events")
@app.route(route="receive_events", auth_level=func.AuthLevel.ANONYMOUS)
def receive_events(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Webhook received")

    try:
        events = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    if not isinstance(events, list):
        return func.HttpResponse("Expected a list of events", status_code=400)

    for event in events:
        insert_event(event)

    increment_metric("events_ingested", len(events))
    return func.HttpResponse("OK", status_code=200)

# ------------------------------------------------------------
# HTTP TRIGGER — Reprocess dead‑lettered events
# ------------------------------------------------------------
@app.function_name(name="reprocess_dead_letters")
@app.route(route="reprocess_dead_letters", auth_level=func.AuthLevel.ANONYMOUS, methods=["POST"])
def reprocess_dead_letters(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Dead‑letter reprocessing endpoint invoked.")

    from sql_client import fetch_dead_letter_batch, move_dead_letter_to_staging

    # Optional: allow batch size override
    try:
        body = req.get_json()
        batch_size = body.get("batch_size", 50)
    except Exception:
        batch_size = 50

    rows = fetch_dead_letter_batch(batch_size=batch_size)

    if not rows:
        logging.info("No dead‑lettered events to reprocess.")
        return func.HttpResponse("No dead‑lettered events to reprocess.", status_code=200)

    reprocessed = 0

    for row in rows:
        dl_id = row.id
        payload_raw = row.payload

        try:
            payload = json.loads(payload_raw)
        except Exception as e:
            logging.error(f"Dead‑letter {dl_id}: invalid JSON, skipping: {e}")
            increment_metric("dead_letter_invalid_json_reprocess")
            continue

        # Determine event type (fallback to 'unknown')
        event_type = payload.get("event", "unknown")

        # Move back into staging
        move_dead_letter_to_staging(dl_id, payload, event_type=event_type)
        reprocessed += 1

    increment_metric("dead_letter_reprocessed", reprocessed)

    msg = f"Reprocessed {reprocessed} dead‑lettered events back into staging."
    logging.info(msg)
    return func.HttpResponse(msg, status_code=200)


# ------------------------------------------------------------
# TIMER TRIGGER — Lightweight heartbeat
# ------------------------------------------------------------
@app.timer_trigger(
    schedule="0 */5 * * * *",
    arg_name="myTimer",
    run_on_startup=False,
    use_monitor=False,
)
def timer_heartbeat(myTimer: func.TimerRequest) -> None:
    if myTimer.past_due:
        logging.info("The timer is past due!")

    logging.info("Python timer heartbeat executed.")


# ------------------------------------------------------------
# TIMER TRIGGER — Main staging processor
# ------------------------------------------------------------
@app.function_name(name="process_staged_rows")
@app.schedule(schedule="0 */5 * * * *", arg_name="mytimer")
def process_staged_rows(mytimer: func.TimerRequest) -> None:
    logging.info("Processing staged rows...")

    # Fetch rows with retry
    try:
        rows = retry(lambda: fetch_unprocessed_rows(batch_size=50))
    except Exception as e:
        logging.error(f"Failed to fetch unprocessed rows after retries: {e}")
        increment_metric("staging_fetch_failures")
        return

    if not rows:
        logging.info("No staged rows to process.")
        return

    processed_count = 0
    dead_letter_count = 0

    for row in rows:
        row_id = row.id
        event_type = row.event_type
        payload_raw = row.payload

        logging.info(f"Processing event {row_id} ({event_type})")

        # Decode JSON
        try:
            payload = json.loads(payload_raw)
        except Exception as e:
            logging.error(f"Row {row_id}: invalid JSON payload, moving to dead-letter: {e}")
            dead_letter(row_id, {"raw_payload": payload_raw}, f"JSON decode error: {e}")
            increment_metric("dead_letter_invalid_json")
            dead_letter_count += 1
            continue

        # Transform event (optional but recommended)
        try:
            transformed = transform_event(row)
        except Exception as e:
            logging.error(f"Row {row_id}: transform failed, moving to dead-letter: {e}")
            dead_letter(row_id, payload, f"Transform error: {e}")
            increment_metric("dead_letter_transform_failures")
            dead_letter_count += 1
            continue

        # Process event
        try:
            def process_op():
                insert_processed_event(event_type, transformed, row_id)

            retry(process_op)
            retry(lambda: mark_row_processed(row_id))

            processed_count += 1
            increment_metric("events_processed")

        except Exception as e:
            logging.error(f"Row {row_id}: processing failed after retries, moving to dead-letter: {e}")
            dead_letter(row_id, payload, e)
            increment_metric("dead_letter_processing_failures")
            dead_letter_count += 1

    logging.info(
        f"Staged row processing complete. "
        f"Processed: {processed_count}, Dead-lettered: {dead_letter_count}"
    )
