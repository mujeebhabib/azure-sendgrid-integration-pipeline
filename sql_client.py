import os
import json
import logging
import pyodbc
from datetime import datetime

def get_connection():
    conn_str = os.environ["SQL_CONNECTION_STRING"]
    return pyodbc.connect(conn_str)

# ---------- STAGING EVENTS ----------

def fetch_unprocessed_rows(batch_size=50):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT TOP (?) id, event_type, payload, received_at, processed, processed_at
        FROM dbo.StagingEvents
        WHERE processed = 0
        ORDER BY received_at ASC
    """, (batch_size,))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def mark_row_processed(row_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE dbo.StagingEvents
        SET processed = 1,
            processed_at = SYSUTCDATETIME()
        WHERE id = ?
    """, (row_id,))

    conn.commit()
    cursor.close()
    conn.close()

# ---------- PROCESSED EVENTS ----------

def insert_processed_event(event_type, payload, source_event_id):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO dbo.ProcessedEvents (event_type, payload, source_event_id)
        VALUES (?, ?, ?)
    """, (event_type, json.dumps(payload), source_event_id))

    conn.commit()
    cursor.close()
    conn.close()

# ---------- DEAD LETTER EVENTS ----------

def dead_letter(original_event_id, payload, error_message):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO dbo.DeadLetterEvents (original_event_id, payload, error_message)
        VALUES (?, ?, ?)
    """, (original_event_id, json.dumps(payload), str(error_message)))

    conn.commit()
    cursor.close()
    conn.close()

# ---------- METRICS ----------

def increment_metric(metric_name, amount=1):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        MERGE dbo.Metrics AS target
        USING (SELECT ? AS metric_name) AS source
        ON target.metric_name = source.metric_name
        WHEN MATCHED THEN
            UPDATE SET metric_value = target.metric_value + ?
        WHEN NOT MATCHED THEN
            INSERT (metric_name, metric_value) VALUES (?, ?);
    """, (metric_name, amount, metric_name, amount))

    conn.commit()
    cursor.close()
    conn.close()

# ---------- DEAD LETTER REPROCESSING ----------

def fetch_dead_letter_batch(batch_size=50):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT TOP (?) id, original_event_id, payload, error_message, failed_at
        FROM dbo.DeadLetterEvents
        ORDER BY failed_at ASC
    """, (batch_size,))

    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def move_dead_letter_to_staging(dead_letter_id, payload, event_type="unknown"):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO dbo.StagingEvents (event_type, payload, received_at, processed)
        VALUES (?, ?, SYSUTCDATETIME(), 0)
    """, (event_type, json.dumps(payload)))

    cursor.execute("""
        DELETE FROM dbo.DeadLetterEvents
        WHERE id = ?
    """, (dead_letter_id,))

    conn.commit()
    cursor.close()
    conn.close()

def insert_event(event):
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO dbo.StagingEvents (event_type, payload, received_at, processed)
        VALUES (?, ?, SYSUTCDATETIME(), 0)
    """, (
        event.get("event"),
        json.dumps(event)
    ))

    conn.commit()
    cursor.close()
    conn.close()
