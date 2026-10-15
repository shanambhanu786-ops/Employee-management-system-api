"""
MongoDB change stream -> Kafka.

  python -m stream.producer
Env: MONGO_URI, MONGO_DB, KAFKA_BOOTSTRAP (default localhost:9092), KAFKA_TOPIC (default attendance.events)

Needs a replica set (Atlas or a single-node rs). Delivery is at-least-once: the resume token is saved only
after Kafka acknowledged the batch, so a crash replays events instead of losing them. Consumers are idempotent
(MERGE on event_id / latest-state-wins). Kafka key = emp_code|date, so one record's events stay ordered.

Event ops: INSERT (punch-in), UPDATE (punch-out etc.), CORRECTION (history was touched).
If the oplog no longer contains the saved token (stream down too long) the checkpoint is dropped and the
nightly `python -m lakehouse.pipeline ingest` reconciles the gap.
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone

log = logging.getLogger("producer")
UTC = timezone.utc
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
NAME = "attendance_events"
TOPIC = os.getenv("KAFKA_TOPIC", "attendance.events")
BATCH = 500


def to_ms(dt):
    if dt is None:
        return None
    dt = dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)
    return (dt - EPOCH) // timedelta(milliseconds=1)


def build_event(change: dict):
    """Pure function: change-stream document -> flat event dict (or None to skip)."""
    op = change.get("operationType")
    doc = change.get("fullDocument")
    if op not in ("insert", "update", "replace") or not doc:   # doc is None if deleted before lookup
        return None
    updated = (change.get("updateDescription") or {}).get("updatedFields") or {}
    touched_history = any(k == "history" or k.startswith("history.") for k in updated)
    kind = "INSERT" if op == "insert" else ("CORRECTION" if touched_history else "UPDATE")
    return {
        "event_id": change["_id"]["_data"],
        "op": kind,
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc.get("status"),
        "punch_in": to_ms(doc.get("punch_in")),
        "punch_out": to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history_len": len(doc.get("history") or []),
        "event_time": to_ms(change["clusterTime"].as_datetime()),
    }


def run():
    from confluent_kafka import Producer
    from pymongo import MongoClient
    from pymongo.errors import OperationFailure, PyMongoError

    db = MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"), tz_aware=True)[
        os.getenv("MONGO_DB", "attendance_db")]
    checkpoints = db.stream_checkpoints
    producer = Producer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"),
        "enable.idempotence": True, "acks": "all", "linger.ms": 20, "compression.type": "lz4"})
    pipeline = [{"$match": {"operationType": {"$in": ["insert", "update", "replace"]}}}]
    backoff = 1

    while True:
        errors = []

        def on_delivery(err, _msg):
            if err:
                errors.append(err)

        def commit(stream):
            producer.flush(30)
            if errors:
                raise RuntimeError(f"kafka delivery failed: {errors[0]}")
            checkpoints.replace_one({"_id": NAME}, {"_id": NAME, "token": stream.resume_token}, upsert=True)

        try:
            saved = checkpoints.find_one({"_id": NAME})
            kw = {"resume_after": saved["token"]} if saved else {}
            with db.attendance_logs.watch(pipeline, full_document="updateLookup", max_await_time_ms=1000, **kw) as stream:
                log.info("change stream open (%s)", "resumed" if saved else "from now")
                pending, backoff = 0, 1
                while stream.alive:
                    change = stream.try_next()
                    if change is not None:
                        ev = build_event(change)
                        if ev:
                            producer.produce(TOPIC, key=f"{ev['emp_code']}|{ev['date']}",
                                             value=json.dumps(ev), on_delivery=on_delivery)
                            pending += 1
                            producer.poll(0)
                    if pending and (pending >= BATCH or change is None):
                        commit(stream)
                        pending = 0
        except OperationFailure as exc:
            if exc.code == 286:                                  # ChangeStreamHistoryLost
                log.error("resume token fell off the oplog; dropping checkpoint, run the reconcile ingest")
                checkpoints.delete_one({"_id": NAME})
            else:
                log.exception("mongo error")
        except (PyMongoError, RuntimeError):
            log.exception("stream error; retrying from last checkpoint")
        time.sleep(backoff)
        backoff = min(backoff * 2, 30)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()
