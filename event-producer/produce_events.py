from __future__ import annotations

import csv
import json
import logging
import os
import sys
import time
from datetime import datetime
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

logging.basicConfig(
    format="%(asctime)s %(levelname)s event-producer | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

BOOTSTRAP   = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC       = os.environ.get("KAFKA_TOPIC", "user_events")
PARTITIONS  = int(os.environ.get("KAFKA_TOPIC_PARTITIONS", 3))
REPLICATION = int(os.environ.get("KAFKA_TOPIC_REPLICATION", 1))
EVENTS_CSV  = Path(os.environ.get("EVENTS_CSV", "/data/user_events.csv"))
DELAY_MS    = float(os.environ.get("PRODUCE_DELAY_MS", 0))
LOG_EVERY   = int(os.environ.get("LOG_EVERY", 2000))


def wait_for_kafka(timeout_s: int = 60) -> AdminClient:
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            md = admin.list_topics(timeout=3)
            log.info(f"kafka reachable; topics: {sorted(md.topics.keys())}")
            return admin
        except Exception as exc:
            log.info(f"waiting for kafka: {exc}")
            time.sleep(2)
    raise RuntimeError("kafka unreachable")


def ensure_topic(admin: AdminClient) -> None:
    md = admin.list_topics(timeout=5)
    if TOPIC in md.topics:
        n = len(md.topics[TOPIC].partitions)
        if n != PARTITIONS:
            log.warning(f"topic {TOPIC!r} exists with {n} partitions, expected {PARTITIONS}")
        else:
            log.info(f"topic {TOPIC!r} exists with {n} partitions")
        return

    log.info(f"creating topic {TOPIC!r} (partitions={PARTITIONS}, rf={REPLICATION})")
    fs = admin.create_topics([NewTopic(TOPIC, PARTITIONS, REPLICATION)])
    for t, fut in fs.items():
        try:
            fut.result(timeout=10)
            log.info(f"  created topic {t}")
        except Exception as exc:
            if "already exists" in str(exc).lower():
                log.info(f"  topic {t} already exists (race)")
            else:
                raise


def read_events_sorted(path: Path) -> list[dict]:
    rows: list[dict] = []
    with path.open() as f:
        for row in csv.DictReader(f):
            row["_event_dt"] = datetime.strptime(row["event_time"], "%Y-%m-%d %H:%M:%S")
            rows.append(row)
    rows.sort(key=lambda r: r["_event_dt"])
    log.info(f"read {len(rows):,} events from {path}; sorted by event_time")
    return rows


def main() -> int:
    log.info(f"=== producer starting (bootstrap={BOOTSTRAP}, topic={TOPIC}, delay={DELAY_MS}ms) ===")

    if not EVENTS_CSV.exists():
        log.error(f"CSV not found at {EVENTS_CSV}")
        return 1

    admin = wait_for_kafka()
    ensure_topic(admin)

    failures = {"count": 0}

    def delivery_report(err, msg):
        if err is not None:
            failures["count"] += 1
            log.error(f"delivery failed for key={msg.key()}: {err}")

    producer = Producer({
        "bootstrap.servers":  BOOTSTRAP,
        "client.id":          "user-events-producer",
        "linger.ms":          5,
        "batch.size":         32 * 1024,
        "compression.type":   "snappy",
        "enable.idempotence": True,
        "acks":               "all",
    })

    rows = read_events_sorted(EVENTS_CSV)
    delay_s = DELAY_MS / 1000.0
    sent = 0

    for row in rows:
        payload = {
            "event_time":     row["event_time"],
            "user_id":        int(row["user_id"]),
            "session_id":     row["session_id"],
            "event_type":     row["event_type"],
            "channel":        row["channel"],
            "premium_amount": int(row["premium_amount"] or 0),
        }
        producer.produce(
            topic=TOPIC,
            key=str(payload["user_id"]),
            value=json.dumps(payload).encode("utf-8"),
            on_delivery=delivery_report,
        )
        sent += 1
        producer.poll(0)
        if sent % LOG_EVERY == 0:
            log.info(f"  produced {sent:,} events")
        if delay_s > 0:
            time.sleep(delay_s)

    log.info(f"flushing producer ({sent:,} sent)")
    producer.flush(60)

    if failures["count"]:
        log.error(f"{failures['count']:,} deliveries failed; exiting non-zero")
        return 1

    log.info(f"=== producer done: {sent:,} events produced ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
