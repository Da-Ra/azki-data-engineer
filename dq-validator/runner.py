from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pymysql
import yaml
from clickhouse_driver import Client as CHClient
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

MYSQL_CFG = dict(
    host=os.environ.get("MYSQL_HOST", "mysql"),
    port=int(os.environ.get("MYSQL_PORT", 3306)),
    user=os.environ.get("MYSQL_USER", "azki_reader"),
    password=os.environ.get("MYSQL_PASSWORD", "azki_reader_pw"),
    database=os.environ.get("MYSQL_DATABASE", "azki"),
    charset="utf8mb4",
)
CH_CFG = dict(
    host=os.environ.get("CH_HOST", "clickhouse"),
    port=int(os.environ.get("CH_PORT", 9000)),
    user=os.environ.get("CH_USER", "default"),
    password=os.environ.get("CH_PASSWORD", ""),
    database=os.environ.get("CH_DATABASE", "azki"),
)
KAFKA_BOOTSTRAP    = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CHECKS_FILE        = Path(os.environ.get("CHECKS_FILE", "/app/checks.yaml"))
DEFAULT_INTERVAL   = int(os.environ.get("DEFAULT_INTERVAL_SECONDS", 60))
INITIAL_WAIT_S     = int(os.environ.get("INITIAL_WAIT_SECONDS", 30))

logging.basicConfig(
    format="%(asctime)s %(levelname)s dq | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)


@dataclass
class CheckResult:
    name: str
    type: str
    severity: str
    status: str          # pass | warn | fail | error
    value: float | None
    expected: str
    message: str
    duration_ms: float
    timestamp: str


class Clients:
    def __init__(self) -> None:
        self._ch: CHClient | None = None
        self._kafka_admin: AdminClient | None = None

    def mysql(self) -> pymysql.Connection:
        return pymysql.connect(**MYSQL_CFG)

    def ch(self) -> CHClient:
        if self._ch is None:
            self._ch = CHClient(**CH_CFG)
        return self._ch

    def kafka_admin(self) -> AdminClient:
        if self._kafka_admin is None:
            self._kafka_admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        return self._kafka_admin


def mysql_query_scalar(conn: pymysql.Connection, query: str) -> float:
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
    return 0.0 if row is None or row[0] is None else float(row[0])


def ch_query_scalar(client: CHClient, query: str) -> float:
    rows = client.execute(query)
    if not rows or rows[0][0] is None:
        return 0.0
    return float(rows[0][0])


def mysql_query_string(conn: pymysql.Connection, query: str) -> str:
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return "\n".join(" ".join(str(c) for c in row) for row in rows)


def ch_query_string(client: CHClient, query: str) -> str:
    rows = client.execute(query)
    return "\n".join(" ".join(str(c) for c in row) for row in rows)


def execute_sql_equality(check: dict, clients: Clients) -> tuple[str, float, str]:
    tolerance = check.get("tolerance", 0)
    if "sources" in check:
        srcs = check["sources"]
        a = mysql_query_scalar(clients.mysql(), srcs["mysql"]) if "mysql" in srcs else None
        b = ch_query_scalar(clients.ch(), srcs["clickhouse"])  if "clickhouse" in srcs else None
        if a is None or b is None:
            raise ValueError("sql_equality with `sources` needs both `mysql` and `clickhouse`")
    else:
        src = check.get("source", "clickhouse")
        if src == "mysql":
            a = mysql_query_scalar(clients.mysql(), check["query_a"])
            b = mysql_query_scalar(clients.mysql(), check["query_b"])
        else:
            a = ch_query_scalar(clients.ch(), check["query_a"])
            b = ch_query_scalar(clients.ch(), check["query_b"])

    diff = abs(a - b)
    msg = f"a={a:.0f}, b={b:.0f}, diff={diff:.0f}"
    if diff <= tolerance:
        return "pass", diff, msg
    return "fail", diff, f"{msg} > tolerance={tolerance}"


def execute_sql_threshold(check: dict, clients: Clients) -> tuple[str, float, str]:
    src = check.get("source", "clickhouse")
    val = (mysql_query_scalar(clients.mysql(), check["query"])
           if src == "mysql"
           else ch_query_scalar(clients.ch(), check["query"]))
    lo, hi = check.get("min"), check.get("max")
    msg = f"value={val:.0f}, min={lo}, max={hi}"
    if lo is not None and val < lo:
        return "fail", val, msg
    if hi is not None and val > hi:
        return "fail", val, msg
    return "pass", val, msg


def execute_sql_nonzero(check: dict, clients: Clients) -> tuple[str, float, str]:
    src = check.get("source", "clickhouse")
    val = (mysql_query_scalar(clients.mysql(), check["query"])
           if src == "mysql"
           else ch_query_scalar(clients.ch(), check["query"]))
    if val > 0:
        return "pass", val, f"value={val:.0f}"
    return "fail", val, f"value={val:.0f} is not > 0"


def execute_schema_sha(check: dict, clients: Clients) -> tuple[str, float | None, str]:
    src = check.get("source", "clickhouse")
    ddl = (mysql_query_string(clients.mysql(), check["query"])
           if src == "mysql"
           else ch_query_string(clients.ch(), check["query"]))
    actual = hashlib.sha256(ddl.encode()).hexdigest()
    expected = check.get("expected_sha256", "")

    if not expected or expected == "BOOTSTRAP":
        return "warn", None, f"sha={actual[:16]}... (no expected_sha256 set)"
    if actual == expected:
        return "pass", None, f"sha matches ({actual[:16]}...)"
    return "fail", None, f"expected={expected[:16]}... actual={actual[:16]}..."


def execute_kafka_lag(check: dict, clients: Clients) -> tuple[str, float, str]:
    topic = check["topic"]
    consumer_group = check.get("consumer_group")
    max_lag = check.get("max_lag")

    admin = clients.kafka_admin()
    md = admin.list_topics(topic, timeout=5)
    if topic not in md.topics or md.topics[topic].error is not None:
        return "warn", 0.0, f"topic {topic} not present"
    partitions = list(md.topics[topic].partitions.keys())

    consumer = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "dq-probe"})
    high_offsets: dict[int, int] = {}
    for pid in partitions:
        try:
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, pid), timeout=5)
            high_offsets[pid] = high
        except Exception as exc:
            log.warning(f"watermark fetch failed for {topic}/{pid}: {exc}")
    consumer.close()

    total_high = sum(high_offsets.values())

    if not consumer_group:
        return "pass", float(total_high), f"topic {topic} total offsets = {total_high}"

    # Try to fetch consumer-group committed offsets and compute lag
    try:
        from confluent_kafka import ConsumerGroupTopicPartitions
        request = ConsumerGroupTopicPartitions(
            consumer_group,
            [TopicPartition(topic, pid) for pid in partitions],
        )
        futures = admin.list_consumer_group_offsets([request])
        committed: dict[int, int] = {}
        for _, fut in futures.items():
            res = fut.result(timeout=5)
            for tp in res.topic_partitions:
                committed[tp.partition] = max(0, tp.offset)
        total_lag = sum(high_offsets[p] - committed.get(p, 0) for p in partitions)
        msg = f"lag={total_lag}, high={total_high}, committed={sum(committed.values())}"
        if max_lag is not None and total_lag > max_lag:
            return "fail", float(total_lag), msg
        return "pass", float(total_lag), msg
    except Exception as exc:
        log.warning(f"consumer group offsets fetch failed for {consumer_group}: {exc}")
        return "warn", float(total_high), f"could not query consumer_group; topic total={total_high}"


def execute_kafka_message_shape(check: dict, clients: Clients) -> tuple[str, float, str]:
    topic = check["topic"]
    sample_size = int(check.get("sample_size", 20))
    required = set(check.get("required_fields", []))
    forbid_extra = bool(check.get("forbidden_extra_fields", False))

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": f"dq-shape-{topic}-{int(time.time())}",
        "auto.offset.reset": "latest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([topic])

    messages = []
    deadline = time.time() + 8
    while len(messages) < sample_size and time.time() < deadline:
        msg = consumer.poll(0.5)
        if msg is None or msg.error():
            continue
        messages.append(msg.value())
    consumer.close()

    if not messages:
        return "warn", 0.0, f"no recent messages on {topic}"

    failures: list[str] = []
    for m in messages:
        try:
            payload = json.loads(m)
            fields = set(payload.keys())
            missing = required - fields
            extra = (fields - required) if forbid_extra else set()
            if missing or extra:
                failures.append(f"missing={sorted(missing)} extra={sorted(extra)}")
        except Exception as exc:
            failures.append(f"json-parse: {exc}")

    n_bad = len(failures)
    if n_bad == 0:
        return "pass", 0.0, f"{len(messages)} messages sampled, all valid"
    return "fail", float(n_bad), f"{n_bad}/{len(messages)} invalid: {failures[:3]}"


EXECUTORS = {
    "sql_equality":         execute_sql_equality,
    "sql_threshold":        execute_sql_threshold,
    "sql_nonzero":          execute_sql_nonzero,
    "schema_sha":           execute_schema_sha,
    "kafka_lag":            execute_kafka_lag,
    "kafka_message_shape":  execute_kafka_message_shape,
}


def expected_str(check: dict) -> str:
    if check["type"] == "schema_sha":
        return f"sha256={check.get('expected_sha256', '')[:16]}..."
    if check["type"] == "sql_threshold":
        return f"min={check.get('min')}, max={check.get('max')}"
    if check["type"] == "sql_equality":
        return f"tolerance={check.get('tolerance', 0)}"
    if check["type"] == "kafka_lag":
        return f"max_lag={check.get('max_lag', 'n/a')}"
    if check["type"] == "kafka_message_shape":
        return f"required={check.get('required_fields', [])}"
    return ""


def run_check(check: dict, clients: Clients) -> CheckResult:
    start = time.time()
    try:
        executor = EXECUTORS[check["type"]]
        status, value, message = executor(check, clients)
    except Exception as exc:
        log.error(f"check {check['name']} errored: {exc}")
        status, value, message = "error", None, str(exc)
    duration_ms = (time.time() - start) * 1000
    return CheckResult(
        name=check["name"],
        type=check["type"],
        severity=check.get("severity", "info"),
        status=status,
        value=value,
        expected=expected_str(check),
        message=message,
        duration_ms=duration_ms,
        timestamp=datetime.utcnow().isoformat(),
    )


def emit_stdout(r: CheckResult) -> None:
    print(json.dumps(asdict(r)))


def emit_clickhouse(r: CheckResult, ch: CHClient) -> None:
    try:
        ch.execute(
            """INSERT INTO azki.dq_results
               (check_name, check_type, severity, status, value, expected, message, duration_ms, ts)
               VALUES""",
            [(r.name, r.type, r.severity, r.status, r.value,
              r.expected, r.message, r.duration_ms, datetime.utcnow())],
        )
    except Exception as exc:
        log.warning(f"sink:ch failed for {r.name}: {exc}")


def load_checks() -> list[dict]:
    with CHECKS_FILE.open() as f:
        return yaml.safe_load(f)["checks"]


def bootstrap_schemas(clients: Clients) -> None:
    log.info("bootstrap mode: computing expected_sha256 values for schema_sha checks")
    print()
    for check in load_checks():
        if check["type"] != "schema_sha":
            continue
        src = check.get("source", "clickhouse")
        ddl = (mysql_query_string(clients.mysql(), check["query"])
               if src == "mysql"
               else ch_query_string(clients.ch(), check["query"]))
        sha = hashlib.sha256(ddl.encode()).hexdigest()
        print(f"# {check['name']}")
        print(f"# source={src}, query={check['query']}")
        print(f"  expected_sha256: \"{sha}\"")
        print()
    log.info("Copy each expected_sha256 line into the matching check in checks.yaml")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-schemas", action="store_true",
                        help="print expected_sha256 for each schema_sha check, then exit")
    args = parser.parse_args()

    clients = Clients()

    if args.bootstrap_schemas:
        bootstrap_schemas(clients)
        return 0

    log.info(f"=== dq-validator starting (checks={CHECKS_FILE}) ===")
    if INITIAL_WAIT_S:
        log.info(f"initial wait {INITIAL_WAIT_S}s for upstream to settle")
        time.sleep(INITIAL_WAIT_S)

    checks = load_checks()
    log.info(f"loaded {len(checks)} checks")

    next_due: dict[str, float] = {c["name"]: 0.0 for c in checks}

    while True:
        now = time.time()
        for check in checks:
            if next_due[check["name"]] > now:
                continue
            r = run_check(check, clients)
            emit_stdout(r)
            emit_clickhouse(r, clients.ch())
            log.info(f"[{r.severity:>8}] {r.name}: {r.status} -- {r.message[:90]}")
            interval = check.get("interval_seconds", DEFAULT_INTERVAL)
            next_due[check["name"]] = now + interval

        sleep_for = max(1.0, min(next_due.values()) - time.time())
        time.sleep(min(sleep_for, 15.0))


if __name__ == "__main__":
    sys.exit(main())
