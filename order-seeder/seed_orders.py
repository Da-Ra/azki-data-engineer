from __future__ import annotations

import csv
import logging
import os
import random
import sys
import time
from pathlib import Path

import pymysql

logging.basicConfig(
    format="%(asctime)s %(levelname)s order-seeder | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

MYSQL_CFG = dict(
    host=os.environ.get("MYSQL_HOST", "mysql"),
    port=int(os.environ.get("MYSQL_PORT", 3306)),
    user=os.environ.get("MYSQL_USER", "root"),
    password=os.environ.get("MYSQL_PASSWORD", "rootpw"),
    database=os.environ.get("MYSQL_DATABASE", "azki"),
    charset="utf8mb4",
    autocommit=False,
)
EVENTS_CSV = Path(os.environ.get("EVENTS_CSV", "/data/user_events.csv"))

random.seed(42)

INSURERS = [
    "Iran Insurance", "Asia Insurance", "Alborz Insurance", "Dana Insurance",
    "Parsian Insurance", "Saman Insurance", "Pasargad Insurance",
]
PRODUCT_WEIGHTS = [("third", 0.40), ("body", 0.25), ("medical", 0.20), ("fire", 0.15)]
PAYMENT_METHODS = ["card", "wallet", "installments"]
PAYMENT_STATUSES_WEIGHTED = [("paid", 0.95), ("pending", 0.04), ("failed", 0.01)]

SCHEMAS_DEFENSIVE = [
    """CREATE TABLE IF NOT EXISTS third_order (
        order_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL, session_id VARCHAR(64) NOT NULL,
        event_time DATETIME NOT NULL,
        vehicle_type VARCHAR(20) NOT NULL, vehicle_usage VARCHAR(20) NOT NULL,
        no_claim_discount_pct TINYINT NOT NULL, coverage_amount BIGINT NOT NULL,
        insurer VARCHAR(64) NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_event (user_id, session_id, event_time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS body_order (
        order_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL, session_id VARCHAR(64) NOT NULL,
        event_time DATETIME NOT NULL,
        vehicle_make VARCHAR(32) NOT NULL, vehicle_model VARCHAR(32) NOT NULL,
        vehicle_year SMALLINT NOT NULL, coverage_amount BIGINT NOT NULL,
        has_natural_disaster_cover TINYINT NOT NULL,
        insurer VARCHAR(64) NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_event (user_id, session_id, event_time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS medical_order (
        order_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL, session_id VARCHAR(64) NOT NULL,
        event_time DATETIME NOT NULL,
        plan_tier VARCHAR(16) NOT NULL, num_insured TINYINT NOT NULL,
        has_dental TINYINT NOT NULL, has_vision TINYINT NOT NULL,
        annual_limit BIGINT NOT NULL,
        insurer VARCHAR(64) NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_event (user_id, session_id, event_time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS fire_order (
        order_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL, session_id VARCHAR(64) NOT NULL,
        event_time DATETIME NOT NULL,
        property_type VARCHAR(20) NOT NULL, property_area_m2 INT NOT NULL,
        building_value BIGINT NOT NULL, contents_value BIGINT NOT NULL,
        insurer VARCHAR(64) NOT NULL,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_event (user_id, session_id, event_time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS financial_order (
        financial_id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        order_id BIGINT NOT NULL, product_type VARCHAR(16) NOT NULL,
        user_id INT NOT NULL, session_id VARCHAR(64) NOT NULL,
        event_time DATETIME NOT NULL,
        premium_amount BIGINT NOT NULL,
        discount_amount BIGINT NOT NULL DEFAULT 0,
        tax_amount BIGINT NOT NULL DEFAULT 0,
        final_amount BIGINT NOT NULL,
        payment_method VARCHAR(32) NOT NULL,
        payment_status VARCHAR(16) NOT NULL DEFAULT 'paid',
        installments_count TINYINT NOT NULL DEFAULT 1,
        created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_order_product (order_id, product_type),
        INDEX idx_event (user_id, session_id, event_time),
        INDEX idx_event_time (event_time)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    "GRANT SELECT ON azki.* TO 'azki_reader'@'%'",
    "FLUSH PRIVILEGES",
]


def _weighted_choice(choices: list[tuple[str, float]]) -> str:
    r = random.random()
    cum = 0.0
    for label, w in choices:
        cum += w
        if r <= cum:
            return label
    return choices[-1][0]


def connect_with_retry(timeout_s: int = 60) -> pymysql.Connection:
    deadline = time.time() + timeout_s
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            conn = pymysql.connect(**MYSQL_CFG)
            log.info(f"connected to mysql at {MYSQL_CFG['host']}:{MYSQL_CFG['port']}")
            return conn
        except Exception as exc:
            last_err = exc
            time.sleep(2)
    raise RuntimeError(f"mysql unreachable: {last_err}")


def apply_schemas(conn: pymysql.Connection) -> None:
    with conn.cursor() as cur:
        for stmt in SCHEMAS_DEFENSIVE:
            cur.execute(stmt)
    conn.commit()
    log.info("schemas + grants applied")


def read_purchase_events(path: Path) -> list[dict]:
    events: list[dict] = []
    with path.open() as f:
        for row in csv.DictReader(f):
            if row["event_type"] != "purchase":
                continue
            events.append({
                "event_time": row["event_time"],
                "user_id": int(row["user_id"]),
                "session_id": row["session_id"],
                "premium_amount": int(row["premium_amount"]),
            })
    log.info(f"read {len(events):,} purchase events from {path}")
    return events


def generate_third(p: dict) -> dict:
    return {
        "user_id": p["user_id"], "session_id": p["session_id"], "event_time": p["event_time"],
        "vehicle_type":          random.choice(["sedan", "suv", "motorcycle", "truck", "van"]),
        "vehicle_usage":         random.choice(["personal", "taxi", "cargo"]),
        "no_claim_discount_pct": random.choice([0, 5, 10, 15, 20, 30, 50, 70]),
        "coverage_amount":       random.choice([500_000_000, 1_000_000_000, 2_000_000_000]),
        "insurer":               random.choice(INSURERS),
    }


def generate_body(p: dict) -> dict:
    makes_models = {
        "Iran Khodro": ["Samand", "Dena", "Tara", "Peugeot 206", "Peugeot Pars"],
        "Saipa":       ["Pride", "Tiba", "Quick", "Shahin"],
        "Toyota":      ["Corolla", "Camry", "RAV4", "Land Cruiser"],
        "Hyundai":     ["Tucson", "Sonata", "Elantra"],
        "Kia":         ["Sportage", "Cerato", "Optima"],
    }
    make = random.choice(list(makes_models.keys()))
    return {
        "user_id": p["user_id"], "session_id": p["session_id"], "event_time": p["event_time"],
        "vehicle_make":               make,
        "vehicle_model":              random.choice(makes_models[make]),
        "vehicle_year":               random.randint(2005, 2024),
        "coverage_amount":            random.choice([800_000_000, 1_500_000_000, 3_000_000_000]),
        "has_natural_disaster_cover": random.choice([0, 1]),
        "insurer":                    random.choice(INSURERS),
    }


def generate_medical(p: dict) -> dict:
    tier = random.choice(["bronze", "silver", "gold", "platinum"])
    annual_limits = {"bronze": 200_000_000, "silver": 500_000_000, "gold": 1_500_000_000, "platinum": 5_000_000_000}
    return {
        "user_id": p["user_id"], "session_id": p["session_id"], "event_time": p["event_time"],
        "plan_tier":    tier,
        "num_insured":  random.randint(1, 6),
        "has_dental":   random.choice([0, 1]),
        "has_vision":   random.choice([0, 1]),
        "annual_limit": annual_limits[tier],
        "insurer":      random.choice(INSURERS),
    }


def generate_fire(p: dict) -> dict:
    return {
        "user_id": p["user_id"], "session_id": p["session_id"], "event_time": p["event_time"],
        "property_type":    random.choice(["apartment", "villa", "commercial", "warehouse"]),
        "property_area_m2": random.randint(45, 600),
        "building_value":   random.randint(2_000_000_000, 30_000_000_000),
        "contents_value":   random.randint(200_000_000, 3_000_000_000),
        "insurer":          random.choice(INSURERS),
    }


GENERATORS = {
    "third":   generate_third,
    "body":    generate_body,
    "medical": generate_medical,
    "fire":    generate_fire,
}


def generate_financial(order_id: int, product_type: str, p: dict) -> dict:
    discount = int(p["premium_amount"] * random.uniform(0.0, 0.15))
    tax = int(p["premium_amount"] * 0.09)  # Iranian VAT ~9%
    final = p["premium_amount"] + tax - discount
    method = random.choice(PAYMENT_METHODS)
    return {
        "order_id":           order_id,
        "product_type":       product_type,
        "user_id":            p["user_id"],
        "session_id":         p["session_id"],
        "event_time":         p["event_time"],
        "premium_amount":     p["premium_amount"],
        "discount_amount":    discount,
        "tax_amount":         tax,
        "final_amount":       final,
        "payment_method":     method,
        "payment_status":     _weighted_choice(PAYMENT_STATUSES_WEIGHTED),
        "installments_count": random.choice([1, 3, 6, 12]) if method == "installments" else 1,
    }


def seed(conn: pymysql.Connection, purchases: list[dict]) -> None:
    buckets: dict[str, list[dict]] = {p: [] for p, _ in PRODUCT_WEIGHTS}
    assignments: list[tuple[str, dict]] = []

    for p in purchases:
        product = _weighted_choice(PRODUCT_WEIGHTS)
        assignments.append((product, p))
        buckets[product].append(GENERATORS[product](p))

    with conn.cursor() as cur:
        for tbl in ("financial_order", "third_order", "body_order", "medical_order", "fire_order"):
            cur.execute(f"TRUNCATE TABLE {tbl}")

        inserted: dict[str, list[dict]] = {p: [] for p, _ in PRODUCT_WEIGHTS}
        for product, rows in buckets.items():
            if not rows:
                continue
            cols = list(rows[0].keys())
            placeholders = ", ".join(["%s"] * len(cols))
            sql = f"INSERT INTO {product}_order ({', '.join(cols)}) VALUES ({placeholders})"
            for row in rows:
                cur.execute(sql, tuple(row[c] for c in cols))
                row["order_id"] = cur.lastrowid
                inserted[product].append(row)

        # Walk assignments in original order; per-product cursor matches
        # each (product, purchase) pair to its just-inserted order row.
        product_cursors = {p: 0 for p, _ in PRODUCT_WEIGHTS}
        financials: list[dict] = []
        for product, p in assignments:
            order_row = inserted[product][product_cursors[product]]
            product_cursors[product] += 1
            financials.append(generate_financial(order_row["order_id"], product, p))

        if financials:
            cols = list(financials[0].keys())
            placeholders = ", ".join(["%s"] * len(cols))
            sql = f"INSERT INTO financial_order ({', '.join(cols)}) VALUES ({placeholders})"
            cur.executemany(sql, [tuple(r[c] for c in cols) for r in financials])

    conn.commit()
    for product, rows in inserted.items():
        log.info(f"  mysql.{product}_order ← {len(rows):,} rows")
    log.info(f"  mysql.financial_order ← {len(financials):,} rows")


def main() -> int:
    log.info("=== order-seeder starting ===")
    if not EVENTS_CSV.exists():
        log.error(f"CSV not found at {EVENTS_CSV}")
        return 1

    conn = connect_with_retry()
    try:
        apply_schemas(conn)
        purchases = read_purchase_events(EVENTS_CSV)
        seed(conn, purchases)
    finally:
        conn.close()

    log.info("=== order-seeder done ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
