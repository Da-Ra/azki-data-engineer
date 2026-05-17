from __future__ import annotations

import csv
import logging
import os
import sys
import time
from datetime import date
from pathlib import Path

import pymysql

logging.basicConfig(
    format="%(asctime)s %(levelname)s users-loader | %(message)s",
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
USERS_CSV = Path(os.environ.get("USERS_CSV", "/data/users.csv"))
CHUNK_SIZE = int(os.environ.get("CHUNK_SIZE", 1000))

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
    user_id     INT          NOT NULL,
    signup_date DATE         NOT NULL,
    city        VARCHAR(64)  NOT NULL,
    device_type VARCHAR(32)  NOT NULL,
    PRIMARY KEY (user_id),
    INDEX idx_city (city),
    INDEX idx_signup_date (signup_date)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci
"""

READER_USER_SQL = [
    "CREATE USER IF NOT EXISTS 'azki_reader'@'%' IDENTIFIED BY 'azki_reader_pw'",
    "GRANT SELECT ON azki.* TO 'azki_reader'@'%'",
    "FLUSH PRIVILEGES",
]


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


def apply_schema(conn: pymysql.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
        for stmt in READER_USER_SQL:
            cur.execute(stmt)
    conn.commit()
    log.info("schema + reader user applied")


def read_csv(path: Path) -> list[tuple]:
    rows: list[tuple] = []
    with path.open() as f:
        for row in csv.DictReader(f):
            rows.append((
                int(row["user_id"]),
                date.fromisoformat(row["signup_date"]),
                row["city"],
                row["device_type"],
            ))
    log.info(f"parsed {len(rows):,} rows from {path}")
    return rows


def load(conn: pymysql.Connection, rows: list[tuple]) -> None:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE users")
        sql = "INSERT INTO users (user_id, signup_date, city, device_type) VALUES (%s, %s, %s, %s)"
        for i in range(0, len(rows), CHUNK_SIZE):
            cur.executemany(sql, rows[i : i + CHUNK_SIZE])
    conn.commit()
    log.info(f"inserted {len(rows):,} users")


def main() -> int:
    log.info("=== users-loader starting ===")
    if not USERS_CSV.exists():
        log.error(f"CSV not found at {USERS_CSV}")
        return 1

    conn = connect_with_retry()
    try:
        apply_schema(conn)
        rows = read_csv(USERS_CSV)
        load(conn, rows)
    finally:
        conn.close()

    log.info("=== users-loader done ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
