from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path

import requests

logging.basicConfig(
    format="%(asctime)s %(levelname)s registrar | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

CONNECT_URL    = os.environ.get("CONNECT_URL", "http://kafka-connect:8083")
CONNECTORS_DIR = Path(os.environ.get("CONNECTORS_DIR", "/connectors"))


def wait_for_connect(timeout_s: int = 180) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            r = requests.get(f"{CONNECT_URL}/connectors", timeout=3)
            if r.status_code == 200:
                log.info(f"Kafka Connect ready at {CONNECT_URL}")
                return
        except requests.RequestException as exc:
            log.info(f"  not ready yet: {exc}")
        time.sleep(3)
    raise RuntimeError(f"Kafka Connect at {CONNECT_URL} not reachable within {timeout_s}s")


def register_all() -> None:
    cfgs = sorted(CONNECTORS_DIR.glob("*.json"))
    if not cfgs:
        log.warning(f"no connector configs found in {CONNECTORS_DIR}")
        return

    for cfg_path in cfgs:
        with cfg_path.open() as f:
            spec = json.load(f)
        name = spec["name"]
        config = spec["config"]
        log.info(f"Registering connector: {name}")
        r = requests.put(
            f"{CONNECT_URL}/connectors/{name}/config",
            json=config,
            timeout=15,
        )
        if r.status_code in (200, 201):
            log.info(f"  OK {name} (HTTP {r.status_code})")
        else:
            log.error(f"  FAILED {name} (HTTP {r.status_code}): {r.text[:500]}")
            sys.exit(1)


def show_status() -> None:
    log.info("waiting 5s for status to stabilise")
    time.sleep(5)
    names = requests.get(f"{CONNECT_URL}/connectors", timeout=5).json()
    for c in names:
        s = requests.get(f"{CONNECT_URL}/connectors/{c}/status", timeout=5).json()
        state = s.get("connector", {}).get("state", "?")
        tasks = ",".join(t.get("state", "?") for t in s.get("tasks", []))
        log.info(f"  {c}: connector={state} tasks=[{tasks}]")


def main() -> int:
    log.info(f"=== connector-registrar starting (target {CONNECT_URL}) ===")
    wait_for_connect()
    register_all()
    show_status()
    log.info("=== connector-registrar done ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
