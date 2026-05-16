#!/usr/bin/env python3
import json
import sys
import urllib.parse
import urllib.request

prom_url = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:19090"


def query(expr: str) -> dict:
    url = f"{prom_url}/api/v1/query?query={urllib.parse.quote(expr)}"
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.load(resp)


def ok(result: dict) -> bool:
    series = result.get("data", {}).get("result", [])
    return bool(series) and float(series[0]["value"][1]) > 0


def main() -> int:
    print(f"Checking Prometheus at {prom_url} ...")
    prod = query('count(kafka_producer_app{env="lab"})')
    cons = query('count(kafka_consumer_app{env="lab"})')
    prod_ok = ok(prod)
    cons_ok = ok(cons)
    print(
        "Producer metrics (kafka_producer_app):",
        "PASS" if prod_ok else "FAIL — run make perf-producer",
    )
    print(
        "Consumer metrics (kafka_consumer_app):",
        "PASS" if cons_ok else "FAIL — run make perf-consumer",
    )
    print(
        "Grafana: Kafka → Kafka Perf (lab) (no variables) or "
        "Kafka Producer / Kafka Consumer (defaults: env=lab)"
    )
    return 0 if (prod_ok or cons_ok) else 1


if __name__ == "__main__":
    sys.exit(main())
