#!/usr/bin/env python3
"""Insert synthetic rows into public.orders (runs inside Compose; Postgres must be reachable)."""
import json
import os
import sys

import psycopg2
from faker import Faker


def main() -> None:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    user = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "postgres")
    db = os.environ.get("POSTGRES_DB", "demo")
    n = max(0, int(os.environ.get("FAKE_ROWS", "25")))
    faker_seed_raw = os.environ.get("FAKER_SEED", "").strip()

    faker = Faker()
    if faker_seed_raw.isdigit():
        faker.seed_instance(int(faker_seed_raw))

    if n == 0:
        print(json.dumps({"inserted": 0, "message": "FAKE_ROWS=0; nothing to do"}))
        return

    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=db,
    )
    inserted = 0
    try:
        with conn, conn.cursor() as cur:
            for _ in range(n):
                name = f"{faker.word()}-{faker.uuid4()[:8]}"
                qty = faker.random_int(min=1, max=49)
                cur.execute(
                    "INSERT INTO public.orders (product_name, qty) VALUES (%s, %s)",
                    (name, qty),
                )
                inserted += 1
    finally:
        conn.close()

    print(json.dumps({"inserted": inserted, "postgres": f"{host}:{port}/{db}"}))


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
