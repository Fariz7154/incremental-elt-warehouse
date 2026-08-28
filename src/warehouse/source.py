"""A simulated operating system, generating changes over time.

Incremental loading cannot be demonstrated against a static file: the whole
point is what happens on the second and third run. So the source produces orders
and customers across several days, mutates customer attributes partway through
(so SCD Type 2 has something to version), restates a handful of order lines
after they were first loaded, and back-dates one customer's creation so the
late-arriving-dimension path is exercised.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

import duckdb

SEGMENTS = ["Retail", "SME", "Corporate", "Private"]
COUNTRIES = ["AE", "SA", "IN", "GB", "US"]
RATINGS = ["AAA", "AA", "A", "BBB", "BB"]
CATEGORIES = ["Electronics", "Apparel", "Home", "Grocery", "Sports"]

DDL = """
CREATE OR REPLACE TABLE src_customer (
    customer_id   VARCHAR PRIMARY KEY,
    customer_name VARCHAR,
    segment       VARCHAR,
    country       VARCHAR,
    credit_rating VARCHAR,
    updated_at    TIMESTAMP NOT NULL
);

CREATE OR REPLACE TABLE src_product (
    product_id    VARCHAR PRIMARY KEY,
    product_name  VARCHAR,
    category      VARCHAR,
    unit_cost     DECIMAL(18,4),
    updated_at    TIMESTAMP NOT NULL
);

CREATE OR REPLACE TABLE src_order_line (
    order_line_id VARCHAR PRIMARY KEY,
    order_id      VARCHAR NOT NULL,
    order_ts      TIMESTAMP NOT NULL,
    customer_id   VARCHAR NOT NULL,
    product_id    VARCHAR NOT NULL,
    quantity      INTEGER NOT NULL,
    unit_price    DECIMAL(18,4) NOT NULL,
    discount_pct  DECIMAL(9,4) NOT NULL,
    updated_at    TIMESTAMP NOT NULL
);
"""

DAY_ONE = datetime(2026, 2, 1, 0, 0, 0)


def build(con: duckdb.DuckDBPyConnection, customers: int = 800, products: int = 200,
          orders: int = 12_000, days: int = 5, seed: int = 23) -> dict:
    rnd = random.Random(seed)
    con.execute(DDL)

    customer_rows = []
    for i in range(1, customers + 1):
        created = DAY_ONE + timedelta(hours=rnd.uniform(0, 24))
        customer_rows.append(
            (f"CUST-{i:05d}", f"Customer {i:05d}", rnd.choice(SEGMENTS),
             rnd.choice(COUNTRIES), rnd.choice(RATINGS), created)
        )
    con.executemany("INSERT INTO src_customer VALUES (?,?,?,?,?,?)", customer_rows)

    product_rows = [
        (f"PROD-{i:05d}", f"Product {i:05d}", rnd.choice(CATEGORIES),
         round(rnd.uniform(2, 900), 4), DAY_ONE)
        for i in range(1, products + 1)
    ]
    con.executemany("INSERT INTO src_product VALUES (?,?,?,?,?)", product_rows)

    order_rows = []
    for i in range(1, orders + 1):
        ts = DAY_ONE + timedelta(seconds=rnd.uniform(0, days * 86400))
        order_rows.append(
            (f"OL-{i:07d}", f"ORD-{(i // 3) + 1:07d}", ts,
             f"CUST-{rnd.randint(1, customers):05d}", f"PROD-{rnd.randint(1, products):05d}",
             rnd.randint(1, 12), round(rnd.uniform(5, 1200), 4),
             round(rnd.choice([0, 0, 0, 0.05, 0.1, 0.15, 0.2]), 4), ts)
        )
    con.executemany("INSERT INTO src_order_line VALUES (?,?,?,?,?,?,?,?,?)", order_rows)

    return {"customers": customers, "products": products, "order_lines": orders}


def apply_day_two_changes(con: duckdb.DuckDBPyConnection, seed: int = 23) -> dict:
    """Mutate the source the way a real operating system does between loads."""
    rnd = random.Random(seed + 1)
    at = DAY_ONE + timedelta(days=6)          # after everything the first load saw
    back_dated = DAY_ONE + timedelta(days=4)  # inside the lookback window

    # Customers whose segment or rating was reassessed -> new Type 2 versions.
    reassessed = [f"CUST-{i:05d}" for i in rnd.sample(range(1, 801), 60)]
    for cid in reassessed:
        con.execute(
            "UPDATE src_customer SET segment = ?, credit_rating = ?, updated_at = ? WHERE customer_id = ?",
            [rnd.choice(SEGMENTS), rnd.choice(RATINGS), at, cid],
        )

    # Products renamed -> Type 1, overwritten with no history.
    renamed = [f"PROD-{i:05d}" for i in rnd.sample(range(1, 201), 15)]
    for pid in renamed:
        con.execute(
            "UPDATE src_product SET product_name = product_name || ' (v2)', updated_at = ? WHERE product_id = ?",
            [at, pid],
        )

    # Order lines restated after the fact -- quantity corrections raised by ops.
    restated = [f"OL-{i:07d}" for i in rnd.sample(range(1, 12_001), 120)]
    for olid in restated:
        con.execute(
            "UPDATE src_order_line SET quantity = quantity + 1, updated_at = ? WHERE order_line_id = ?",
            [at, olid],
        )

    # A customer created in the same window as its first orders. Because
    # dimensions load before facts, this resolves to a real surrogate key rather
    # than the Unknown member -- which is the point of that ordering.
    late_id = "CUST-99999"
    con.execute(
        "INSERT INTO src_customer VALUES (?,?,?,?,?,?)",
        [late_id, "Customer 99999", "Corporate", "AE", "A", at + timedelta(hours=6)],
    )
    con.execute(
        "INSERT INTO src_order_line VALUES (?,?,?,?,?,?,?,?,?)",
        ["OL-9999999", "ORD-9999999", at, late_id, "PROD-00001", 3, 100.0, 0.0, at],
    )

    # A correction stamped with the original transaction time rather than the
    # time the correction was made. It sits *behind* the watermark, and is only
    # ever picked up because the extract re-reads a trailing lookback window.
    con.execute(
        "UPDATE src_order_line SET quantity = quantity + 5, updated_at = ? WHERE order_line_id = ?",
        [back_dated, "OL-0000001"],
    )

    return {
        "customers_reassessed": len(reassessed),
        "back_dated_correction": 1,
        "products_renamed": len(renamed),
        "order_lines_restated": len(restated),
        "late_arriving_customer": 1,
        "new_order_lines": 1,
    }
