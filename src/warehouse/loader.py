"""The load orchestration.

Order is not arbitrary: dimensions before facts, because a fact needs the
surrogate key of the dimension version that was live when the event happened.
Loading facts first means either a null key or a join to whatever version
happens to be current at load time -- which is how a report quietly restates
itself every month.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict

import duckdb

from . import scd, watermark

CUSTOMER_ATTRIBUTES = ["customer_name", "segment", "country", "credit_rating"]
PRODUCT_ATTRIBUTES = ["product_name", "category", "unit_cost"]

UNKNOWN_SK = -1  # the inferred member every unmatched fact points at


@dataclass
class LoadReport:
    high_water: datetime
    dim_customer: scd.LoadStats = field(default_factory=scd.LoadStats)
    dim_product: scd.LoadStats = field(default_factory=scd.LoadStats)
    facts_read: int = 0
    facts_inserted: int = 0
    facts_updated: int = 0
    inferred_members: int = 0
    history_problems: list = field(default_factory=list)

    def summary(self) -> str:
        return "\n".join(
            [
                f"  high-water mark     {self.high_water:%Y-%m-%d %H:%M:%S}",
                f"  dim_customer        {self.dim_customer}",
                f"  dim_product         {self.dim_product}",
                f"  fact_order_line     read={self.facts_read} inserted={self.facts_inserted} restated={self.facts_updated}",
                f"  inferred members    {self.inferred_members}",
                f"  history validation  {'clean' if not self.history_problems else '; '.join(self.history_problems)}",
            ]
        )


def initialise(con: duckdb.DuckDBPyConnection, models_dir: str = "models") -> None:
    con.execute(Path(models_dir, "star_schema.sql").read_text())
    watermark.ensure_tables(con)
    _seed_unknown_member(con)


def _seed_unknown_member(con: duckdb.DuckDBPyConnection) -> None:
    """Every dimension needs an Unknown row so facts never carry a null key."""
    exists = con.execute("SELECT count(*) FROM dim_customer WHERE sk = ?", [UNKNOWN_SK]).fetchone()[0]
    if not exists:
        con.execute(
            """INSERT INTO dim_customer
               VALUES (?, 'UNKNOWN', 'Unknown', 'Unknown', 'Unknown', 'Unknown',
                       TIMESTAMP '1900-01-01', TIMESTAMP '9999-12-31 23:59:59', TRUE)""",
            [UNKNOWN_SK],
        )


def build_dim_date(con: duckdb.DuckDBPyConnection, start: str = "2025-01-01", end: str = "2027-12-31") -> int:
    """A date dimension is generated, never loaded -- the calendar is known."""
    con.execute(
        f"""INSERT INTO dim_date
            SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER), d,
                   year(d), quarter(d), month(d), monthname(d),
                   day(d), isodow(d), dayname(d), isodow(d) >= 6
            FROM (SELECT unnest(generate_series(DATE '{start}', DATE '{end}', INTERVAL 1 DAY)) AS d)
            WHERE CAST(strftime(d, '%Y%m%d') AS INTEGER) NOT IN (SELECT date_key FROM dim_date)"""
    )
    return con.execute("SELECT count(*) FROM dim_date").fetchone()[0]


def _high_water(con: duckdb.DuckDBPyConnection) -> datetime:
    """One high-water mark for the whole run, captured once.

    Reading it per table would let a row written between two statements fall
    into the gap: after this table's mark, before the next one's, and skipped by
    both this run and the next.
    """
    marks = [
        con.execute(f"SELECT max(updated_at) FROM {table}").fetchone()[0]
        for table in ("src_customer", "src_product", "src_order_line")
    ]
    marks = [m for m in marks if m is not None]
    return max(marks) if marks else datetime.now()


def run(
    con: duckdb.DuckDBPyConnection,
    models_dir: str = "models",
    lookback: timedelta = watermark.DEFAULT_LOOKBACK,
) -> LoadReport:
    initialise(con, models_dir)
    build_dim_date(con)

    high = _high_water(con)
    ctx = watermark.begin(con, "sales", high, lookback=lookback)
    report = LoadReport(high_water=high)

    try:
        report.dim_customer = _load_customers(con, ctx)
        report.dim_product = _load_products(con, ctx)
        facts = _load_facts(con, ctx)
        report.facts_read, report.facts_inserted, report.facts_updated, report.inferred_members = facts

        watermark.commit(
            con, ctx,
            rows_read=report.facts_read,
            rows_inserted=report.facts_inserted,
            rows_updated=report.facts_updated,
        )
    except Exception as exc:  # the watermark deliberately stays where it was
        watermark.fail(con, ctx, str(exc))
        raise

    report.history_problems = scd.validate_history(con, "dim_customer", "customer_id")
    return report


def _load_customers(con: duckdb.DuckDBPyConnection, ctx: watermark.RunContext) -> scd.LoadStats:
    con.execute(
        """CREATE OR REPLACE TEMP TABLE stg_customer AS
           SELECT customer_id, customer_name, segment, country, credit_rating
           FROM src_customer WHERE updated_at > ? AND updated_at <= ?""",
        [ctx.low_water, ctx.high_water],
    )
    return scd.load_type_2(
        con, "dim_customer", "stg_customer", "customer_id",
        CUSTOMER_ATTRIBUTES, effective_at=ctx.high_water,
    )


def _load_products(con: duckdb.DuckDBPyConnection, ctx: watermark.RunContext) -> scd.LoadStats:
    con.execute(
        """CREATE OR REPLACE TEMP TABLE stg_product AS
           SELECT product_id, product_name, category, unit_cost
           FROM src_product WHERE updated_at > ? AND updated_at <= ?""",
        [ctx.low_water, ctx.high_water],
    )
    return scd.load_type_1(con, "dim_product", "stg_product", "product_id", PRODUCT_ATTRIBUTES)


def _load_facts(con: duckdb.DuckDBPyConnection, ctx: watermark.RunContext) -> tuple:
    con.execute(
        """CREATE OR REPLACE TEMP TABLE stg_order_line AS
           SELECT * FROM src_order_line WHERE updated_at > ? AND updated_at <= ?""",
        [ctx.low_water, ctx.high_water],
    )
    read = con.execute("SELECT count(*) FROM stg_order_line").fetchone()[0]

    # Facts whose customer is not in the dimension at all. Rather than dropping
    # them or nulling the key, they are pointed at the Unknown member and
    # counted, so the number is visible instead of being a silent shortfall.
    inferred = con.execute(
        """SELECT count(*) FROM stg_order_line s
           WHERE NOT EXISTS (SELECT 1 FROM dim_customer d WHERE d.customer_id = s.customer_id)"""
    ).fetchone()[0]

    # The as-at join: pick the dimension version that was live at the order
    # timestamp, not the one that is current now.
    con.execute(
        """CREATE OR REPLACE TEMP TABLE stg_fact AS
           SELECT s.order_line_id,
                  s.order_id,
                  CAST(strftime(s.order_ts, '%Y%m%d') AS INTEGER) AS order_date_key,
                  coalesce(d.sk, -1) AS customer_sk,
                  s.product_id,
                  s.quantity,
                  s.unit_price,
                  s.discount_pct,
                  CAST(s.quantity * s.unit_price AS DECIMAL(18,4)) AS gross_amount,
                  CAST(s.quantity * s.unit_price * (1 - s.discount_pct) AS DECIMAL(18,4)) AS net_amount,
                  s.updated_at AS source_updated_at
           FROM stg_order_line s
           LEFT JOIN dim_customer d
             ON d.customer_id = s.customer_id
            AND s.order_ts >= d.valid_from
            AND s.order_ts <  d.valid_to"""
    )

    updated = con.execute(
        """SELECT count(*) FROM stg_fact s JOIN fact_order_line f
           USING (order_line_id)"""
    ).fetchone()[0]

    # Delete-then-insert on the natural key makes the load idempotent: running
    # the same window twice produces the same table, and a restated line
    # replaces its earlier version rather than duplicating it.
    con.execute(
        """DELETE FROM fact_order_line
           WHERE order_line_id IN (SELECT order_line_id FROM stg_fact)"""
    )
    con.execute(
        """INSERT INTO fact_order_line
           SELECT order_line_id, order_id, order_date_key, customer_sk, product_id,
                  quantity, unit_price, discount_pct, gross_amount, net_amount,
                  source_updated_at, now()
           FROM stg_fact"""
    )
    return read, read - updated, updated, inferred
