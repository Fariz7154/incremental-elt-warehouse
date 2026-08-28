"""Command line entry point: build the source, run loads, inspect the warehouse."""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from . import loader, scd, source


def _connect(path: str) -> duckdb.DuckDBPyConnection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(path)


def cmd_seed(args) -> int:
    con = _connect(args.database)
    stats = source.build(con, orders=args.orders)
    con.close()
    print(f"seeded source system in {args.database}")
    for k, v in stats.items():
        print(f"  {k:<24}{v:>8,}")
    return 0


def cmd_load(args) -> int:
    con = _connect(args.database)
    try:
        report = loader.run(con, args.models)
        print("\n  incremental load complete")
        print(report.summary())
        print()
    finally:
        con.close()
    return 0


def cmd_change(args) -> int:
    con = _connect(args.database)
    stats = source.apply_day_two_changes(con)
    con.close()
    print("applied source changes")
    for k, v in stats.items():
        print(f"  {k:<28}{v:>6,}")
    return 0


def cmd_inspect(args) -> int:
    con = _connect(args.database)
    print("\n  warehouse contents")
    for table in ("dim_customer", "dim_product", "dim_date", "fact_order_line"):
        n = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"    {table:<20}{n:>10,}")

    versions = con.execute(
        """SELECT count(*) FROM (SELECT customer_id FROM dim_customer
           GROUP BY customer_id HAVING count(*) > 1)"""
    ).fetchone()[0]
    print(f"\n    customers with history  {versions:,}")

    problems = scd.validate_history(con, "dim_customer", "customer_id")
    print(f"    history validation      {'clean' if not problems else '; '.join(problems)}")

    print("\n  run log")
    rows = con.execute(
        """SELECT run_id, low_water, high_water, rows_read, rows_inserted, rows_updated, status
           FROM etl_run_log ORDER BY started_at"""
    ).fetchall()
    for r in rows:
        print(f"    {r[0]}  {str(r[1])[:19]} -> {str(r[2])[:19]}  "
              f"read={r[3] or 0:>6}  ins={r[4] or 0:>6}  upd={r[5] or 0:>5}  {r[6]}")

    print("\n  top segments by net revenue (as booked, not as restated)")
    rows = con.execute(
        """SELECT c.segment, count(*) AS lines, round(sum(f.net_amount), 2) AS net
           FROM fact_order_line f JOIN dim_customer c ON c.sk = f.customer_sk
           GROUP BY c.segment ORDER BY net DESC"""
    ).fetchall()
    for seg, lines, net in rows:
        print(f"    {seg:<14}{lines:>8,} lines   {net:>16,.2f}")
    print()
    con.close()
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="warehouse", description="Incremental ELT into a star schema.")
    parser.add_argument("--database", default="data/warehouse.duckdb")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("seed", help="create the simulated source system")
    p.add_argument("--orders", type=int, default=12_000)
    p.set_defaults(func=cmd_seed)

    p = sub.add_parser("load", help="run an incremental load")
    p.add_argument("--models", default="models")
    p.set_defaults(func=cmd_load)

    p = sub.add_parser("change", help="mutate the source between loads")
    p.set_defaults(func=cmd_change)

    p = sub.add_parser("inspect", help="show warehouse contents and the run log")
    p.set_defaults(func=cmd_inspect)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
