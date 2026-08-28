"""End-to-end incremental behaviour across three consecutive loads."""

from datetime import timedelta

import duckdb
import pytest

from warehouse import loader, source
from warehouse.scd import validate_history

from conftest import MODELS


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    db = tmp_path_factory.mktemp("wh") / "warehouse.duckdb"
    con = duckdb.connect(str(db))
    source.build(con, orders=4_000)

    first = loader.run(con, MODELS)
    second = loader.run(con, MODELS)          # no source change at all
    changes = source.apply_day_two_changes(con)
    third = loader.run(con, MODELS)

    yield con, first, second, third, changes
    con.close()


def test_first_load_populates_every_dimension_and_the_fact(warehouse):
    _, first, _, _, _ = warehouse
    assert first.dim_customer.inserted == 800
    assert first.dim_product.inserted == 200
    assert first.facts_inserted == 4_000


def test_reload_with_no_source_change_inserts_nothing(warehouse):
    """The lookback re-reads rows, but idempotency means nothing is duplicated."""
    con, first, second, _, _ = warehouse
    assert second.dim_customer.inserted == 0
    assert second.dim_customer.versioned == 0
    assert second.facts_inserted == 0


def test_fact_table_never_duplicates_a_line(warehouse):
    con = warehouse[0]
    duplicates = con.execute(
        "SELECT count(*) FROM (SELECT order_line_id FROM fact_order_line "
        "GROUP BY order_line_id HAVING count(*) > 1)"
    ).fetchone()[0]
    assert duplicates == 0


def test_changed_customers_are_versioned_not_overwritten(warehouse):
    _, _, _, third, changes = warehouse
    # Some reassessments land on the same values by chance and are correctly
    # treated as unchanged, so versioned + unchanged accounts for all of them.
    assert third.dim_customer.versioned > 0
    assert third.dim_customer.versioned + third.dim_customer.unchanged >= changes["customers_reassessed"]


def test_renamed_products_are_overwritten_with_no_history(warehouse):
    con, _, _, third, changes = warehouse
    assert third.dim_product.updated == changes["products_renamed"]
    assert con.execute("SELECT count(*) FROM dim_product").fetchone()[0] == 200
    assert con.execute(
        "SELECT count(*) FROM dim_product WHERE product_name LIKE '%(v2)'"
    ).fetchone()[0] == changes["products_renamed"]


def test_restated_order_lines_replace_rather_than_duplicate(warehouse):
    con, _, _, third, _ = warehouse
    assert third.facts_updated > 0
    total = con.execute("SELECT count(*) FROM fact_order_line").fetchone()[0]
    source_total = con.execute("SELECT count(*) FROM src_order_line").fetchone()[0]
    assert total == source_total


def test_back_dated_correction_is_caught_by_the_lookback_window(warehouse):
    """It sits behind the watermark; only the trailing re-read finds it."""
    con = warehouse[0]
    fact_qty = con.execute(
        "SELECT quantity FROM fact_order_line WHERE order_line_id = 'OL-0000001'"
    ).fetchone()[0]
    source_qty = con.execute(
        "SELECT quantity FROM src_order_line WHERE order_line_id = 'OL-0000001'"
    ).fetchone()[0]
    assert fact_qty == source_qty


def test_zero_lookback_would_miss_the_back_dated_correction(tmp_path):
    """The counterexample that justifies the lookback existing at all."""
    db = tmp_path / "strict.duckdb"
    con = duckdb.connect(str(db))
    source.build(con, orders=2_000)
    loader.run(con, MODELS, lookback=timedelta(0))
    source.apply_day_two_changes(con)
    loader.run(con, MODELS, lookback=timedelta(0))

    fact_qty = con.execute(
        "SELECT quantity FROM fact_order_line WHERE order_line_id = 'OL-0000001'"
    ).fetchone()[0]
    source_qty = con.execute(
        "SELECT quantity FROM src_order_line WHERE order_line_id = 'OL-0000001'"
    ).fetchone()[0]
    con.close()
    assert fact_qty != source_qty  # the correction was silently skipped


def test_facts_join_to_the_dimension_version_live_at_order_time(warehouse):
    """An as-at join, not a current-version join -- history must not restate."""
    con = warehouse[0]
    mismatched = con.execute(
        """SELECT count(*) FROM fact_order_line f
           JOIN dim_customer d ON d.sk = f.customer_sk
           JOIN src_order_line s ON s.order_line_id = f.order_line_id
           WHERE s.order_ts < d.valid_from OR s.order_ts >= d.valid_to"""
    ).fetchone()[0]
    assert mismatched == 0


def test_no_fact_carries_a_null_or_orphan_customer_key(warehouse):
    con = warehouse[0]
    orphans = con.execute(
        """SELECT count(*) FROM fact_order_line f
           WHERE f.customer_sk IS NULL
              OR NOT EXISTS (SELECT 1 FROM dim_customer d WHERE d.sk = f.customer_sk)"""
    ).fetchone()[0]
    assert orphans == 0


def test_dimension_history_stays_valid_after_every_load(warehouse):
    con = warehouse[0]
    assert validate_history(con, "dim_customer", "customer_id") == []


def test_run_log_records_one_success_per_load(warehouse):
    con = warehouse[0]
    successes = con.execute(
        "SELECT count(*) FROM etl_run_log WHERE status = 'SUCCESS'"
    ).fetchone()[0]
    assert successes == 3


def test_facts_resolve_to_a_real_customer_not_the_unknown_member(warehouse):
    """The bug this catches: opening new dimension members at load time instead
    of at the beginning of time puts every pre-existing fact before the
    dimension's validity window, so the as-at join misses and the whole fact
    table lands on Unknown -- while every other integrity check still passes.
    """
    con = warehouse[0]
    unknown = con.execute(
        "SELECT count(*) FROM fact_order_line WHERE customer_sk = -1"
    ).fetchone()[0]
    assert unknown == 0


def test_segment_revenue_is_spread_across_real_segments(warehouse):
    con = warehouse[0]
    rows = con.execute(
        """SELECT c.segment, count(*) FROM fact_order_line f
           JOIN dim_customer c ON c.sk = f.customer_sk
           GROUP BY c.segment"""
    ).fetchall()
    segments = {r[0] for r in rows}
    assert "Unknown" not in segments
    assert len(segments) >= 4
