from datetime import datetime, timedelta

from warehouse import watermark

T1 = datetime(2026, 1, 10, 12, 0)
T2 = datetime(2026, 1, 11, 12, 0)


def _ready(con):
    watermark.ensure_tables(con)
    return con


def test_first_run_starts_at_the_epoch(con):
    _ready(con)
    assert watermark.get(con, "sales") == watermark.EPOCH


def test_first_run_reads_everything_regardless_of_lookback(con):
    """A cold start must not have the lookback subtracted off the epoch."""
    _ready(con)
    ctx = watermark.begin(con, "sales", T1, lookback=timedelta(days=2))
    assert ctx.low_water == watermark.EPOCH


def test_commit_advances_the_watermark(con):
    _ready(con)
    ctx = watermark.begin(con, "sales", T1)
    watermark.commit(con, ctx, rows_read=10, rows_inserted=10)
    assert watermark.get(con, "sales") == T1


def test_failure_leaves_the_watermark_untouched(con):
    """A run that dies mid-load must be retried, not skipped."""
    _ready(con)
    watermark.commit(con, watermark.begin(con, "sales", T1), rows_read=1)
    ctx = watermark.begin(con, "sales", T2)
    watermark.fail(con, ctx, "connection reset")
    assert watermark.get(con, "sales") == T1

    status = con.execute(
        "SELECT status FROM etl_run_log WHERE run_id = ?", [ctx.run_id]
    ).fetchone()[0]
    assert status.startswith("FAILED")


def test_second_run_reads_from_the_watermark_minus_lookback(con):
    _ready(con)
    watermark.commit(con, watermark.begin(con, "sales", T1), rows_read=1)
    ctx = watermark.begin(con, "sales", T2, lookback=timedelta(days=2))
    assert ctx.low_water == T1 - timedelta(days=2)
    assert ctx.high_water == T2


def test_lookback_widens_reads_without_rewinding_the_watermark(con):
    """The mark still advances to the high-water; only the read window widens."""
    _ready(con)
    watermark.commit(con, watermark.begin(con, "sales", T1), rows_read=1)
    ctx = watermark.begin(con, "sales", T2, lookback=timedelta(days=2))
    watermark.commit(con, ctx, rows_read=5)
    assert watermark.get(con, "sales") == T2


def test_zero_lookback_is_a_strict_boundary(con):
    _ready(con)
    watermark.commit(con, watermark.begin(con, "sales", T1), rows_read=1)
    ctx = watermark.begin(con, "sales", T2, lookback=timedelta(0))
    assert ctx.low_water == T1


def test_streams_advance_independently(con):
    _ready(con)
    watermark.commit(con, watermark.begin(con, "sales", T1), rows_read=1)
    assert watermark.get(con, "sales") == T1
    assert watermark.get(con, "inventory") == watermark.EPOCH


def test_reset_forces_a_full_reload(con):
    _ready(con)
    watermark.commit(con, watermark.begin(con, "sales", T1), rows_read=1)
    watermark.reset(con, "sales")
    assert watermark.get(con, "sales") == watermark.EPOCH


def test_run_log_records_the_window_that_was_read(con):
    _ready(con)
    ctx = watermark.begin(con, "sales", T1)
    watermark.commit(con, ctx, rows_read=100, rows_inserted=90, rows_updated=10)
    row = con.execute(
        "SELECT low_water, high_water, rows_read, rows_inserted, rows_updated, status "
        "FROM etl_run_log WHERE run_id = ?", [ctx.run_id]
    ).fetchone()
    assert row[1] == T1 and row[2] == 100 and row[3] == 90 and row[4] == 10
    assert row[5] == "SUCCESS"
