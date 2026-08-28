"""Watermarks and the run log.

Incremental loading needs an answer to "what have I already loaded?" that
survives a crash. Two rules make that answer trustworthy:

  1. The watermark advances only after the load it covers has committed. A run
     that dies mid-load leaves the old watermark in place and is simply retried.

  2. Extraction is ``> low AND <= high``, with the high-water mark captured
     once at the start of the run. Reading it per-statement instead lets rows
     that arrive during the run fall into the gap between the watermark and the
     next extract, and be skipped forever.

  3. A **lookback window** is subtracted from the low-water mark, because the
     failure mode of every watermark-based extract is the back-dated write: a
     correction stamped with the original transaction time rather than the
     moment it was made. It lands behind the mark and is never seen again.
     Re-reading a trailing window catches those. This is only affordable
     because the loads are idempotent -- re-processing a row that was already
     loaded replaces it rather than duplicating it -- so the lookback costs
     compute and nothing else.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import duckdb

EPOCH = datetime(1900, 1, 1)

DDL = """
CREATE TABLE IF NOT EXISTS etl_watermark (
    stream        VARCHAR PRIMARY KEY,
    watermark     TIMESTAMP NOT NULL,
    updated_at    TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS etl_run_log (
    run_id        VARCHAR NOT NULL,
    stream        VARCHAR NOT NULL,
    started_at    TIMESTAMP NOT NULL,
    finished_at   TIMESTAMP,
    low_water     TIMESTAMP,
    high_water    TIMESTAMP,
    rows_read     BIGINT,
    rows_inserted BIGINT,
    rows_updated  BIGINT,
    status        VARCHAR NOT NULL
);
"""


@dataclass
class RunContext:
    run_id: str
    stream: str
    low_water: datetime
    high_water: datetime
    started_at: datetime


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(DDL)


def get(con: duckdb.DuckDBPyConnection, stream: str) -> datetime:
    """Current watermark for a stream; the epoch if it has never run."""
    row = con.execute("SELECT watermark FROM etl_watermark WHERE stream = ?", [stream]).fetchone()
    return row[0] if row else EPOCH


DEFAULT_LOOKBACK = timedelta(days=2)


def begin(
    con: duckdb.DuckDBPyConnection,
    stream: str,
    high_water: datetime,
    lookback: timedelta = DEFAULT_LOOKBACK,
) -> RunContext:
    """Open a run. The extract window starts ``lookback`` before the watermark.

    The watermark itself still advances to ``high_water``; the lookback only
    widens what is read, never what is remembered.
    """
    mark = get(con, stream)
    low_water = EPOCH if mark == EPOCH else mark - lookback
    ctx = RunContext(
        run_id=uuid.uuid4().hex[:12],
        stream=stream,
        low_water=low_water,
        high_water=high_water,
        started_at=datetime.now(),
    )
    con.execute(
        """INSERT INTO etl_run_log
           (run_id, stream, started_at, low_water, high_water, status)
           VALUES (?, ?, ?, ?, ?, 'RUNNING')""",
        [ctx.run_id, ctx.stream, ctx.started_at, ctx.low_water, ctx.high_water],
    )
    return ctx


def commit(
    con: duckdb.DuckDBPyConnection,
    ctx: RunContext,
    rows_read: int,
    rows_inserted: int = 0,
    rows_updated: int = 0,
) -> None:
    """Advance the watermark and close the run log entry, in that order."""
    con.execute(
        """INSERT INTO etl_watermark (stream, watermark, updated_at) VALUES (?, ?, ?)
           ON CONFLICT (stream) DO UPDATE SET watermark = excluded.watermark,
                                              updated_at = excluded.updated_at""",
        [ctx.stream, ctx.high_water, datetime.now()],
    )
    con.execute(
        """UPDATE etl_run_log
           SET finished_at = ?, rows_read = ?, rows_inserted = ?, rows_updated = ?, status = 'SUCCESS'
           WHERE run_id = ? AND stream = ?""",
        [datetime.now(), rows_read, rows_inserted, rows_updated, ctx.run_id, ctx.stream],
    )


def fail(con: duckdb.DuckDBPyConnection, ctx: RunContext, error: str = "") -> None:
    """Close the run as failed and leave the watermark untouched, so it retries."""
    con.execute(
        """UPDATE etl_run_log SET finished_at = ?, status = ? WHERE run_id = ? AND stream = ?""",
        [datetime.now(), f"FAILED: {error}"[:200], ctx.run_id, ctx.stream],
    )


def reset(con: duckdb.DuckDBPyConnection, stream: Optional[str] = None) -> None:
    """Rewind for a full reload. Deliberately explicit -- never automatic."""
    if stream:
        con.execute("DELETE FROM etl_watermark WHERE stream = ?", [stream])
    else:
        con.execute("DELETE FROM etl_watermark")
