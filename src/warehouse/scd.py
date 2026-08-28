"""Slowly Changing Dimension loaders.

Type 1 overwrites; Type 2 keeps history. The Type 2 loader is where the care
goes, because there are three ways to get it subtly wrong and all of them are
invisible until someone asks for a restated report:

  * **Closing the wrong version.** Only the row where ``is_current`` is true may
    be closed. Re-running a load that has already been applied must not create a
    second version, which is why the change comparison is null-safe -- ``a <> b``
    is NULL when either side is NULL, so a plain comparison treats every
    nullable attribute as unchanged forever.

  * **Overlapping validity windows.** The old version's ``valid_to`` and the new
    version's ``valid_from`` must meet exactly, or an as-at join either
    double-counts or finds nothing. Here the old version is closed at the new
    version's effective timestamp.

  * **Losing the surrogate key.** Facts point at the surrogate, not the business
    key, so surrogates are assigned once and never reused.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Sequence

import duckdb

# The open end of a validity window. A sentinel rather than NULL so that
# ``BETWEEN valid_from AND valid_to`` works without a special case in every
# as-at query anyone ever writes against the dimension.
END_OF_TIME = datetime(9999, 12, 31, 23, 59, 59)

# A dimension member seen for the first time is opened at the beginning of time,
# not at the moment it happened to be loaded. This matters more than it looks:
# open a brand-new member at load time and every fact that predates the load
# falls before its validity window, misses the as-at join, and silently lands on
# the Unknown member. On a first full load that is the entire fact table.
#
# The warehouse has no evidence the attributes were ever different, so "always
# had these values, until proven otherwise" is the honest statement. Subsequent
# *changes* are versioned at the moment they were observed.
BEGINNING_OF_TIME = datetime(1900, 1, 1)


@dataclass
class LoadStats:
    inserted: int = 0
    updated: int = 0
    versioned: int = 0
    unchanged: int = 0

    def __str__(self) -> str:
        return (
            f"inserted={self.inserted} versioned={self.versioned} "
            f"updated={self.updated} unchanged={self.unchanged}"
        )


def _quote(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _null_safe_difference(columns: Sequence[str], left: str, right: str) -> str:
    """SQL that is TRUE when any tracked attribute differs, NULLs included."""
    return " OR ".join(
        f"{left}.{_quote(c)} IS DISTINCT FROM {right}.{_quote(c)}" for c in columns
    )


def load_type_1(
    con: duckdb.DuckDBPyConnection,
    target: str,
    staging: str,
    business_key: str,
    attributes: Sequence[str],
) -> LoadStats:
    """Overwrite in place. Used where history has no analytical value."""
    set_clause = ", ".join(f"{_quote(c)} = s.{_quote(c)}" for c in attributes)
    difference = _null_safe_difference(attributes, "t", "s")

    updated = con.execute(
        f"""SELECT count(*) FROM {target} t JOIN {staging} s
            ON t.{_quote(business_key)} = s.{_quote(business_key)}
            WHERE {difference}"""
    ).fetchone()[0]

    con.execute(
        f"""UPDATE {target} AS t SET {set_clause}, updated_at = now()
            FROM {staging} AS s
            WHERE t.{_quote(business_key)} = s.{_quote(business_key)} AND ({difference})"""
    )

    inserted = con.execute(
        f"""SELECT count(*) FROM {staging} s
            WHERE NOT EXISTS (SELECT 1 FROM {target} t
                              WHERE t.{_quote(business_key)} = s.{_quote(business_key)})"""
    ).fetchone()[0]

    column_list = ", ".join(_quote(c) for c in [business_key, *attributes])
    con.execute(
        f"""INSERT INTO {target} ({column_list}, updated_at)
            SELECT {', '.join('s.' + _quote(c) for c in [business_key, *attributes])}, now()
            FROM {staging} s
            WHERE NOT EXISTS (SELECT 1 FROM {target} t
                              WHERE t.{_quote(business_key)} = s.{_quote(business_key)})"""
    )
    return LoadStats(inserted=inserted, updated=updated)


def load_type_2(
    con: duckdb.DuckDBPyConnection,
    target: str,
    staging: str,
    business_key: str,
    attributes: Sequence[str],
    effective_at: datetime,
    surrogate_key: str = "sk",
    initial_from: datetime = BEGINNING_OF_TIME,
) -> LoadStats:
    """Close changed versions and open new ones, atomically.

    Order matters: identify the changes first, then close, then insert. Closing
    before capturing the change set loses the information needed to decide what
    to insert.

    New members open at ``initial_from``; changed members open at
    ``effective_at``. See ``BEGINNING_OF_TIME`` for why those differ.
    """
    bk = _quote(business_key)
    difference = _null_safe_difference(attributes, "t", "s")

    con.execute(
        f"""CREATE OR REPLACE TEMP VIEW _scd_current AS
            SELECT * FROM {target} WHERE is_current"""
    )

    changed = [
        row[0]
        for row in con.execute(
            f"""SELECT s.{bk} FROM {staging} s JOIN _scd_current t ON t.{bk} = s.{bk}
                WHERE {difference}"""
        ).fetchall()
    ]
    brand_new = [
        row[0]
        for row in con.execute(
            f"""SELECT s.{bk} FROM {staging} s
                WHERE NOT EXISTS (SELECT 1 FROM {target} t WHERE t.{bk} = s.{bk})"""
        ).fetchall()
    ]
    total_matched = con.execute(
        f"SELECT count(*) FROM {staging} s JOIN _scd_current t ON t.{bk} = s.{bk}"
    ).fetchone()[0]

    if changed:
        placeholders = ", ".join("?" for _ in changed)
        # Close the outgoing version at exactly the incoming version's start, so
        # the two windows meet with no gap and no overlap.
        con.execute(
            f"""UPDATE {target} SET valid_to = ?, is_current = FALSE
                WHERE is_current AND {bk} IN ({placeholders})""",
            [effective_at, *changed],
        )

    columns = ", ".join(_quote(c) for c in [business_key, *attributes])
    source_columns = ", ".join("s." + _quote(c) for c in [business_key, *attributes])

    def _insert(keys: List[str], valid_from: datetime) -> None:
        if not keys:
            return
        placeholders = ", ".join("?" for _ in keys)
        next_sk = con.execute(
            f"SELECT coalesce(max({_quote(surrogate_key)}), 0) FROM {target}"
        ).fetchone()[0]
        con.execute(
            f"""INSERT INTO {target} ({_quote(surrogate_key)}, {columns},
                                      valid_from, valid_to, is_current)
                SELECT {next_sk} + row_number() OVER (ORDER BY s.{bk}),
                       {source_columns}, ?, ?, TRUE
                FROM {staging} s
                WHERE s.{bk} IN ({placeholders})""",
            [valid_from, END_OF_TIME, *keys],
        )

    _insert(brand_new, initial_from)   # no known history -> open at the beginning
    _insert(changed, effective_at)     # observed change -> version it here

    return LoadStats(
        inserted=len(brand_new),
        versioned=len(changed),
        unchanged=total_matched - len(changed),
    )


def as_at(target: str, moment: datetime) -> str:
    """SQL for the version of each dimension row that was live at ``moment``."""
    return f"SELECT * FROM {target} WHERE '{moment.isoformat()}' >= valid_from AND '{moment.isoformat()}' < valid_to"


def validate_history(con: duckdb.DuckDBPyConnection, target: str, business_key: str) -> List[str]:
    """Assert the invariants a Type 2 dimension must always satisfy.

    Cheap to run after every load, and the only way these defects get noticed
    before a restated report goes out.
    """
    bk = _quote(business_key)
    problems = []

    multiple_current = con.execute(
        f"SELECT count(*) FROM (SELECT {bk} FROM {target} WHERE is_current GROUP BY {bk} HAVING count(*) > 1)"
    ).fetchone()[0]
    if multiple_current:
        problems.append(f"{multiple_current} business key(s) have more than one current version")

    no_current = con.execute(
        f"""SELECT count(*) FROM (SELECT {bk} FROM {target} GROUP BY {bk}
            HAVING count(*) FILTER (WHERE is_current) = 0)"""
    ).fetchone()[0]
    if no_current:
        problems.append(f"{no_current} business key(s) have no current version")

    overlaps = con.execute(
        f"""SELECT count(*) FROM {target} a JOIN {target} b
            ON a.{bk} = b.{bk} AND a.sk <> b.sk
            WHERE a.valid_from < b.valid_to AND b.valid_from < a.valid_to"""
    ).fetchone()[0]
    if overlaps:
        problems.append(f"{overlaps} overlapping validity window pair(s)")

    inverted = con.execute(f"SELECT count(*) FROM {target} WHERE valid_to <= valid_from").fetchone()[0]
    if inverted:
        problems.append(f"{inverted} row(s) with valid_to on or before valid_from")

    return problems
