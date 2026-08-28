"""SCD Type 2 semantics.

Every assertion here corresponds to a way a dimension can be wrong in a manner
that is invisible until someone runs a restated report.
"""

from datetime import datetime

from warehouse.scd import BEGINNING_OF_TIME, END_OF_TIME, load_type_2, validate_history

T1 = datetime(2026, 1, 1)
T2 = datetime(2026, 2, 1)
T3 = datetime(2026, 3, 1)
ATTRS = ["customer_name", "segment", "country"]


def _stage(con, rows):
    con.execute("DELETE FROM stg")
    con.executemany("INSERT INTO stg VALUES (?,?,?,?)", rows)


def _load(con, at):
    return load_type_2(con, "dim_customer", "stg", "customer_id", ATTRS, effective_at=at)


def test_first_load_opens_a_current_version(dim):
    _stage(dim, [("C1", "Acme", "SME", "AE")])
    stats = _load(dim, T1)
    assert stats.inserted == 1 and stats.versioned == 0

    row = dim.execute("SELECT * FROM dim_customer").fetchone()
    assert row[1] == "C1"
    # A first-seen member opens at the beginning of time, not the load time.
    assert row[5] == BEGINNING_OF_TIME
    assert row[6] == END_OF_TIME     # valid_to
    assert row[7] is True            # is_current


def test_unchanged_reload_does_not_create_a_second_version(dim):
    _stage(dim, [("C1", "Acme", "SME", "AE")])
    _load(dim, T1)
    stats = _load(dim, T2)
    assert stats.versioned == 0 and stats.inserted == 0 and stats.unchanged == 1
    assert dim.execute("SELECT count(*) FROM dim_customer").fetchone()[0] == 1


def test_changed_attribute_closes_the_old_version_and_opens_a_new_one(dim):
    _stage(dim, [("C1", "Acme", "SME", "AE")])
    _load(dim, T1)
    _stage(dim, [("C1", "Acme", "Corporate", "AE")])
    stats = _load(dim, T2)

    assert stats.versioned == 1
    rows = dim.execute("SELECT segment, valid_from, valid_to, is_current FROM dim_customer ORDER BY valid_from").fetchall()
    assert len(rows) == 2
    old, new = rows
    assert old[0] == "SME" and old[2] == T2 and old[3] is False
    assert new[0] == "Corporate" and new[1] == T2 and new[2] == END_OF_TIME and new[3] is True


def test_validity_windows_meet_exactly(dim):
    """No gap and no overlap -- an as-at join must find exactly one version."""
    _stage(dim, [("C1", "Acme", "SME", "AE")])
    _load(dim, T1)
    _stage(dim, [("C1", "Acme", "Corporate", "AE")])
    _load(dim, T2)
    _stage(dim, [("C1", "Acme", "Private", "AE")])
    _load(dim, T3)

    for moment in (T1, T2, T3, datetime(2026, 1, 15), datetime(2026, 2, 15)):
        n = dim.execute(
            "SELECT count(*) FROM dim_customer WHERE ? >= valid_from AND ? < valid_to",
            [moment, moment],
        ).fetchone()[0]
        assert n == 1, f"expected exactly one live version at {moment}, found {n}"


def test_null_attribute_change_is_detected(dim):
    """``a <> b`` is NULL when either side is NULL, so a plain comparison would
    treat every nullable attribute as unchanged forever."""
    _stage(dim, [("C1", "Acme", None, "AE")])
    _load(dim, T1)
    _stage(dim, [("C1", "Acme", "SME", "AE")])
    assert _load(dim, T2).versioned == 1


def test_change_to_null_is_also_detected(dim):
    _stage(dim, [("C1", "Acme", "SME", "AE")])
    _load(dim, T1)
    _stage(dim, [("C1", "Acme", None, "AE")])
    assert _load(dim, T2).versioned == 1


def test_only_the_current_version_is_ever_closed(dim):
    _stage(dim, [("C1", "Acme", "SME", "AE")])
    _load(dim, T1)
    _stage(dim, [("C1", "Acme", "Corporate", "AE")])
    _load(dim, T2)
    _stage(dim, [("C1", "Acme", "Private", "AE")])
    _load(dim, T3)

    closed = dim.execute("SELECT valid_to FROM dim_customer WHERE NOT is_current ORDER BY valid_from").fetchall()
    assert [c[0] for c in closed] == [T2, T3]
    assert dim.execute("SELECT count(*) FROM dim_customer WHERE is_current").fetchone()[0] == 1


def test_surrogate_keys_are_unique_and_never_reused(dim):
    _stage(dim, [("C1", "Acme", "SME", "AE"), ("C2", "Beta", "Retail", "IN")])
    _load(dim, T1)
    _stage(dim, [("C1", "Acme", "Corporate", "AE"), ("C2", "Beta", "Retail", "IN")])
    _load(dim, T2)

    sks = [r[0] for r in dim.execute("SELECT sk FROM dim_customer").fetchall()]
    assert len(sks) == len(set(sks)) == 3


def test_new_and_changed_rows_load_in_the_same_pass(dim):
    _stage(dim, [("C1", "Acme", "SME", "AE")])
    _load(dim, T1)
    _stage(dim, [("C1", "Acme", "Corporate", "AE"), ("C2", "Beta", "Retail", "IN")])
    stats = _load(dim, T2)
    assert stats.versioned == 1 and stats.inserted == 1


def test_validate_history_is_clean_after_normal_loads(dim):
    for at, segment in ((T1, "SME"), (T2, "Corporate"), (T3, "Private")):
        _stage(dim, [("C1", "Acme", segment, "AE")])
        _load(dim, at)
    assert validate_history(dim, "dim_customer", "customer_id") == []


def test_validate_history_detects_two_current_versions(dim):
    _stage(dim, [("C1", "Acme", "SME", "AE")])
    _load(dim, T1)
    dim.execute(
        "INSERT INTO dim_customer VALUES (99, 'C1', 'Acme', 'Corporate', 'AE', ?, ?, TRUE)",
        [T2, datetime(9999, 12, 31, 23, 59, 59)],
    )
    problems = validate_history(dim, "dim_customer", "customer_id")
    assert any("more than one current version" in p for p in problems)


def test_validate_history_detects_overlapping_windows(dim):
    _stage(dim, [("C1", "Acme", "SME", "AE")])
    _load(dim, T1)
    dim.execute("UPDATE dim_customer SET valid_to = ?, is_current = FALSE WHERE sk = 1", [T3])
    dim.execute(
        "INSERT INTO dim_customer VALUES (99, 'C1', 'Acme', 'Corporate', 'AE', ?, ?, TRUE)",
        [T2, datetime(9999, 12, 31, 23, 59, 59)],
    )
    problems = validate_history(dim, "dim_customer", "customer_id")
    assert any("overlapping" in p for p in problems)


def test_new_member_opens_at_the_beginning_of_time(dim):
    """So that facts predating the load still find a live version."""
    _stage(dim, [("C1", "Acme", "SME", "AE")])
    _load(dim, T2)
    valid_from = dim.execute("SELECT valid_from FROM dim_customer").fetchone()[0]
    assert valid_from == BEGINNING_OF_TIME


def test_a_change_versions_at_the_observed_time_not_the_beginning(dim):
    _stage(dim, [("C1", "Acme", "SME", "AE")])
    _load(dim, T1)
    _stage(dim, [("C1", "Acme", "Corporate", "AE")])
    _load(dim, T2)
    new_from = dim.execute(
        "SELECT valid_from FROM dim_customer WHERE is_current"
    ).fetchone()[0]
    assert new_from == T2


def test_a_fact_predating_the_load_still_finds_a_version(dim):
    from datetime import datetime

    _stage(dim, [("C1", "Acme", "SME", "AE")])
    _load(dim, T3)  # loaded in March...
    early = datetime(2026, 1, 5)  # ...but the order happened in January
    n = dim.execute(
        "SELECT count(*) FROM dim_customer WHERE ? >= valid_from AND ? < valid_to",
        [early, early],
    ).fetchone()[0]
    assert n == 1
