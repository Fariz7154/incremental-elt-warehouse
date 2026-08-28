import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

MODELS = str(ROOT / "models")


@pytest.fixture
def con():
    connection = duckdb.connect(":memory:")
    yield connection
    connection.close()


@pytest.fixture
def dim(con):
    """An empty Type 2 dimension plus a staging table, ready to load."""
    con.execute(
        """CREATE TABLE dim_customer (
               sk BIGINT, customer_id VARCHAR, customer_name VARCHAR,
               segment VARCHAR, country VARCHAR,
               valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN)"""
    )
    con.execute(
        "CREATE TABLE stg (customer_id VARCHAR, customer_name VARCHAR, segment VARCHAR, country VARCHAR)"
    )
    return con


T1 = datetime(2026, 1, 1)
T2 = datetime(2026, 2, 1)
T3 = datetime(2026, 3, 1)
