"""pytest configuration and shared fixtures for the Marketplace Analytics test suite.

Architecture:
    - ``postgresql_proc``    — session-scoped: auto-spawns a fresh PostgreSQL instance
                               for the test session (no pre-existing server required).
    - ``postgresql``         — function-scoped: creates a fresh, isolated database per test
                               and yields a psycopg3 Connection to it.
    - ``pg_engine``          — function-scoped: SQLAlchemy engine (psycopg2 dialect) built
                               from the psycopg3 connection info; used by SQLAlchemy pipeline code.
    - ``schema``             — function-scoped: applies schema.sql + dim_date.sql to the fresh
                               DB and yields the engine so tests can insert data immediately.
    - ``use_test_db``        — function-scoped: monkeypatches ``src.db._engine`` singleton so
                               pipeline functions that call it internally use the test engine.

Design rules (per plan §5):
    - No generator is ever called from tests.
    - All test data is hand-crafted and inserted inline.
    - No test touches the development database.

Prerequisites:
    pytest-postgresql handles all Postgres setup automatically. Just run: pytest tests/ -v
"""

from __future__ import annotations

from pathlib import Path

import pytest
from pytest_postgresql import factories
from sqlalchemy import Engine, create_engine, text

_SQL_DIR = Path(__file__).parent.parent / "sql"

# ---------------------------------------------------------------------------
# pytest-postgresql auto-spawn factories
# ---------------------------------------------------------------------------

# Auto-spawn a fresh PostgreSQL instance for the test session.
postgresql_proc = factories.postgresql_proc()

# Fresh isolated database per test function.
postgresql = factories.postgresql("postgresql_proc")


# ---------------------------------------------------------------------------
# pg_engine
# ---------------------------------------------------------------------------


@pytest.fixture
def pg_engine(postgresql) -> Engine:
    """Function-scoped SQLAlchemy engine pointed at the ephemeral test database.

    Extracts host/port/user/dbname from the psycopg3 connection provided by the
    ``postgresql`` fixture and constructs a psycopg2-dialect SQLAlchemy engine.

    Args:
        postgresql: psycopg3 Connection from pytest-postgresql (function-scoped).

    Yields:
        Engine: Configured SQLAlchemy engine; disposed on teardown.
    """
    info = postgresql.info
    url = (
        f"postgresql+psycopg2://{info.user}"
        f"@{info.host}:{info.port}/{info.dbname}"
    )
    engine = create_engine(url, echo=False, future=True)
    yield engine
    engine.dispose()


# ---------------------------------------------------------------------------
# schema
# ---------------------------------------------------------------------------


@pytest.fixture
def schema(pg_engine: Engine) -> Engine:
    """Apply DDL and seed dim_date against the fresh test database.

    Runs ``sql/schema.sql`` (all 12 tables + indexes) and
    ``sql/seeds/dim_date.sql`` (2022-07-01 to 2024-07-31).

    Args:
        pg_engine: Test SQLAlchemy engine (function-scoped).

    Yields:
        Engine: The same engine, ready for test data insertion.

    Raises:
        FileNotFoundError: If required SQL files are missing.
        sqlalchemy.exc.SQLAlchemyError: On any DDL error.
    """
    schema_sql = (_SQL_DIR / "schema.sql").read_text()
    dim_date_sql = (_SQL_DIR / "seeds" / "dim_date.sql").read_text()

    with pg_engine.begin() as conn:
        conn.execute(text(schema_sql))
        conn.execute(text(dim_date_sql))

    yield pg_engine


# ---------------------------------------------------------------------------
# use_test_db
# ---------------------------------------------------------------------------


@pytest.fixture
def use_test_db(schema: Engine, monkeypatch: pytest.MonkeyPatch) -> Engine:
    """Patch the ``src.db._engine`` singleton so all pipeline code uses the test engine.

    Pipeline modules import ``get_engine`` at module level, creating a local
    binding that ``monkeypatch.setattr(src.db, "get_engine", ...)`` cannot
    reach.  Patching the cached ``_engine`` singleton directly is the correct
    approach: ``get_engine()`` always checks ``src.db._engine`` first, so
    every in-process caller sees the test engine without any module re-import.

    The fixture also resets the singleton to ``None`` on teardown so subsequent
    tests get a fresh engine from their own fixture chain.

    Args:
        schema: Test engine with DDL already applied.
        monkeypatch: pytest monkeypatch fixture.

    Yields:
        Engine: The test engine (for direct queries in the test body).
    """
    import src.db  # noqa: PLC0415

    monkeypatch.setattr(src.db, "_engine", schema)
    yield schema
