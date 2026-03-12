"""Database connection utilities for the Marketplace Analytics & FP&A Sandbox.

Provides two public surfaces:

* ``get_engine()`` — returns a singleton SQLAlchemy engine suitable for
  ORM queries and ``pd.read_sql`` / ``DataFrame.to_sql`` calls.
* ``get_connection()`` — context manager that yields a raw ``psycopg2``
  connection, required for high-throughput ``COPY FROM STDIN`` bulk loads.
* ``reset_database()`` — drops all project tables, re-applies schema.sql,
  and seeds dim_date.  Intended for ``make reset-db``.

Usage:
    from src.db import get_engine, get_connection, reset_database

    engine = get_engine()
    with engine.connect() as conn:
        result = conn.execute(text("SELECT 1"))

    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.copy_expert("COPY dim_brand FROM STDIN WITH CSV", buf)
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from typing import Generator

import psycopg2
import psycopg2.extensions
from sqlalchemy import Engine, create_engine, text

from src.config import settings

logger = logging.getLogger(__name__)

_engine: Engine | None = None
_engine_lock: Lock = Lock()

_SQL_DIR = Path(__file__).parent.parent / "sql"


def get_engine() -> Engine:
    """Return (or lazily create) the singleton SQLAlchemy engine.

    The engine is configured with a conservative connection pool suitable for
    a single-process pipeline.  Call this function from anywhere that needs
    SQLAlchemy; the pool is shared across all callers in the same process.

    Returns:
        Engine: Configured SQLAlchemy engine connected to DATABASE_URL.

    Raises:
        sqlalchemy.exc.OperationalError: If the database is unreachable.
    """
    global _engine
    if _engine is None:
        with _engine_lock:
            if _engine is None:
                logger.debug("Creating SQLAlchemy engine for %s", settings.database_url)
                _engine = create_engine(
                    settings.database_url,
                    pool_size=5,
                    max_overflow=10,
                    pool_pre_ping=True,
                    echo=False,
                )
    return _engine


@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Yield a raw psycopg2 connection for bulk COPY operations.

    The connection is committed on clean exit and rolled back (then closed) if
    an exception propagates.  Always use this as a context manager::

        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.copy_expert(sql, data_buffer)
            # commit happens automatically on exit

    Yields:
        psycopg2.extensions.connection: Open, auto-committed connection.

    Raises:
        psycopg2.OperationalError: If the database is unreachable.
        Exception: Re-raises any exception after rolling back.
    """
    conn: psycopg2.extensions.connection = psycopg2.connect(settings.database_url)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def reset_database() -> None:
    """Drop all project tables, re-apply schema.sql, and seed dim_date.

    Intended exclusively for ``make reset-db`` and the test fixture.  This
    function is destructive and irreversible — all data is lost.

    Raises:
        FileNotFoundError: If schema.sql or dim_date.sql are missing.
        sqlalchemy.exc.SQLAlchemyError: On any DDL execution error.
    """
    schema_path = _SQL_DIR / "schema.sql"
    dim_date_path = _SQL_DIR / "seeds" / "dim_date.sql"

    for path in (schema_path, dim_date_path):
        if not path.exists():
            raise FileNotFoundError(f"Required SQL file not found: {path}")

    engine = get_engine()
    with engine.begin() as conn:
        logger.info("Dropping all tables (CASCADE)…")
        conn.execute(text(
            """
            DROP TABLE IF EXISTS
                mart_monthly_subscriptions,
                mart_daily_financials,
                stg_ledger_entries,
                stg_unmatched_events,
                stg_subscriptions,
                raw_payouts,
                raw_payments,
                raw_campaigns,
                raw_subscription_events,
                dim_creator,
                dim_brand,
                dim_date
            CASCADE;
            """
        ))

        logger.info("Applying schema.sql…")
        conn.execute(text(schema_path.read_text()))

        logger.info("Seeding dim_date…")
        conn.execute(text(dim_date_path.read_text()))

    logger.info("Database reset complete.")
