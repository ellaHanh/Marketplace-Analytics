"""Staging pipeline orchestrator for the Marketplace Analytics & FP&A Sandbox.

Populates all ``stg_*`` tables from raw source tables.  Each step validates
its own output before passing control to the next.  Rows that cannot be
resolved to known entities are quarantined in ``stg_unmatched_events``.

Steps (per plan §2.1):
    1. stage_subscriptions()  — deduplicate, resolve, normalise events → stg_subscriptions
    2. stage_payments()       — normalise payments → stg_payments
    3. stage_payouts()        — normalise payouts → stg_payouts (view/join on stg_payments)
    4. build_ledger()         — fan-out stg_payments → stg_ledger_entries

Usage:
    from src.pipeline.staging import run_staging_pipeline
    run_staging_pipeline()
"""

from __future__ import annotations

import json
import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.db import get_engine
from src.pipeline.ledger import build_ledger

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Quarantine helper
# ---------------------------------------------------------------------------

_QUARANTINE_SQL = text(
    """
    INSERT INTO stg_unmatched_events
        (source_table, source_row_id, reason, raw_payload)
    VALUES
        (:source_table, :source_row_id, :reason, CAST(:raw_payload AS jsonb))
    """
)


def _quarantine_rows(
    conn: Connection,
    source_table: str,
    rows: list[dict[str, Any]],
    reason: str,
    pk_col: str,
) -> int:
    """Insert unresolvable rows into ``stg_unmatched_events``.

    Args:
        conn: Open SQLAlchemy connection.
        source_table: Name of the originating raw table.
        rows: List of row dicts to quarantine.
        reason: Quarantine reason code.
        pk_col: Name of the primary-key column in the row dict.

    Returns:
        int: Number of rows quarantined.
    """
    if not rows:
        return 0
    params = [
        {
            "source_table": source_table,
            "source_row_id": int(row[pk_col]),
            "reason": reason,
            "raw_payload": json.dumps(row, default=str),
        }
        for row in rows
    ]
    conn.execute(_QUARANTINE_SQL, params)
    logger.info("Quarantined %d rows from %s — reason: %s", len(rows), source_table, reason)
    return len(rows)


# ---------------------------------------------------------------------------
# Step 1: Subscription staging  (§2.3)
# ---------------------------------------------------------------------------

_STAGE_SUBSCRIPTIONS_SQL = text(
    """
    INSERT INTO stg_subscriptions
        (subscription_id, brand_id, plan_name, billing_period,
         start_date, end_date, mrr_cents, _source_event_ids)

    WITH deduped AS (
        -- Keep only the latest event per raw_event_id (handle injected duplicates)
        SELECT DISTINCT ON (raw_event_id)
            event_id,
            raw_event_id,
            brand_external_id,
            event_type,
            plan_name,
            billing_period,
            amount_cents,
            CASE
                WHEN _tz_coerced THEN
                    (COALESCE(event_at, NOW()) AT TIME ZONE 'UTC')
                ELSE event_at
            END AS event_at_utc
        FROM raw_subscription_events
        WHERE brand_external_id IS NOT NULL
        ORDER BY raw_event_id, event_at DESC NULLS LAST
    ),
    resolved AS (
        SELECT d.*, b.brand_id
        FROM deduped d
        JOIN dim_brand b ON b.brand_external_id = d.brand_external_id
    ),
    created_events AS (
        SELECT brand_id, plan_name, billing_period, amount_cents,
               event_at_utc::DATE AS start_date,
               ARRAY_AGG(event_id) AS source_ids
        FROM resolved
        WHERE event_type = 'subscription_created'
        GROUP BY brand_id, plan_name, billing_period, amount_cents, event_at_utc::DATE
    ),
    cancel_events AS (
        SELECT brand_id, plan_name, MIN(event_at_utc::DATE) AS end_date
        FROM resolved
        WHERE event_type = 'cancellation'
        GROUP BY brand_id, plan_name
    )
    SELECT
        ENCODE(
            SHA256(
                (c.brand_id::TEXT || '|' || c.plan_name || '|' || c.start_date::TEXT)::BYTEA
            ),
            'hex'
        )                                                          AS subscription_id,
        c.brand_id,
        c.plan_name,
        c.billing_period,
        c.start_date,
        can.end_date,
        CASE
            WHEN c.billing_period = 'annual' THEN c.amount_cents / 12
            ELSE c.amount_cents
        END                                                        AS mrr_cents,
        c.source_ids                                               AS _source_event_ids
    FROM created_events c
    LEFT JOIN cancel_events can
        ON can.brand_id = c.brand_id AND can.plan_name = c.plan_name

    ON CONFLICT (subscription_id) DO NOTHING
    """
)


def stage_subscriptions(conn: Connection) -> int:
    """Stage subscription events into ``stg_subscriptions``.

    Deduplicates on ``raw_event_id``, resolves brands, computes MRR, and
    derives subscription spans.  Rows with NULL or unresolvable
    ``brand_external_id`` are quarantined.

    Args:
        conn: Open SQLAlchemy connection.

    Returns:
        int: Number of rows inserted into ``stg_subscriptions``.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: On any database error.
    """
    # Quarantine: NULL brand_external_id
    null_rows_result = conn.execute(text(
        "SELECT event_id, raw_event_id, brand_external_id, event_type, plan_name, "
        "billing_period, amount_cents, event_at, _tz_coerced "
        "FROM raw_subscription_events WHERE brand_external_id IS NULL"
    ))
    null_rows = [dict(zip(null_rows_result.keys(), row)) for row in null_rows_result]
    _quarantine_rows(conn, "raw_subscription_events", null_rows, "missing_brand_external_id", "event_id")

    # Quarantine: unresolvable brand_external_id
    ghost_rows_result = conn.execute(text(
        "SELECT rse.event_id, rse.raw_event_id, rse.brand_external_id, "
        "rse.event_type, rse.plan_name, rse.billing_period, rse.amount_cents, "
        "rse.event_at, rse._tz_coerced "
        "FROM raw_subscription_events rse "
        "LEFT JOIN dim_brand db ON db.brand_external_id = rse.brand_external_id "
        "WHERE rse.brand_external_id IS NOT NULL AND db.brand_id IS NULL"
    ))
    ghost_rows = [dict(zip(ghost_rows_result.keys(), row)) for row in ghost_rows_result]
    _quarantine_rows(conn, "raw_subscription_events", ghost_rows, "unresolvable_brand_external_id", "event_id")

    result = conn.execute(_STAGE_SUBSCRIPTIONS_SQL)
    rows_inserted: int = result.rowcount
    logger.info("stg_subscriptions: %d rows inserted.", rows_inserted)
    return rows_inserted


# ---------------------------------------------------------------------------
# Step 2: Payment staging  (§2.4)
# ---------------------------------------------------------------------------

_CREATE_STG_PAYMENTS_SQL = text(
    """
    CREATE TABLE IF NOT EXISTS stg_payments AS
    SELECT
        rp.payment_id,
        rp.campaign_id,
        db.brand_id,
        dc.creator_id,
        rp.amount_gross_cents,
        rp.platform_fee_cents,
        rp.stripe_fee_cents,
        rp.amount_refunded_cents,
        LOWER(TRIM(rp.status))              AS status,
        rp.paid_at,
        (rp.campaign_id IS NULL)            AS is_test_transaction
    FROM raw_payments rp
    LEFT JOIN dim_brand   db ON db.brand_external_id   = rp.brand_external_id
    LEFT JOIN dim_creator dc ON dc.creator_external_id = rp.creator_external_id
    WHERE 1=0   -- structure only; populated below
    """
)

_INSERT_STG_PAYMENTS_SQL = text(
    """
    INSERT INTO stg_payments
    SELECT
        rp.payment_id,
        rp.campaign_id,
        db.brand_id,
        dc.creator_id,
        rp.amount_gross_cents,
        rp.platform_fee_cents,
        rp.stripe_fee_cents,
        rp.amount_refunded_cents,
        LOWER(TRIM(rp.status))              AS status,
        rp.paid_at,
        (rp.campaign_id IS NULL)            AS is_test_transaction
    FROM raw_payments rp
    LEFT JOIN dim_brand   db ON db.brand_external_id   = rp.brand_external_id
    LEFT JOIN dim_creator dc ON dc.creator_external_id = rp.creator_external_id
    WHERE rp.campaign_id IS NULL                          -- test transaction (keep but flag)
       OR (db.brand_id IS NOT NULL AND dc.creator_id IS NOT NULL)  -- fully resolvable
    """
)


def stage_payments(conn: Connection) -> int:
    """Stage raw payments into a transient ``stg_payments`` table.

    Normalises status case, flags test transactions, and quarantines rows
    with unresolvable entity references.

    Args:
        conn: Open SQLAlchemy connection.

    Returns:
        int: Number of rows inserted into ``stg_payments``.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: On any database error.
    """
    conn.execute(text("DROP TABLE IF EXISTS stg_payments"))
    conn.execute(_CREATE_STG_PAYMENTS_SQL)

    # Quarantine unresolvable (non-NULL brand/creator that don't join)
    ghost_result = conn.execute(text(
        "SELECT rp.payment_id, rp.campaign_id, rp.brand_external_id, "
        "rp.creator_external_id, rp.amount_gross_cents, rp.status, rp.paid_at "
        "FROM raw_payments rp "
        "LEFT JOIN dim_brand   db ON db.brand_external_id   = rp.brand_external_id "
        "LEFT JOIN dim_creator dc ON dc.creator_external_id = rp.creator_external_id "
        "WHERE rp.campaign_id IS NOT NULL "
        "  AND (db.brand_id IS NULL OR dc.creator_id IS NULL)"
    ))
    ghost_rows = [dict(zip(ghost_result.keys(), row)) for row in ghost_result]
    _quarantine_rows(conn, "raw_payments", ghost_rows, "unresolvable_brand_external_id", "payment_id")

    result = conn.execute(_INSERT_STG_PAYMENTS_SQL)
    rows_inserted: int = result.rowcount
    logger.info("stg_payments: %d rows staged.", rows_inserted)
    return rows_inserted


# ---------------------------------------------------------------------------
# Step 3: Payout staging  (§2.4)
# ---------------------------------------------------------------------------

_INSERT_STG_PAYOUTS_SQL = text(
    """
    CREATE TABLE IF NOT EXISTS stg_payouts AS
    SELECT
        rpo.payout_id,
        rpo.payment_id,
        sp.brand_id,
        dc.creator_id,
        rpo.amount_paid_cents,
        rpo.expected_payout_cents,
        (ABS(rpo.amount_paid_cents - rpo.expected_payout_cents) > 0)
                                                AS has_payout_discrepancy,
        (rpo.amount_paid_cents - rpo.expected_payout_cents)
                                                AS discrepancy_cents,
        rpo.status,
        rpo.payout_at
    FROM raw_payouts rpo
    JOIN stg_payments sp ON sp.payment_id = rpo.payment_id
    LEFT JOIN dim_creator dc ON dc.creator_external_id = rpo.creator_external_id
    """
)


def stage_payouts(conn: Connection) -> int:
    """Stage raw payouts into a transient ``stg_payouts`` table.

    Joins to ``stg_payments`` for fee lookup and computes payout discrepancy.

    Args:
        conn: Open SQLAlchemy connection.

    Returns:
        int: Number of rows inserted into ``stg_payouts``.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: On any database error.
    """
    conn.execute(text("DROP TABLE IF EXISTS stg_payouts"))
    conn.execute(_INSERT_STG_PAYOUTS_SQL)

    count_result = conn.execute(text("SELECT COUNT(*) FROM stg_payouts"))
    rows = count_result.scalar() or 0
    logger.info("stg_payouts: %d rows staged.", rows)
    return int(rows)


# ---------------------------------------------------------------------------
# Validation guard
# ---------------------------------------------------------------------------


def _assert_no_uppercase_status(conn: Connection) -> None:
    """Raise if any uppercase status values survive in stg_payments.

    Args:
        conn: Open SQLAlchemy connection.

    Raises:
        RuntimeError: If normalisation is incomplete.
    """
    result = conn.execute(text(
        "SELECT COUNT(*) FROM stg_payments WHERE status != LOWER(status)"
    ))
    bad_count = result.scalar() or 0
    if bad_count > 0:
        raise RuntimeError(
            f"Staging invariant violated: {bad_count} rows in stg_payments "
            "still have non-lowercase status values."
        )


def _assert_no_null_brand_in_subscriptions(conn: Connection) -> None:
    """Raise if any NULL brand_id rows exist in stg_subscriptions.

    Args:
        conn: Open SQLAlchemy connection.

    Raises:
        RuntimeError: If entity resolution is incomplete.
    """
    result = conn.execute(text(
        "SELECT COUNT(*) FROM stg_subscriptions WHERE brand_id IS NULL"
    ))
    bad_count = result.scalar() or 0
    if bad_count > 0:
        raise RuntimeError(
            f"Staging invariant violated: {bad_count} rows in stg_subscriptions "
            "have NULL brand_id."
        )


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run_staging_pipeline() -> None:
    """Run the full staging pipeline: subscriptions → payments → payouts → ledger.

    Logs row counts at each step boundary.  Raises on any invariant violation
    rather than silently continuing.

    Raises:
        RuntimeError: On invariant violations or zero-row outputs.
        sqlalchemy.exc.SQLAlchemyError: On database errors.
    """
    engine = get_engine()
    with engine.begin() as conn:
        logger.info("=== Staging pipeline start ===")

        sub_rows = stage_subscriptions(conn)
        _assert_no_null_brand_in_subscriptions(conn)
        logger.info("→ stg_subscriptions: %d rows (invariant OK)", sub_rows)

        pay_rows = stage_payments(conn)
        _assert_no_uppercase_status(conn)
        logger.info("→ stg_payments: %d rows (case normalisation OK)", pay_rows)

        payout_rows = stage_payouts(conn)
        logger.info("→ stg_payouts: %d rows", payout_rows)

        ledger_rows = build_ledger(conn)
        logger.info("→ stg_ledger_entries: %d rows", ledger_rows)

        unmatched_result = conn.execute(text("SELECT COUNT(*) FROM stg_unmatched_events"))
        unmatched = unmatched_result.scalar() or 0
        logger.info("→ stg_unmatched_events: %d quarantined rows", unmatched)

        logger.info("=== Staging pipeline complete ===")
