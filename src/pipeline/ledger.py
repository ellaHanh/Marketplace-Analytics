"""Ledger fan-out pipeline step for the Marketplace Analytics & FP&A Sandbox.

Implements §2.5 of the plan: for each qualifying staging payment, inserts
4–5 ledger rows using a single SQL ``INSERT … SELECT … UNION ALL`` statement.
No Python loop iterates over individual payments.

Entry types produced per payment:
    1. brand_charge          +amount_gross_cents      (entry_date = DATE(paid_at))
    2. platform_fee_revenue  +platform_fee_cents      (entry_date = DATE(paid_at))
    3. stripe_processing_fee -stripe_fee_cents        (entry_date = DATE(paid_at))
    4. refund_adjustment     -amount_refunded_cents   (only if > 0)
    5. creator_payout        -amount_paid_cents       (entry_date = DATE(payout_at), paid payouts only)

Usage:
    from src.pipeline.ledger import build_ledger
    rows_inserted = build_ledger(conn)
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Connection

logger = logging.getLogger(__name__)

_LEDGER_SQL = text(
    """
    INSERT INTO stg_ledger_entries (payment_id, entry_type, amount_cents, entry_date)

    -- 1. brand_charge: gross payment amount received from brand
    SELECT
        sp.payment_id,
        'brand_charge'                  AS entry_type,
        sp.amount_gross_cents           AS amount_cents,
        DATE(sp.paid_at)                AS entry_date
    FROM stg_payments sp
    WHERE sp.is_test_transaction = FALSE
      AND LOWER(sp.status) IN ('succeeded', 'refunded')

    UNION ALL

    -- 2. platform_fee_revenue: platform's take from the transaction
    SELECT
        sp.payment_id,
        'platform_fee_revenue'          AS entry_type,
        sp.platform_fee_cents           AS amount_cents,
        DATE(sp.paid_at)                AS entry_date
    FROM stg_payments sp
    WHERE sp.is_test_transaction = FALSE
      AND LOWER(sp.status) IN ('succeeded', 'refunded')

    UNION ALL

    -- 3. stripe_processing_fee: cost of payment processing (negative)
    SELECT
        sp.payment_id,
        'stripe_processing_fee'         AS entry_type,
        -sp.stripe_fee_cents            AS amount_cents,
        DATE(sp.paid_at)                AS entry_date
    FROM stg_payments sp
    WHERE sp.is_test_transaction = FALSE
      AND LOWER(sp.status) IN ('succeeded', 'refunded')

    UNION ALL

    -- 4. refund_adjustment: only rows where a partial refund was issued
    SELECT
        sp.payment_id,
        'refund_adjustment'             AS entry_type,
        -sp.amount_refunded_cents       AS amount_cents,
        DATE(sp.paid_at)                AS entry_date
    FROM stg_payments sp
    WHERE sp.is_test_transaction = FALSE
      AND LOWER(sp.status) IN ('succeeded', 'refunded')
      AND sp.amount_refunded_cents > 0

    UNION ALL

    -- 5. creator_payout: linked payout (negative; date comes from payout_at)
    SELECT
        sp.payment_id,
        'creator_payout'                AS entry_type,
        -rp.amount_paid_cents           AS amount_cents,
        DATE(rp.payout_at)              AS entry_date
    FROM stg_payments sp
    JOIN raw_payouts rp ON rp.payment_id = sp.payment_id
    WHERE sp.is_test_transaction = FALSE
      AND LOWER(sp.status) IN ('succeeded', 'refunded')
      AND rp.status = 'paid'
    """
)


def build_ledger(conn: Connection) -> int:
    """Execute the ledger fan-out INSERT and return the number of rows written.

    This function must be called *after* ``stg_payments`` is populated (i.e.,
    after payment staging completes).

    Args:
        conn: An open, transaction-capable SQLAlchemy connection.

    Returns:
        int: Number of ledger rows inserted.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: On any database error.
        RuntimeError: If no rows were inserted (indicates upstream staging failed).
    """
    result = conn.execute(_LEDGER_SQL)
    rows_inserted: int = result.rowcount

    if rows_inserted == 0:
        raise RuntimeError(
            "Ledger fan-out produced 0 rows. Check stg_payments population "
            "and ensure at least one non-test succeeded payment exists."
        )

    logger.info("Ledger fan-out inserted %d rows into stg_ledger_entries.", rows_inserted)
    return rows_inserted
