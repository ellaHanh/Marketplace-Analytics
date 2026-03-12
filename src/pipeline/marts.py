"""Daily financials mart builder for the Marketplace Analytics & FP&A Sandbox.

Implements Phase 3.1: a pure-SQL INSERT that pivots ``stg_ledger_entries`` by
``entry_type`` into a single row per calendar date, then computes derived
financial metrics.

The insert is idempotent: it truncates ``mart_daily_financials`` before writing,
so re-running the pipeline always produces a clean mart.

Usage:
    from src.pipeline.marts import build_daily_financials
    rows = build_daily_financials()
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from src.db import get_engine

logger = logging.getLogger(__name__)

_TRUNCATE_SQL = text("TRUNCATE TABLE mart_daily_financials")

_INSERT_SQL = text(
    """
    INSERT INTO mart_daily_financials (
        entry_date,
        gmv_cents,
        net_gmv_cents,
        platform_revenue_cents,
        stripe_fees_cents,
        creator_payouts_cents,
        gross_margin_cents,
        take_rate_gross,
        take_rate_net
    )
    SELECT
        entry_date,

        -- GMV = all brand charges
        COALESCE(SUM(CASE WHEN entry_type = 'brand_charge'           THEN amount_cents END), 0)
            AS gmv_cents,

        -- Net GMV = GMV minus refunds (refund_adjustment is negative in ledger)
        COALESCE(SUM(CASE WHEN entry_type = 'brand_charge'           THEN amount_cents END), 0)
        + COALESCE(SUM(CASE WHEN entry_type = 'refund_adjustment'    THEN amount_cents END), 0)
            AS net_gmv_cents,

        -- Platform revenue = take-rate fees collected
        COALESCE(SUM(CASE WHEN entry_type = 'platform_fee_revenue'   THEN amount_cents END), 0)
            AS platform_revenue_cents,

        -- Stripe fees (negative in ledger; stored as positive magnitude)
        COALESCE(-SUM(CASE WHEN entry_type = 'stripe_processing_fee' THEN amount_cents END), 0)
            AS stripe_fees_cents,

        -- Creator payouts (negative in ledger; stored as positive magnitude)
        COALESCE(-SUM(CASE WHEN entry_type = 'creator_payout'        THEN amount_cents END), 0)
            AS creator_payouts_cents,

        -- Gross margin = platform_revenue - stripe_fees - creator_payouts
        -- All three addends: platform_fee_revenue(+), stripe_processing_fee(-), creator_payout(-)
        COALESCE(SUM(CASE WHEN entry_type = 'platform_fee_revenue'   THEN amount_cents END), 0)
        + COALESCE(SUM(CASE WHEN entry_type = 'stripe_processing_fee' THEN amount_cents END), 0)
        + COALESCE(SUM(CASE WHEN entry_type = 'creator_payout'        THEN amount_cents END), 0)
            AS gross_margin_cents,

        -- Take rate gross = platform revenue / GMV
        ROUND(
            COALESCE(SUM(CASE WHEN entry_type = 'platform_fee_revenue' THEN amount_cents END), 0)::NUMERIC
            / NULLIF(
                COALESCE(SUM(CASE WHEN entry_type = 'brand_charge' THEN amount_cents END), 0),
                0
            ),
            6
        ) AS take_rate_gross,

        -- Take rate net = platform revenue / net GMV
        ROUND(
            COALESCE(SUM(CASE WHEN entry_type = 'platform_fee_revenue' THEN amount_cents END), 0)::NUMERIC
            / NULLIF(
                COALESCE(SUM(CASE WHEN entry_type = 'brand_charge'        THEN amount_cents END), 0)
                + COALESCE(SUM(CASE WHEN entry_type = 'refund_adjustment' THEN amount_cents END), 0),
                0
            ),
            6
        ) AS take_rate_net

    FROM stg_ledger_entries
    GROUP BY entry_date
    ORDER BY entry_date
    """
)

_TAKE_RATE_CHECK_SQL = text(
    """
    SELECT
        MIN(take_rate_gross) AS min_tr,
        MAX(take_rate_gross) AS max_tr
    FROM mart_daily_financials
    WHERE take_rate_gross IS NOT NULL
    """
)

_TAKE_RATE_MIN = 0.05
_TAKE_RATE_MAX = 0.20


def build_daily_financials() -> int:
    """Populate ``mart_daily_financials`` from ``stg_ledger_entries``.

    Truncates the mart first, then inserts one row per ``entry_date`` via a
    single pivoting INSERT … SELECT.  After insertion, validates that all
    take_rate_gross values fall within the expected [0.05, 0.20] band.

    Returns:
        int: Number of rows inserted into ``mart_daily_financials``.

    Raises:
        RuntimeError: If the mart is empty after the insert, or if any
            take_rate_gross value falls outside [0.05, 0.20].
        sqlalchemy.exc.SQLAlchemyError: On any database error.
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(_TRUNCATE_SQL)
        logger.info("Truncated mart_daily_financials.")

        result = conn.execute(_INSERT_SQL)
        rows_inserted: int = result.rowcount
        logger.info("mart_daily_financials: %d rows inserted.", rows_inserted)

        if rows_inserted == 0:
            raise RuntimeError(
                "mart_daily_financials is empty after insert. "
                "Ensure stg_ledger_entries is populated before running marts."
            )

        tr_row = conn.execute(_TAKE_RATE_CHECK_SQL).fetchone()
        if tr_row and tr_row[0] is not None:
            min_tr = float(tr_row[0])
            max_tr = float(tr_row[1])
            if min_tr < _TAKE_RATE_MIN or max_tr > _TAKE_RATE_MAX:
                raise RuntimeError(
                    f"take_rate_gross out of expected range "
                    f"[{_TAKE_RATE_MIN}, {_TAKE_RATE_MAX}]: "
                    f"observed [{min_tr:.4f}, {max_tr:.4f}]"
                )
            logger.info("take_rate_gross range OK: [%.4f, %.4f]", min_tr, max_tr)

    return rows_inserted
