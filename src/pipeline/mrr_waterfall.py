"""MRR waterfall mart builder for the Marketplace Analytics & FP&A Sandbox.

Implements Phase 3.2: the most analytically complex pipeline step.

Algorithm (per plan §3.2):
    1. Build a monthly spine of (brand_id, month_start_date) for every brand
       that has any stg_subscriptions activity.
    2. Compute each brand's MRR for each month: sum of mrr_cents across all
       subscriptions active in that month.
    3. Classify MRR movements using LAG():
       - mrr_new:         brand has no prior-month MRR
       - mrr_expansion:   mrr grew vs prior month
       - mrr_contraction: mrr shrank vs prior month
       - mrr_churned:     prior MRR > 0, current = 0
    4. Enforce the continuity invariant: mrr_end[t] == mrr_start[t+1] for all
       consecutive months per brand.  Violations raise PipelineInvariantError.

Usage:
    from src.pipeline.mrr_waterfall import build_mrr_waterfall
    rows = build_mrr_waterfall()
"""

from __future__ import annotations

import logging

from sqlalchemy import text

from src.db import get_engine

logger = logging.getLogger(__name__)


class PipelineInvariantError(Exception):
    """Raised when the MRR continuity invariant is violated after insert."""


_TRUNCATE_SQL = text("TRUNCATE TABLE mart_monthly_subscriptions")

# ---------------------------------------------------------------------------
# Main INSERT: spine × monthly MRR → waterfall classification
# ---------------------------------------------------------------------------
_INSERT_SQL = text(
    """
    INSERT INTO mart_monthly_subscriptions (
        brand_id,
        month_start_date,
        mrr_start_cents,
        mrr_new_cents,
        mrr_expansion_cents,
        mrr_contraction_cents,
        mrr_churned_cents,
        mrr_end_cents
    )
    WITH

    -- ── 1. Spine: every month that has any subscription activity ──────────
    active_brand_months AS (
        SELECT DISTINCT
            ss.brand_id,
            dd.month_start AS month_start_date
        FROM stg_subscriptions ss
        -- Cross-join with every month in the generation window
        CROSS JOIN (
            SELECT DISTINCT month_start
            FROM dim_date
            WHERE date_day BETWEEN
                (SELECT MIN(start_date) FROM stg_subscriptions)
                AND
                DATE_TRUNC('month',
                    COALESCE(
                        (SELECT MAX(end_date)   FROM stg_subscriptions WHERE end_date IS NOT NULL),
                        (SELECT MAX(start_date) FROM stg_subscriptions)
                    )
                ) + INTERVAL '1 month'
        ) dd
    ),

    -- ── 2. MRR per (brand, month): sum of active subscriptions ───────────
    brand_month_mrr AS (
        SELECT
            abm.brand_id,
            abm.month_start_date,
            COALESCE(SUM(ss.mrr_cents), 0) AS mrr_this_month
        FROM active_brand_months abm
        LEFT JOIN stg_subscriptions ss
            ON  ss.brand_id    = abm.brand_id
            AND ss.start_date <= (
                    SELECT month_end
                    FROM dim_date
                    WHERE date_day = abm.month_start_date
                )
            AND (
                    ss.end_date IS NULL
                    OR ss.end_date >= abm.month_start_date
                )
        GROUP BY abm.brand_id, abm.month_start_date
    ),

    -- ── 3. LAG to get prior-month MRR ────────────────────────────────────
    with_prior AS (
        SELECT
            brand_id,
            month_start_date,
            mrr_this_month,
            LAG(mrr_this_month) OVER (
                PARTITION BY brand_id
                ORDER BY month_start_date
            ) AS mrr_prior
        FROM brand_month_mrr
    ),

    -- ── 4. Classify movements ────────────────────────────────────────────
    classified AS (
        SELECT
            brand_id,
            month_start_date,

            -- mrr_start = prior month's MRR (NULL on first appearance → 0)
            COALESCE(mrr_prior, 0) AS mrr_start_cents,

            -- mrr_new: first ever month with MRR
            CASE
                WHEN mrr_prior IS NULL AND mrr_this_month > 0
                THEN mrr_this_month
                ELSE 0
            END AS mrr_new_cents,

            -- mrr_expansion: grew vs prior
            CASE
                WHEN mrr_prior IS NOT NULL
                     AND mrr_this_month > mrr_prior
                     AND mrr_prior > 0
                THEN mrr_this_month - mrr_prior
                ELSE 0
            END AS mrr_expansion_cents,

            -- mrr_contraction: shrank but not to zero
            CASE
                WHEN mrr_prior IS NOT NULL
                     AND mrr_this_month < mrr_prior
                     AND mrr_this_month > 0
                THEN mrr_prior - mrr_this_month
                ELSE 0
            END AS mrr_contraction_cents,

            -- mrr_churned: had MRR, now zero
            CASE
                WHEN mrr_prior IS NOT NULL
                     AND mrr_prior > 0
                     AND mrr_this_month = 0
                THEN mrr_prior
                ELSE 0
            END AS mrr_churned_cents,

            mrr_this_month AS mrr_end_cents
        FROM with_prior
    )

    SELECT
        brand_id,
        month_start_date,
        mrr_start_cents,
        mrr_new_cents,
        mrr_expansion_cents,
        mrr_contraction_cents,
        mrr_churned_cents,
        mrr_end_cents
    FROM classified
    ORDER BY brand_id, month_start_date

    ON CONFLICT (brand_id, month_start_date) DO NOTHING
    """
)

# ---------------------------------------------------------------------------
# Invariant: mrr_end[t] must equal mrr_start[t+1] for consecutive months
# ---------------------------------------------------------------------------
_INVARIANT_SQL = text(
    """
    SELECT COUNT(*) FROM (
        SELECT
            brand_id,
            month_start_date,
            mrr_end_cents,
            LEAD(mrr_start_cents) OVER (
                PARTITION BY brand_id
                ORDER BY month_start_date
            ) AS next_start
        FROM mart_monthly_subscriptions
    ) t
    WHERE mrr_end_cents != next_start
      AND next_start IS NOT NULL
    """
)

_ROW_COUNT_SQL = text("SELECT COUNT(*) FROM mart_monthly_subscriptions")


def build_mrr_waterfall() -> int:
    """Populate ``mart_monthly_subscriptions`` with the MRR waterfall.

    Truncates the mart, inserts all waterfall rows via a single SQL statement,
    then enforces the continuity invariant (mrr_end[t] == mrr_start[t+1]).

    Returns:
        int: Number of rows inserted into ``mart_monthly_subscriptions``.

    Raises:
        PipelineInvariantError: If any brand has mrr_end != next mrr_start,
            indicating a broken waterfall.  The pipeline must halt.
        RuntimeError: If the mart is empty after the insert.
        sqlalchemy.exc.SQLAlchemyError: On any database error.
    """
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(_TRUNCATE_SQL)
        logger.info("Truncated mart_monthly_subscriptions.")

        result = conn.execute(_INSERT_SQL)
        rows_inserted: int = result.rowcount
        logger.info("mart_monthly_subscriptions: %d rows inserted.", rows_inserted)

        if rows_inserted == 0:
            raise RuntimeError(
                "mart_monthly_subscriptions is empty after insert. "
                "Ensure stg_subscriptions is populated before running the waterfall."
            )

        # Enforce continuity invariant
        violation_count = conn.execute(_INVARIANT_SQL).scalar() or 0
        if violation_count > 0:
            raise PipelineInvariantError(
                f"MRR waterfall invariant violated: {violation_count} brand-month pair(s) "
                "have mrr_end_cents != next month's mrr_start_cents. "
                "Pipeline halted — do not proceed to validation."
            )

        logger.info("MRR waterfall invariant OK: 0 continuity violations.")

    return rows_inserted
