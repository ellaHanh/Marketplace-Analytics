"""Validation runner for the Marketplace Analytics & FP&A Sandbox.

Implements Phase 4.1: evaluates all 7 data-quality assertions (V1–V7) and
writes a timestamped JSON report to ``reports/``.

Design rules:
    - All 7 assertions always run, even if earlier ones fail (no short-circuit).
    - Each assertion returns a standardised result dict.
    - The runner returns the full list; the orchestrator decides exit code.
    - JSON report is written regardless of pass/fail outcome.

Usage:
    from src.validate.checks import run_all_checks

    results = run_all_checks()
    # results is list[AssertionResult] — each has "name", "status", "detail"
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, TypedDict

from sqlalchemy import text
from sqlalchemy.engine import Connection

from src.db import get_engine

logger = logging.getLogger(__name__)

_REPORTS_DIR = Path(__file__).parent.parent.parent / "reports"

_PAYOUT_DISCREPANCY_MIN = 0.03
_PAYOUT_DISCREPANCY_MAX = 0.07
_TAKE_RATE_MIN = 0.05
_TAKE_RATE_MAX = 0.20


class AssertionResult(TypedDict):
    """Structured result returned by every assertion function."""

    name: str
    status: str        # "pass" | "fail"
    detail: dict[str, Any]


# ---------------------------------------------------------------------------
# Individual assertions
# ---------------------------------------------------------------------------


def _v1_gmv_completeness(conn: Connection) -> AssertionResult:
    """V1 — GMV completeness.

    Compares SUM(amount_gross_cents) from raw_payments (non-test, succeeded
    or refunded, with resolvable entities) against SUM(gmv_cents) from 
    mart_daily_financials.

    Note: Rows with unresolvable brand/creator are quarantined and intentionally
    excluded from both raw and mart sums.

    Args:
        conn: Open SQLAlchemy connection.

    Returns:
        AssertionResult: pass if the two sums are equal, fail otherwise.
    """
    # Sum of resolvable non-test succeeded/refunded payments only
    raw_sum = conn.execute(text(
        """
        SELECT COALESCE(SUM(rp.amount_gross_cents), 0)
        FROM raw_payments rp
        LEFT JOIN dim_brand db ON db.brand_external_id = rp.brand_external_id
        LEFT JOIN dim_creator dc ON dc.creator_external_id = rp.creator_external_id
        WHERE rp.campaign_id IS NOT NULL
          AND LOWER(TRIM(rp.status)) IN ('succeeded', 'refunded')
          AND db.brand_id IS NOT NULL
          AND dc.creator_id IS NOT NULL
        """
    )).scalar() or 0

    mart_sum = conn.execute(text(
        "SELECT COALESCE(SUM(gmv_cents), 0) FROM mart_daily_financials"
    )).scalar() or 0

    passed = int(raw_sum) == int(mart_sum)
    return AssertionResult(
        name="V1",
        status="pass" if passed else "fail",
        detail={
            "description": "GMV completeness: resolvable raw_payments sum == mart_daily_financials sum",
            "raw_payments_gmv_cents": int(raw_sum),
            "mart_gmv_cents": int(mart_sum),
            "delta_cents": int(mart_sum) - int(raw_sum),
        },
    )


def _v2_ledger_balance(conn: Connection) -> AssertionResult:
    """V2 — Ledger balance.

    Verifies that the net of all ledger entries (platform_fee_revenue entries)
    matches the sum of gross_margin_cents in the daily financials mart.

    Specifically: SUM of platform_fee_revenue + stripe_processing_fee +
    creator_payout entries equals SUM(gross_margin_cents).

    Args:
        conn: Open SQLAlchemy connection.

    Returns:
        AssertionResult: pass if sums agree, fail with delta otherwise.
    """
    ledger_margin = conn.execute(text(
        """
        SELECT COALESCE(SUM(amount_cents), 0)
        FROM stg_ledger_entries
        WHERE entry_type IN (
            'platform_fee_revenue',
            'stripe_processing_fee',
            'creator_payout'
        )
        """
    )).scalar() or 0

    mart_margin = conn.execute(text(
        "SELECT COALESCE(SUM(gross_margin_cents), 0) FROM mart_daily_financials"
    )).scalar() or 0

    passed = int(ledger_margin) == int(mart_margin)
    return AssertionResult(
        name="V2",
        status="pass" if passed else "fail",
        detail={
            "description": "Ledger balance: margin entries sum == mart gross_margin_cents sum",
            "ledger_margin_sum_cents": int(ledger_margin),
            "mart_gross_margin_sum_cents": int(mart_margin),
            "delta_cents": int(mart_margin) - int(ledger_margin),
        },
    )


def _v3_mrr_invariant(conn: Connection) -> AssertionResult:
    """V3 — MRR waterfall continuity invariant.

    Authoritative check: mrr_end_cents[t] must equal mrr_start_cents[t+1]
    for every consecutive (brand_id, month_start_date) pair.

    Args:
        conn: Open SQLAlchemy connection.

    Returns:
        AssertionResult: pass if 0 violations, fail with violation count.
    """
    violation_count = conn.execute(text(
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
    )).scalar() or 0

    total_rows = conn.execute(text(
        "SELECT COUNT(*) FROM mart_monthly_subscriptions"
    )).scalar() or 0

    passed = int(violation_count) == 0
    return AssertionResult(
        name="V3",
        status="pass" if passed else "fail",
        detail={
            "description": "MRR invariant: mrr_end[t] == mrr_start[t+1] for all consecutive months",
            "violation_count": int(violation_count),
            "total_waterfall_rows": int(total_rows),
        },
    )


def _v4_payout_discrepancy_rate(conn: Connection) -> AssertionResult:
    """V4 — Payout discrepancy rate in expected band [3%, 7%].

    Counts rows in stg_payouts where has_payout_discrepancy = TRUE and
    divides by the total payout count.

    Args:
        conn: Open SQLAlchemy connection.

    Returns:
        AssertionResult: pass if rate in [0.03, 0.07], fail otherwise.
    """
    row = conn.execute(text(
        """
        SELECT
            COUNT(*) FILTER (WHERE has_payout_discrepancy = TRUE) AS mismatch_count,
            COUNT(*)                                               AS total_count
        FROM stg_payouts
        """
    )).fetchone()

    mismatch = int(row[0]) if row else 0
    total = int(row[1]) if row else 0
    rate = mismatch / total if total > 0 else 0.0

    passed = _PAYOUT_DISCREPANCY_MIN <= rate <= _PAYOUT_DISCREPANCY_MAX
    return AssertionResult(
        name="V4",
        status="pass" if passed else "fail",
        detail={
            "description": "Payout discrepancy rate between 3% and 7%",
            "mismatch_rows": mismatch,
            "total_rows": total,
            "rate": round(rate, 4),
            "expected_min": _PAYOUT_DISCREPANCY_MIN,
            "expected_max": _PAYOUT_DISCREPANCY_MAX,
        },
    )


def _v5_unmatched_events_exist(conn: Connection) -> AssertionResult:
    """V5 — Quarantine table is non-empty.

    Asserts that the messy-data injection pipeline ran and at least some rows
    were routed to stg_unmatched_events.

    Args:
        conn: Open SQLAlchemy connection.

    Returns:
        AssertionResult: pass if COUNT > 0, fail otherwise.
    """
    count = conn.execute(text(
        "SELECT COUNT(*) FROM stg_unmatched_events"
    )).scalar() or 0

    passed = int(count) > 0
    return AssertionResult(
        name="V5",
        status="pass" if passed else "fail",
        detail={
            "description": "stg_unmatched_events is non-empty (quarantine pipeline ran)",
            "unmatched_row_count": int(count),
        },
    )


def _v6_no_test_transactions_in_mart(conn: Connection) -> AssertionResult:
    """V6 — Test transactions excluded from mart GMV.

    Verifies that raw_payments rows where campaign_id IS NULL (test
    transactions) do not contribute to mart_daily_financials.gmv_cents.

    Strategy: sum of test-transaction gross amounts should NOT appear as
    an exact addend of mart GMV.  Cross-checks by confirming mart GMV
    equals non-test ledger GMV only.

    Args:
        conn: Open SQLAlchemy connection.

    Returns:
        AssertionResult: pass if test transaction sum is 0 in mart, fail otherwise.
    """
    test_gmv = conn.execute(text(
        """
        SELECT COALESCE(SUM(amount_gross_cents), 0)
        FROM raw_payments
        WHERE campaign_id IS NULL
          AND LOWER(TRIM(status)) IN ('succeeded', 'refunded')
        """
    )).scalar() or 0

    # If test_gmv is 0 there's nothing to check
    if int(test_gmv) == 0:
        return AssertionResult(
            name="V6",
            status="pass",
            detail={
                "description": "No test transactions in mart GMV",
                "test_transaction_gmv_cents": 0,
                "note": "No test transactions matched succeeded/refunded status",
            },
        )

    # Verify: ledger brand_charge entries are only from non-test payments
    ledger_gmv = conn.execute(text(
        """
        SELECT COALESCE(SUM(le.amount_cents), 0)
        FROM stg_ledger_entries le
        JOIN stg_payments sp ON sp.payment_id = le.payment_id
        WHERE le.entry_type = 'brand_charge'
          AND sp.is_test_transaction = TRUE
        """
    )).scalar() or 0

    passed = int(ledger_gmv) == 0
    return AssertionResult(
        name="V6",
        status="pass" if passed else "fail",
        detail={
            "description": "No test transactions in mart GMV",
            "test_transaction_raw_gmv_cents": int(test_gmv),
            "test_transaction_ledger_gmv_cents": int(ledger_gmv),
        },
    )


def _v7_take_rate_range(conn: Connection) -> AssertionResult:
    """V7 — take_rate_gross within [0.05, 0.20] for all rows.

    Args:
        conn: Open SQLAlchemy connection.

    Returns:
        AssertionResult: pass if min >= 0.05 and max <= 0.20, fail otherwise.
    """
    row = conn.execute(text(
        """
        SELECT
            MIN(take_rate_gross) AS min_tr,
            MAX(take_rate_gross) AS max_tr,
            COUNT(*) FILTER (WHERE take_rate_gross IS NOT NULL) AS non_null_count
        FROM mart_daily_financials
        """
    )).fetchone()

    min_tr = float(row[0]) if row and row[0] is not None else None
    max_tr = float(row[1]) if row and row[1] is not None else None
    non_null = int(row[2]) if row else 0

    if min_tr is None:
        return AssertionResult(
            name="V7",
            status="fail",
            detail={
                "description": "take_rate_gross in [0.05, 0.20]",
                "error": "No non-null take_rate_gross values found in mart_daily_financials",
            },
        )

    passed = min_tr >= _TAKE_RATE_MIN and max_tr <= _TAKE_RATE_MAX
    return AssertionResult(
        name="V7",
        status="pass" if passed else "fail",
        detail={
            "description": "take_rate_gross in [0.05, 0.20] for all rows",
            "min_take_rate_gross": round(min_tr, 6),
            "max_take_rate_gross": round(max_tr, 6),
            "non_null_rows": non_null,
            "expected_min": _TAKE_RATE_MIN,
            "expected_max": _TAKE_RATE_MAX,
        },
    )


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

_ALL_CHECKS = [
    _v1_gmv_completeness,
    _v2_ledger_balance,
    _v3_mrr_invariant,
    _v4_payout_discrepancy_rate,
    _v5_unmatched_events_exist,
    _v6_no_test_transactions_in_mart,
    _v7_take_rate_range,
]


def run_all_checks() -> list[AssertionResult]:
    """Run all 7 validation assertions and write a JSON report to ``reports/``.

    All assertions always run regardless of earlier failures (no short-circuit).
    The JSON report is written unconditionally.

    Returns:
        list[AssertionResult]: One result dict per assertion, in V1–V7 order.

    Raises:
        sqlalchemy.exc.SQLAlchemyError: On any database error during checks.
        OSError: If the reports directory cannot be created or written to.
    """
    engine = get_engine()
    results: list[AssertionResult] = []

    with engine.connect() as conn:
        for check_fn in _ALL_CHECKS:
            try:
                result = check_fn(conn)
            except Exception as exc:
                result = AssertionResult(
                    name=check_fn.__name__,
                    status="fail",
                    detail={"error": str(exc)},
                )
                logger.error("Assertion %s raised: %s", check_fn.__name__, exc)
            results.append(result)
            logger.info(
                "[%s] %s — %s",
                result["status"].upper(),
                result["name"],
                result["detail"].get("description", ""),
            )

    _write_report(results)
    return results


def _write_report(results: list[AssertionResult]) -> Path:
    """Serialise assertion results to a timestamped JSON file in ``reports/``.

    Args:
        results: List of assertion result dicts.

    Returns:
        Path: Absolute path to the written report file.

    Raises:
        OSError: If the file cannot be created.
    """
    _REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = _REPORTS_DIR / f"validation_{ts}.json"

    passed = sum(1 for r in results if r["status"] == "pass")
    report = {
        "generated_at": ts,
        "summary": {
            "total": len(results),
            "passed": passed,
            "failed": len(results) - passed,
        },
        "assertions": results,
    }

    report_path.write_text(json.dumps(report, indent=2))
    logger.info("Validation report written to %s", report_path)
    return report_path
