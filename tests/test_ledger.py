"""Tests for the ledger fan-out pipeline step.

T3 — Single payment with no refund: assert exactly 4 ledger rows with
     correct entry types and signed amounts.
T4 — Single payment with a partial refund: assert exactly 5 ledger rows;
     the ``refund_adjustment`` entry must be negative.

Both tests bypass the staging pipeline by hand-inserting into ``raw_payments``,
manually creating and populating ``stg_payments``, and inserting a linked
``raw_payouts`` row.  The ``stg_ledger_entries`` table is created by schema.sql.
"""

from __future__ import annotations

from datetime import timezone
from typing import Any

import pytest
from sqlalchemy import Engine, text

from src.pipeline.ledger import build_ledger


# ---------------------------------------------------------------------------
# Test data constants
# ---------------------------------------------------------------------------

_PAID_AT = "2023-06-15T00:00:00+00:00"
_PAYOUT_AT = "2023-06-25T00:00:00+00:00"
_GROSS = 100_000
_PLATFORM_FEE = 10_000
_STRIPE_FEE = 3_200
_EXPECTED_PAYOUT = _GROSS - _PLATFORM_FEE - _STRIPE_FEE  # 86 800
_REFUND = 20_000


# ---------------------------------------------------------------------------
# Setup helpers
# ---------------------------------------------------------------------------


def _insert_raw_payment(conn: Any, *, amount_refunded: int = 0) -> int:
    """Insert one ``raw_payments`` row and return the generated payment_id.

    Args:
        conn: Open SQLAlchemy connection.
        amount_refunded: Amount refunded in cents (0 for T3, > 0 for T4).

    Returns:
        int: The auto-generated payment_id.
    """
    result = conn.execute(text(
        """
        INSERT INTO raw_payments
            (campaign_id, brand_external_id, creator_external_id,
             amount_gross_cents, platform_fee_cents, stripe_fee_cents,
             amount_refunded_cents, status, paid_at)
        VALUES
            (NULL, NULL, NULL,
             :gross, :platform_fee, :stripe_fee,
             :refund, 'succeeded', :paid_at)
        RETURNING payment_id
        """
    ), {
        "gross": _GROSS,
        "platform_fee": _PLATFORM_FEE,
        "stripe_fee": _STRIPE_FEE,
        "refund": amount_refunded,
        "paid_at": _PAID_AT,
    })
    return result.scalar()


def _create_stg_payments(conn: Any) -> None:
    """Create the transient stg_payments table if it does not exist.

    Args:
        conn: Open SQLAlchemy connection.
    """
    conn.execute(text(
        """
        CREATE TABLE IF NOT EXISTS stg_payments (
            payment_id            BIGINT      NOT NULL,
            campaign_id           BIGINT,
            brand_id              INTEGER,
            creator_id            INTEGER,
            amount_gross_cents    BIGINT      NOT NULL,
            platform_fee_cents    BIGINT      NOT NULL,
            stripe_fee_cents      BIGINT      NOT NULL,
            amount_refunded_cents BIGINT      NOT NULL DEFAULT 0,
            status                TEXT        NOT NULL,
            paid_at               TIMESTAMPTZ NOT NULL,
            is_test_transaction   BOOLEAN     NOT NULL DEFAULT FALSE
        )
        """
    ))


def _insert_stg_payment(conn: Any, payment_id: int, *, amount_refunded: int = 0) -> None:
    """Insert a corresponding row into the hand-created stg_payments table.

    is_test_transaction is set to FALSE so the ledger fan-out processes it.

    Args:
        conn: Open SQLAlchemy connection.
        payment_id: FK matching the raw_payments row.
        amount_refunded: Amount refunded in cents.
    """
    conn.execute(text(
        """
        INSERT INTO stg_payments
            (payment_id, campaign_id, brand_id, creator_id,
             amount_gross_cents, platform_fee_cents, stripe_fee_cents,
             amount_refunded_cents, status, paid_at, is_test_transaction)
        VALUES
            (:pid, NULL, NULL, NULL,
             :gross, :platform_fee, :stripe_fee,
             :refund, 'succeeded', :paid_at, FALSE)
        """
    ), {
        "pid": payment_id,
        "gross": _GROSS,
        "platform_fee": _PLATFORM_FEE,
        "stripe_fee": _STRIPE_FEE,
        "refund": amount_refunded,
        "paid_at": _PAID_AT,
    })


def _insert_raw_payout(conn: Any, payment_id: int) -> None:
    """Insert a paid raw_payout linked to the given payment.

    Args:
        conn: Open SQLAlchemy connection.
        payment_id: FK matching the raw_payments row.
    """
    conn.execute(text(
        """
        INSERT INTO raw_payouts
            (payment_id, creator_external_id, expected_payout_cents,
             amount_paid_cents, status, payout_at)
        VALUES
            (:pid, NULL, :expected, :paid, 'paid', :payout_at)
        """
    ), {
        "pid": payment_id,
        "expected": _EXPECTED_PAYOUT,
        "paid": _EXPECTED_PAYOUT,
        "payout_at": _PAYOUT_AT,
    })


def _fetch_ledger(conn: Any, payment_id: int) -> dict[str, int]:
    """Return a mapping of entry_type → amount_cents for one payment.

    Args:
        conn: Open SQLAlchemy connection.
        payment_id: Payment to filter on.

    Returns:
        dict[str, int]: entry_type → amount_cents.
    """
    rows = conn.execute(text(
        "SELECT entry_type, amount_cents FROM stg_ledger_entries WHERE payment_id = :pid"
    ), {"pid": payment_id}).fetchall()
    return {r[0]: r[1] for r in rows}


# ---------------------------------------------------------------------------
# T3 — No refund: expect exactly 4 ledger rows
# ---------------------------------------------------------------------------


def test_t3_ledger_no_refund(schema: Engine) -> None:
    """T3: A succeeded payment with no refund produces exactly 4 ledger rows.

    Expected rows:
        brand_charge          +100 000
        platform_fee_revenue  + 10 000
        stripe_processing_fee -  3 200
        creator_payout        - 86 800

    Asserts:
        - Exactly 4 rows in stg_ledger_entries for this payment.
        - Each entry has the correct sign and amount.
        - No refund_adjustment row is present.
    """
    with schema.begin() as conn:
        payment_id = _insert_raw_payment(conn, amount_refunded=0)
        _create_stg_payments(conn)
        _insert_stg_payment(conn, payment_id, amount_refunded=0)
        _insert_raw_payout(conn, payment_id)

        build_ledger(conn)
        ledger = _fetch_ledger(conn, payment_id)

    assert len(ledger) == 4, f"Expected 4 ledger entries, got {len(ledger)}: {list(ledger)}"

    assert ledger["brand_charge"] == _GROSS
    assert ledger["platform_fee_revenue"] == _PLATFORM_FEE
    assert ledger["stripe_processing_fee"] == -_STRIPE_FEE
    assert ledger["creator_payout"] == -_EXPECTED_PAYOUT
    assert "refund_adjustment" not in ledger


# ---------------------------------------------------------------------------
# T4 — With refund: expect exactly 5 ledger rows
# ---------------------------------------------------------------------------


def test_t4_ledger_with_refund(schema: Engine) -> None:
    """T4: A succeeded payment with a partial refund produces exactly 5 ledger rows.

    The extra row is ``refund_adjustment`` with a negative amount equal to the
    refunded amount.

    Expected rows:
        brand_charge          +100 000
        platform_fee_revenue  + 10 000
        stripe_processing_fee -  3 200
        refund_adjustment     - 20 000
        creator_payout        - 86 800

    Asserts:
        - Exactly 5 rows in stg_ledger_entries for this payment.
        - refund_adjustment is negative (a cost entry).
        - All other entries have correct signs.
    """
    with schema.begin() as conn:
        payment_id = _insert_raw_payment(conn, amount_refunded=_REFUND)
        _create_stg_payments(conn)
        _insert_stg_payment(conn, payment_id, amount_refunded=_REFUND)
        _insert_raw_payout(conn, payment_id)

        build_ledger(conn)
        ledger = _fetch_ledger(conn, payment_id)

    assert len(ledger) == 5, f"Expected 5 ledger entries, got {len(ledger)}: {list(ledger)}"

    assert ledger["brand_charge"] == _GROSS
    assert ledger["platform_fee_revenue"] == _PLATFORM_FEE
    assert ledger["stripe_processing_fee"] == -_STRIPE_FEE
    assert ledger["refund_adjustment"] == -_REFUND, (
        f"refund_adjustment should be negative -{_REFUND}, got {ledger['refund_adjustment']}"
    )
    assert ledger["creator_payout"] == -_EXPECTED_PAYOUT
