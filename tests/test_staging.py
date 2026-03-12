"""Tests for the staging pipeline and mart exclusion of test transactions.

T5 — Status case normalisation: 10 payments with mixed-case status are staged;
     all rows in stg_payments must have LOWER(status).
T6 — Quarantine: a subscription event with NULL brand_external_id is routed to
     stg_unmatched_events; stg_subscriptions receives 0 rows.
T7 — Test transaction exclusion: 5 payments (1 with campaign_id IS NULL) are
     staged, ledgered, and mart-built.  The null-campaign payment must not
     appear in mart_daily_financials.gmv_cents.  V6 assertion is run explicitly.
"""

from __future__ import annotations

from typing import Any

import pytest
from sqlalchemy import Engine, text

from src.pipeline.staging import stage_payments, stage_subscriptions
from src.pipeline.staging import run_staging_pipeline
from src.pipeline.marts import build_daily_financials
from src.validate.checks import _v6_no_test_transactions_in_mart


# ---------------------------------------------------------------------------
# Shared insert helpers
# ---------------------------------------------------------------------------


def _insert_dim_brand(conn: Any, brand_id: int, ext_id: str) -> None:
    conn.execute(text(
        "INSERT INTO dim_brand (brand_id, brand_external_id, brand_name, industry, tier, created_at) "
        "VALUES (:bid, :ext, 'Test Brand', 'Retail', 'SMB', NOW())"
    ), {"bid": brand_id, "ext": ext_id})


def _insert_dim_creator(conn: Any, creator_id: int, ext_id: str) -> None:
    conn.execute(text(
        "INSERT INTO dim_creator (creator_id, creator_external_id, creator_name, "
        "follower_tier, category, created_at) "
        "VALUES (:cid, :ext, 'Test Creator', 'nano', 'beauty', NOW())"
    ), {"cid": creator_id, "ext": ext_id})


def _insert_raw_campaign(conn: Any, campaign_id_hint: int, brand_id: int, creator_id: int) -> int:
    """Insert a completed raw_campaigns row and return the generated campaign_id."""
    result = conn.execute(text(
        """
        INSERT INTO raw_campaigns
            (brand_id, creator_id, agreed_budget_cents, status, created_at, completed_at)
        VALUES
            (:bid, :cid, 100000, 'completed', NOW(), NOW())
        RETURNING campaign_id
        """
    ), {"bid": brand_id, "cid": creator_id})
    return result.scalar()


def _insert_raw_payment(
    conn: Any,
    *,
    campaign_id: int | None,
    brand_ext: str | None,
    creator_ext: str | None,
    amount: int = 100_000,
    status: str = "succeeded",
) -> int:
    """Insert a raw_payments row and return the generated payment_id."""
    result = conn.execute(text(
        """
        INSERT INTO raw_payments
            (campaign_id, brand_external_id, creator_external_id,
             amount_gross_cents, platform_fee_cents, stripe_fee_cents,
             amount_refunded_cents, status, paid_at)
        VALUES
            (:campaign_id, :brand_ext, :creator_ext,
             :amount, :platform_fee, :stripe_fee,
             0, :status, '2023-06-15 00:00:00+00')
        RETURNING payment_id
        """
    ), {
        "campaign_id": campaign_id,
        "brand_ext": brand_ext,
        "creator_ext": creator_ext,
        "amount": amount,
        "platform_fee": int(amount * 0.10),
        "stripe_fee": int(amount * 0.029) + 30,
        "status": status,
    })
    return result.scalar()


def _insert_raw_payout(conn: Any, payment_id: int, amount: int) -> None:
    conn.execute(text(
        """
        INSERT INTO raw_payouts
            (payment_id, creator_external_id, expected_payout_cents,
             amount_paid_cents, status, payout_at)
        VALUES
            (:pid, NULL, :expected, :paid, 'paid', '2023-06-25 00:00:00+00')
        """
    ), {"pid": payment_id, "expected": amount, "paid": amount})


# ---------------------------------------------------------------------------
# T5 — Status case normalisation
# ---------------------------------------------------------------------------


def test_t5_status_case_normalisation(schema: Engine) -> None:
    """T5: 10 payments (1 with 'Succeeded') → stg_payments all have lower(status).

    All 10 payments use campaign_id=NULL (test transactions) to avoid needing
    entity resolution.  The test verifies LOWER(TRIM(status)) normalisation.

    Asserts:
        - stg_payments contains 10 rows.
        - 0 rows have status != LOWER(status) after staging.
        - The originally-drifted 'Succeeded' row now reads 'succeeded'.
    """
    statuses = ["succeeded"] * 9 + ["Succeeded"]

    payment_ids = []
    with schema.begin() as conn:
        for status in statuses:
            pid = _insert_raw_payment(
                conn,
                campaign_id=None,
                brand_ext=None,
                creator_ext=None,
                status=status,
            )
            payment_ids.append(pid)

        stage_payments(conn)

        row_count = conn.execute(text("SELECT COUNT(*) FROM stg_payments")).scalar()
        bad_count = conn.execute(text(
            "SELECT COUNT(*) FROM stg_payments WHERE status != LOWER(status)"
        )).scalar()
        status_values = conn.execute(text(
            "SELECT DISTINCT status FROM stg_payments"
        )).scalars().all()

    assert row_count == 10, f"Expected 10 staged rows, got {row_count}"
    assert bad_count == 0, (
        f"{bad_count} rows still have non-lowercase status after staging"
    )
    assert set(status_values) == {"succeeded"}, (
        f"Unexpected status values: {status_values}"
    )


# ---------------------------------------------------------------------------
# T6 — Quarantine for NULL brand_external_id
# ---------------------------------------------------------------------------


def test_t6_null_brand_quarantined(schema: Engine) -> None:
    """T6: Subscription event with NULL brand_external_id is quarantined.

    Inserts one raw_subscription_events row with brand_external_id=NULL.
    After running stage_subscriptions, the event must NOT be in stg_subscriptions
    and MUST be in stg_unmatched_events with reason='missing_brand_external_id'.

    Asserts:
        - stg_subscriptions row count == 0.
        - stg_unmatched_events row count == 1.
        - Quarantine reason is 'missing_brand_external_id'.
    """
    with schema.begin() as conn:
        conn.execute(text(
            """
            INSERT INTO raw_subscription_events
                (raw_event_id, brand_external_id, event_type, plan_name,
                 billing_period, amount_cents, event_at, _tz_coerced)
            VALUES
                ('evt-null-brand-001', NULL, 'subscription_created', 'starter',
                 'monthly', 50000, '2023-01-15 00:00:00+00', FALSE)
            """
        ))

        stage_subscriptions(conn)

        sub_count = conn.execute(text("SELECT COUNT(*) FROM stg_subscriptions")).scalar()
        unmatched_count = conn.execute(text("SELECT COUNT(*) FROM stg_unmatched_events")).scalar()
        reasons = conn.execute(text(
            "SELECT reason FROM stg_unmatched_events"
        )).scalars().all()

    assert sub_count == 0, (
        f"Expected 0 rows in stg_subscriptions, got {sub_count}"
    )
    assert unmatched_count == 1, (
        f"Expected 1 quarantined row, got {unmatched_count}"
    )
    assert reasons == ["missing_brand_external_id"], (
        f"Unexpected quarantine reason(s): {reasons}"
    )


# ---------------------------------------------------------------------------
# T7 — Test transaction excluded from mart GMV; V6 assertion passes
# ---------------------------------------------------------------------------


def test_t7_test_transaction_excluded_from_mart(use_test_db: Engine) -> None:
    """T7: NULL-campaign payment is excluded from mart GMV; V6 assertion passes.

    Inserts 5 raw_payments (4 linked to a campaign, 1 with campaign_id IS NULL).
    Runs the full staging + mart pipeline.  The NULL-campaign payment must not
    contribute to mart_daily_financials.gmv_cents, and the V6 validation
    assertion must pass.

    Asserts:
        - mart_daily_financials.gmv_cents equals exactly the sum of the 4
          non-test payments (400 000 cents).
        - V6 assertion status is 'pass'.
    """
    engine = use_test_db
    brand_ext = "BRD-T7-001"
    creator_ext = "CRT-T7-001"
    non_test_amount = 100_000
    test_amount = 999_999  # must NOT appear in mart GMV

    with engine.begin() as conn:
        _insert_dim_brand(conn, brand_id=201, ext_id=brand_ext)
        _insert_dim_creator(conn, creator_id=301, ext_id=creator_ext)
        campaign_id = _insert_raw_campaign(conn, 1, brand_id=201, creator_id=301)

        # 4 non-test payments linked to the campaign
        non_test_ids = []
        for _ in range(4):
            pid = _insert_raw_payment(
                conn,
                campaign_id=campaign_id,
                brand_ext=brand_ext,
                creator_ext=creator_ext,
                amount=non_test_amount,
            )
            non_test_ids.append(pid)
            payout = non_test_amount - int(non_test_amount * 0.10) - (int(non_test_amount * 0.029) + 30)
            _insert_raw_payout(conn, pid, payout)

        # 1 test transaction (campaign_id IS NULL)
        _insert_raw_payment(
            conn,
            campaign_id=None,
            brand_ext=None,
            creator_ext=None,
            amount=test_amount,
        )

    # Full pipeline: staging → ledger → mart
    run_staging_pipeline()
    build_daily_financials()

    expected_gmv = non_test_amount * 4  # 400 000

    with engine.connect() as conn:
        mart_gmv = conn.execute(text(
            "SELECT COALESCE(SUM(gmv_cents), 0) FROM mart_daily_financials"
        )).scalar()

        v6_result = _v6_no_test_transactions_in_mart(conn)

    assert int(mart_gmv) == expected_gmv, (
        f"Mart GMV {mart_gmv} != expected {expected_gmv}. "
        f"Test transaction amount {test_amount} may have leaked into the mart."
    )
    assert v6_result["status"] == "pass", (
        f"V6 assertion failed: {v6_result['detail']}"
    )
