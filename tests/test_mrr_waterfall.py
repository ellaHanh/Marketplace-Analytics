"""Tests for the MRR waterfall mart builder.

T1 — Monthly plan: 3-month subscription, assert all waterfall columns and the
     continuity invariant.
T2 — Annual plan: pre-computed MRR from an annual subscription; assert
     Jan/Feb/Mar MRR = 10 000 cents, Apr shows churn movement.

Both tests insert directly into ``stg_subscriptions`` (bypassing the raw
staging pipeline) and monkeypatch ``src.db.get_engine`` so
``build_mrr_waterfall()`` targets the ephemeral test database.
"""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest
from sqlalchemy import Engine, text

from src.pipeline.mrr_waterfall import PipelineInvariantError, build_mrr_waterfall


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _insert_brand(conn: Any, brand_id_override: int, ext_id: str) -> None:
    """Insert a minimal dim_brand row with a known brand_id.

    Uses a sequence override trick: insert with an explicit brand_id by
    temporarily bypassing the serial default via nextval alignment.

    Args:
        conn: Open SQLAlchemy connection.
        brand_id_override: Desired brand_id value.
        ext_id: Unique brand_external_id string.
    """
    conn.execute(text(
        "INSERT INTO dim_brand (brand_id, brand_external_id, brand_name, industry, tier, created_at) "
        "VALUES (:bid, :ext, 'Test Brand', 'Retail', 'SMB', NOW())"
    ), {"bid": brand_id_override, "ext": ext_id})


def _insert_subscription(conn: Any, **kwargs: Any) -> None:
    """Insert a row directly into ``stg_subscriptions``.

    Args:
        conn: Open SQLAlchemy connection.
        **kwargs: Column values.  Required keys: brand_id, plan_name,
            billing_period, start_date, mrr_cents.  Optional: end_date,
            subscription_id.
    """
    conn.execute(text(
        """
        INSERT INTO stg_subscriptions
            (subscription_id, brand_id, plan_name, billing_period,
             start_date, end_date, mrr_cents, _source_event_ids)
        VALUES
            (:sub_id, :brand_id, :plan_name, :billing_period,
             :start_date, :end_date, :mrr_cents, ARRAY[1::BIGINT])
        """
    ), {
        "sub_id": kwargs.get("subscription_id", f"test-sub-{kwargs['brand_id']}"),
        "brand_id": kwargs["brand_id"],
        "plan_name": kwargs["plan_name"],
        "billing_period": kwargs["billing_period"],
        "start_date": kwargs["start_date"],
        "end_date": kwargs.get("end_date"),
        "mrr_cents": kwargs["mrr_cents"],
    })


def _fetch_waterfall(engine: Engine, brand_id: int) -> dict[date, dict[str, int]]:
    """Fetch mart_monthly_subscriptions for a brand as {month_start: row_dict}.

    Args:
        engine: SQLAlchemy engine connected to the test DB.
        brand_id: Brand to filter on.

    Returns:
        dict: Keyed by month_start_date, values are column dicts.
    """
    with engine.connect() as conn:
        rows = conn.execute(text(
            """
            SELECT month_start_date,
                   mrr_start_cents, mrr_new_cents, mrr_expansion_cents,
                   mrr_contraction_cents, mrr_churned_cents, mrr_end_cents
            FROM mart_monthly_subscriptions
            WHERE brand_id = :bid
            ORDER BY month_start_date
            """
        ), {"bid": brand_id}).fetchall()
    return {
        row[0]: {
            "mrr_start_cents": row[1],
            "mrr_new_cents": row[2],
            "mrr_expansion_cents": row[3],
            "mrr_contraction_cents": row[4],
            "mrr_churned_cents": row[5],
            "mrr_end_cents": row[6],
        }
        for row in rows
    }


def _invariant_violations(engine: Engine) -> int:
    """Return the number of MRR continuity invariant violations.

    Args:
        engine: SQLAlchemy engine connected to the test DB.

    Returns:
        int: Number of rows where mrr_end[t] != mrr_start[t+1].
    """
    with engine.connect() as conn:
        return conn.execute(text(
            """
            SELECT COUNT(*) FROM (
                SELECT mrr_end_cents,
                       LEAD(mrr_start_cents) OVER (
                           PARTITION BY brand_id ORDER BY month_start_date
                       ) AS next_start
                FROM mart_monthly_subscriptions
            ) t
            WHERE mrr_end_cents != next_start AND next_start IS NOT NULL
            """
        )).scalar() or 0


# ---------------------------------------------------------------------------
# T1 — Monthly plan
# ---------------------------------------------------------------------------


def test_t1_mrr_waterfall_monthly_plan(use_test_db: Engine) -> None:
    """T1: 3-month monthly subscription produces correct waterfall with churn row.

    Setup: one brand with a starter monthly subscription (mrr_cents=50 000)
    from 2023-01-01 to 2023-03-31.

    Asserts:
        - Jan row: mrr_new=50000, mrr_end=50000, start=0, no churn/expansion
        - Feb row: mrr_start=50000, mrr_end=50000, no movements
        - Mar row: mrr_start=50000, mrr_end=50000, no movements
        - Apr row: mrr_churned=50000, mrr_end=0 (churn month)
        - Continuity invariant: 0 violations
        - Exactly 4 waterfall rows for this brand
    """
    engine = use_test_db
    brand_id = 101
    mrr = 50_000

    with engine.begin() as conn:
        _insert_brand(conn, brand_id, "BRD-T1-001")
        _insert_subscription(
            conn,
            brand_id=brand_id,
            plan_name="starter",
            billing_period="monthly",
            start_date=date(2023, 1, 1),
            end_date=date(2023, 3, 31),
            mrr_cents=mrr,
        )

    build_mrr_waterfall()

    wf = _fetch_waterfall(engine, brand_id)
    assert len(wf) == 4, f"Expected 4 waterfall rows, got {len(wf)}: {sorted(wf)}"

    jan = wf[date(2023, 1, 1)]
    assert jan["mrr_start_cents"] == 0
    assert jan["mrr_new_cents"] == mrr
    assert jan["mrr_expansion_cents"] == 0
    assert jan["mrr_contraction_cents"] == 0
    assert jan["mrr_churned_cents"] == 0
    assert jan["mrr_end_cents"] == mrr

    for month_start in [date(2023, 2, 1), date(2023, 3, 1)]:
        row = wf[month_start]
        assert row["mrr_start_cents"] == mrr
        assert row["mrr_new_cents"] == 0
        assert row["mrr_expansion_cents"] == 0
        assert row["mrr_contraction_cents"] == 0
        assert row["mrr_churned_cents"] == 0
        assert row["mrr_end_cents"] == mrr

    apr = wf[date(2023, 4, 1)]
    assert apr["mrr_start_cents"] == mrr
    assert apr["mrr_churned_cents"] == mrr
    assert apr["mrr_end_cents"] == 0
    assert apr["mrr_new_cents"] == 0

    assert _invariant_violations(engine) == 0


# ---------------------------------------------------------------------------
# T2 — Annual plan
# ---------------------------------------------------------------------------


def test_t2_mrr_waterfall_annual_plan(use_test_db: Engine) -> None:
    """T2: Annual subscription with pre-computed MRR; cancelled at end of month 3.

    An annual subscription at $1 200/year → $100/month → 10 000 cents/month MRR.
    The subscription is cancelled at the end of March 2023.

    Asserts:
        - Jan/Feb/Mar rows: mrr_end_cents = 10 000
        - Apr row: mrr_churned_cents = 10 000, mrr_end_cents = 0
        - Continuity invariant: 0 violations
    """
    engine = use_test_db
    brand_id = 102
    mrr = 10_000  # $1200/year ÷ 12 = $100/month = 10 000 cents

    with engine.begin() as conn:
        _insert_brand(conn, brand_id, "BRD-T2-001")
        _insert_subscription(
            conn,
            subscription_id="test-sub-t2-annual",
            brand_id=brand_id,
            plan_name="enterprise",
            billing_period="annual",
            start_date=date(2023, 1, 1),
            end_date=date(2023, 3, 31),
            mrr_cents=mrr,
        )

    build_mrr_waterfall()

    wf = _fetch_waterfall(engine, brand_id)

    for month_start in [date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)]:
        assert wf[month_start]["mrr_end_cents"] == mrr, (
            f"Expected mrr_end_cents={mrr} for {month_start}, "
            f"got {wf[month_start]['mrr_end_cents']}"
        )

    apr = wf[date(2023, 4, 1)]
    assert apr["mrr_end_cents"] == 0
    assert apr["mrr_churned_cents"] == mrr

    assert _invariant_violations(engine) == 0
