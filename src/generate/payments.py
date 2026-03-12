"""Payment generator for the Marketplace Analytics & FP&A Sandbox.

Produces a list of raw payment dicts matching the ``raw_payments`` schema
(clean — before injection).  Each ``completed`` campaign generates 1 or 2
installment payments.  ``paid_at`` is clustered mid-month or end-of-month.

Usage:
    from src.generate.payments import generate_payments
    payments = generate_payments(campaigns)
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timezone, timedelta
from typing import Any

import numpy as np

from src.config import settings

logger = logging.getLogger(__name__)


def _cluster_paid_at(rng: np.random.Generator, base: datetime) -> datetime:
    """Return a ``paid_at`` timestamp clustered mid- or end-of-month (70% prob).

    Args:
        rng: NumPy random generator.
        base: Anchor datetime used for year and month.

    Returns:
        datetime: UTC timestamp on a clustered day within the same month.
    """
    if rng.random() < 0.70:
        # Mid-month: day 14–16 or end-of-month: day 28–31 (capped to actual month end)
        cluster = rng.choice(["mid", "end"])
        if cluster == "mid":
            day = int(rng.integers(14, 17))
        else:
            day = int(rng.integers(28, 32))

        # Clamp to valid calendar day for the given month
        import calendar
        _, last_day = calendar.monthrange(base.year, base.month)
        day = min(day, last_day)
        return datetime(base.year, base.month, day, tzinfo=timezone.utc)
    else:
        # Random day in same month
        import calendar
        _, last_day = calendar.monthrange(base.year, base.month)
        day = int(rng.integers(1, last_day + 1))
        return datetime(base.year, base.month, day, tzinfo=timezone.utc)


def generate_payments(
    campaigns: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Generate synthetic ``raw_payments`` rows for completed campaigns.

    Each completed campaign spawns 1 payment (65%) or 2 installments (35%).
    Fees are computed per the platform and Stripe rate constants in settings.

    Args:
        campaigns: Campaign dicts from ``generate_campaigns()``.  Only rows
            with ``status == 'completed'`` produce payments.

    Returns:
        list[dict]: Payment dicts with keys matching the non-serial columns of
            ``raw_payments`` (``campaign_id`` is stored as a positional index
            into the campaigns list; the loader resolves this to an actual PK).

    Raises:
        ValueError: If ``campaigns`` is empty.
    """
    if not campaigns:
        raise ValueError("campaigns list must not be empty")

    rng = np.random.default_rng(settings.seeds.numpy_seed + 30)
    random.seed(settings.seeds.python_random_seed + 30)

    take_rate: float = settings.fees.platform_take_rate
    stripe_pct: float = settings.fees.stripe_percentage
    stripe_fixed: int = settings.fees.stripe_fixed_cents
    installment_rate: float = settings.distributions.installment_payment_rate

    payments: list[dict[str, Any]] = []

    for campaign_idx, campaign in enumerate(campaigns):
        if campaign["status"] != "completed":
            continue

        budget: int = campaign["agreed_budget_cents"]
        completed_at_str: str = campaign["completed_at"]
        base_dt = datetime.fromisoformat(completed_at_str).replace(tzinfo=timezone.utc)

        brand_ext_id: str = campaign["brand_external_id"]
        creator_ext_id: str = campaign["creator_external_id"]

        def _make_payment(gross: int, paid_at: datetime) -> dict[str, Any]:
            platform_fee = int(round(gross * take_rate))
            stripe_fee = int(round(gross * stripe_pct)) + stripe_fixed
            return {
                "campaign_idx": campaign_idx,   # resolved to FK by loader
                "brand_external_id": brand_ext_id,
                "creator_external_id": creator_ext_id,
                "amount_gross_cents": gross,
                "platform_fee_cents": platform_fee,
                "stripe_fee_cents": stripe_fee,
                "amount_refunded_cents": 0,     # overridden by injector
                "status": "succeeded",
                "paid_at": paid_at.isoformat(),
            }

        if rng.random() < installment_rate:
            # Two-installment split
            split = float(rng.uniform(0.40, 0.70))
            first_gross = int(round(budget * split))
            second_gross = budget - first_gross

            first_paid_at = _cluster_paid_at(rng, base_dt)
            # Second installment 15–45 days later
            second_base = base_dt + timedelta(days=int(rng.integers(15, 46)))
            second_paid_at = _cluster_paid_at(rng, second_base)

            payments.append(_make_payment(first_gross, first_paid_at))
            payments.append(_make_payment(second_gross, second_paid_at))
        else:
            paid_at = _cluster_paid_at(rng, base_dt)
            payments.append(_make_payment(budget, paid_at))

    logger.info(
        "Generated %d payment rows from %d completed campaigns.",
        len(payments),
        sum(1 for c in campaigns if c["status"] == "completed"),
    )
    return payments
