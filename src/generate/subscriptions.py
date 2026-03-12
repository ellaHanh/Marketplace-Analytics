"""Subscription event generator for the Marketplace Analytics & FP&A Sandbox.

Produces a list of raw subscription event dicts matching the
``raw_subscription_events`` schema (clean — before injection).

Logic summary (per plan §1.2):
    1. Decide if each brand subscribes based on tier probability.
    2. Assign plan & billing period consistent with tier.
    3. Emit ``subscription_created`` in month 1 or 2 of the window.
    4. Emit monthly ``renewal`` events until churn.
    5. Churn sampled each month using tier-specific monthly churn rates.
    6. On churn → emit ``cancellation``.
    7. 15% of SMB brands receive an ``upgrade`` or ``downgrade`` at month 4–6.
    8. Q1/Q4 seasonality boosts new subscription creation rate by 20%.

Usage:
    from src.generate.subscriptions import generate_subscription_events
    events = generate_subscription_events(brands)
"""

from __future__ import annotations

import hashlib
import logging
import random
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np

from src.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tier-level configuration constants
# ---------------------------------------------------------------------------

_SUBSCRIPTION_PROBABILITY: dict[str, float] = {
    "Enterprise": 0.80,
    "Mid-Market": 0.60,
    "SMB": 0.40,
}

# Primary plan assignment per tier; 15% crossover handled below
_PLAN_PRIMARY: dict[str, str] = {
    "Enterprise": "enterprise",
    "Mid-Market": "growth",
    "SMB": "starter",
}

# Billing period ratios: (annual_probability) per tier
_ANNUAL_PROB: dict[str, float] = {
    "Enterprise": 0.70,
    "Mid-Market": 0.45,
    "SMB": 0.25,
}

# Monthly churn rates per tier
_MONTHLY_CHURN_RATE: dict[str, float] = {
    "Enterprise": 0.01,
    "Mid-Market": 0.03,
    "SMB": 0.05,
}

# Plan → monthly cost in cents
_PLAN_MONTHLY_CENTS: dict[str, int] = {
    "starter": 50_000,     # $500/mo
    "growth": 200_000,     # $2,000/mo
    "enterprise": 800_000, # $8,000/mo
}

_UPGRADE_DOWNGRADE_PLANS: dict[str, dict[str, str]] = {
    "starter": {"upgrade": "growth", "downgrade": "starter"},
    "growth": {"upgrade": "enterprise", "downgrade": "starter"},
    "enterprise": {"upgrade": "enterprise", "downgrade": "growth"},
}

_CROSSOVER_RATE = 0.15
_SEASONALITY_BOOST = 0.20
_Q1_Q4_MONTHS = {1, 2, 3, 10, 11, 12}
_UPGRADE_DOWNGRADE_RATE = 0.15


def _month_start(base: date, offset_months: int) -> date:
    """Return the first day of the month ``offset_months`` after ``base``.

    Args:
        base: Reference date (its year/month are used).
        offset_months: Non-negative integer month offset.

    Returns:
        date: First day of the target month.
    """
    month = base.month + offset_months
    year = base.year + (month - 1) // 12
    month = (month - 1) % 12 + 1
    return date(year, month, 1)


def _make_event(
    brand_external_id: str,
    event_type: str,
    plan_name: str,
    billing_period: str,
    amount_cents: int,
    event_at: datetime,
    seq: int,
) -> dict[str, Any]:
    """Build a single raw subscription event dict.

    Args:
        brand_external_id: Brand identifier string.
        event_type: One of the allowed event types.
        plan_name: Subscription plan name.
        billing_period: ``'monthly'`` or ``'annual'``.
        amount_cents: Billed amount for this event.
        event_at: Timestamp of the event (UTC).
        seq: Monotonic sequence integer used to generate a unique event_id.

    Returns:
        dict: Row matching the ``raw_subscription_events`` schema.
    """
    raw_event_id = hashlib.sha256(
        f"{brand_external_id}:{event_type}:{event_at.isoformat()}:{seq}".encode()
    ).hexdigest()[:32]
    return {
        "raw_event_id": raw_event_id,
        "brand_external_id": brand_external_id,
        "event_type": event_type,
        "plan_name": plan_name,
        "billing_period": billing_period,
        "amount_cents": amount_cents,
        "event_at": event_at.isoformat(),
        "_tz_coerced": False,
    }


def generate_subscription_events(
    brands: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Generate all raw subscription events for the provided brand list.

    Args:
        brands: List of brand dicts as returned by ``generate_brands()``.
            Must contain keys ``brand_external_id``, ``tier``, ``created_at``.

    Returns:
        list[dict]: All subscription events (clean; injection not yet applied).

    Raises:
        ValueError: If ``brands`` is empty.
    """
    if not brands:
        raise ValueError("brands list must not be empty")

    rng = np.random.default_rng(settings.seeds.numpy_seed + 10)
    random.seed(settings.seeds.python_random_seed + 10)

    start_date = date.fromisoformat(settings.scale.start_date)
    total_months: int = settings.scale.months

    events: list[dict[str, Any]] = []
    seq = 0

    for brand in brands:
        ext_id: str = brand["brand_external_id"]
        tier: str = brand["tier"]
        sub_prob: float = _SUBSCRIPTION_PROBABILITY[tier]

        # Q1/Q4 seasonality boost for new subscription creation
        subscription_month_offset = int(rng.integers(0, 2))  # month 0 or 1
        creation_month_start = _month_start(start_date, subscription_month_offset)
        creation_month = creation_month_start.month
        if creation_month in _Q1_Q4_MONTHS:
            sub_prob = min(1.0, sub_prob * (1 + _SEASONALITY_BOOST))

        if rng.random() > sub_prob:
            continue  # Brand does not subscribe

        # Plan assignment with 15% crossover
        if rng.random() < _CROSSOVER_RATE:
            all_plans = list(_PLAN_MONTHLY_CENTS.keys())
            plan = rng.choice([p for p in all_plans if p != _PLAN_PRIMARY[tier]])
        else:
            plan = _PLAN_PRIMARY[tier]

        billing_period = "annual" if rng.random() < _ANNUAL_PROB[tier] else "monthly"
        monthly_cents = _PLAN_MONTHLY_CENTS[plan]
        amount_cents = monthly_cents * 12 if billing_period == "annual" else monthly_cents

        # subscription_created event — mid-first-month timestamp
        creation_day = int(rng.integers(1, 15))
        event_at = datetime(
            creation_month_start.year,
            creation_month_start.month,
            creation_day,
            tzinfo=timezone.utc,
        )
        events.append(
            _make_event(ext_id, "subscription_created", plan, billing_period, amount_cents, event_at, seq)
        )
        seq += 1

        # Upgrade/downgrade for 15% of SMB brands at month 4–6
        upgrade_month: int | None = None
        if tier == "SMB" and rng.random() < _UPGRADE_DOWNGRADE_RATE:
            upgrade_month = int(rng.integers(4, 7))
            change_type = rng.choice(["upgrade", "downgrade"])
            new_plan = _UPGRADE_DOWNGRADE_PLANS[plan][change_type]
            new_monthly = _PLAN_MONTHLY_CENTS[new_plan]
            new_amount = new_monthly * 12 if billing_period == "annual" else new_monthly
            upgrade_date = _month_start(start_date, upgrade_month - 1)
            upgrade_at = datetime(upgrade_date.year, upgrade_date.month, 1, tzinfo=timezone.utc)
            events.append(
                _make_event(ext_id, change_type, new_plan, billing_period, new_amount, upgrade_at, seq)
            )
            seq += 1
            plan = new_plan
            monthly_cents = new_monthly
            amount_cents = new_amount

        # Monthly renewals + churn sampling
        churn_rate: float = _MONTHLY_CHURN_RATE[tier]
        churned = False
        for month_idx in range(1, total_months):
            current_month = _month_start(start_date, month_idx)
            if current_month < creation_month_start:
                continue
            if upgrade_month and month_idx < upgrade_month:
                continue

            # Churn check
            if rng.random() < churn_rate:
                churn_at = datetime(current_month.year, current_month.month, 1, tzinfo=timezone.utc)
                events.append(
                    _make_event(ext_id, "cancellation", plan, billing_period, amount_cents, churn_at, seq)
                )
                seq += 1
                churned = True
                break

            # Renewal
            renewal_day = int(rng.integers(1, 5))
            renewal_at = datetime(
                current_month.year, current_month.month, renewal_day, tzinfo=timezone.utc
            )
            events.append(
                _make_event(ext_id, "renewal", plan, billing_period, amount_cents, renewal_at, seq)
            )
            seq += 1

        if not churned:
            logger.debug("Brand %s active through end of window.", ext_id)

    logger.info("Generated %d subscription events for %d brands.", len(events), len(brands))
    return events
