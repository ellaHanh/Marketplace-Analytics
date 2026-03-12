"""Payout generator for the Marketplace Analytics & FP&A Sandbox.

Produces a list of raw payout dicts in a 1:1 relationship with payments.
Payout delay follows a log-normal distribution clipped to [min, max] days.
3% of payouts are ``failed``, 5% are ``pending``, the rest are ``paid``.

Usage:
    from src.generate.payouts import generate_payouts
    payouts = generate_payouts(payments)
"""

from __future__ import annotations

import logging
import math
import random
from datetime import datetime, timezone, timedelta
from typing import Any

import numpy as np

from src.config import settings

logger = logging.getLogger(__name__)

_STATUS_PROBS = [("failed", 0.03), ("pending", 0.05)]  # remainder = paid


def generate_payouts(
    payments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Generate synthetic ``raw_payouts`` rows.

    Each payment maps to exactly one payout.  The expected payout amount is
    ``amount_gross - platform_fee - stripe_fee``.  Payout delay is sampled
    from LogNormal(μ, σ) and clipped to the configured min/max days.

    Args:
        payments: Payment dicts from ``generate_payments()``.  Must contain
            ``amount_gross_cents``, ``platform_fee_cents``, ``stripe_fee_cents``,
            ``paid_at``, and ``creator_external_id``.

    Returns:
        list[dict]: Payout dicts (1:1 with payments, in the same order) with
            keys matching the non-serial columns of ``raw_payouts``.  The
            positional index ``payment_idx`` is used by the loader as a FK.

    Raises:
        ValueError: If ``payments`` is empty.
    """
    if not payments:
        raise ValueError("payments list must not be empty")

    rng = np.random.default_rng(settings.seeds.numpy_seed + 40)
    random.seed(settings.seeds.python_random_seed + 40)

    dist = settings.distributions
    mu: float = dist.payout_delay_mean_days
    sigma: float = dist.payout_delay_std_days
    delay_min: int = dist.payout_delay_min_days
    delay_max: int = dist.payout_delay_max_days

    # Convert mean/std to log-normal parameters
    variance = sigma ** 2
    lognorm_mu = math.log(mu ** 2 / math.sqrt(variance + mu ** 2))
    lognorm_sigma = math.sqrt(math.log(1 + variance / mu ** 2))

    delays = rng.lognormal(mean=lognorm_mu, sigma=lognorm_sigma, size=len(payments))
    delays = np.clip(delays, delay_min, delay_max).astype(int)

    payouts: list[dict[str, Any]] = []

    for payment_idx, (payment, delay_days) in enumerate(zip(payments, delays)):
        gross = payment["amount_gross_cents"]
        platform_fee = payment["platform_fee_cents"]
        stripe_fee = payment["stripe_fee_cents"]
        expected_payout = gross - platform_fee - stripe_fee

        paid_at = datetime.fromisoformat(payment["paid_at"]).replace(tzinfo=timezone.utc)
        payout_at = paid_at + timedelta(days=int(delay_days))

        # Status sampling
        r = rng.random()
        cumulative = 0.0
        status = "paid"
        for s, prob in _STATUS_PROBS:
            cumulative += prob
            if r < cumulative:
                status = s
                break

        payouts.append(
            {
                "payment_idx": payment_idx,         # resolved to FK by loader
                "creator_external_id": payment["creator_external_id"],
                "expected_payout_cents": expected_payout,
                "amount_paid_cents": expected_payout,   # overridden by injector for mismatches
                "status": status,
                "payout_at": payout_at.isoformat(),
            }
        )

    logger.info("Generated %d payout rows (1:1 with payments).", len(payouts))
    return payouts
