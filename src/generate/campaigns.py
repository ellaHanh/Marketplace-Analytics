"""Campaign generator for the Marketplace Analytics & FP&A Sandbox.

Produces a list of raw campaign dicts matching the ``raw_campaigns`` schema
(clean — before injection).  Campaign counts per creator follow a Poisson
distribution; budgets are drawn from per-follower-tier uniform ranges.

Usage:
    from src.generate.campaigns import generate_campaigns
    campaigns = generate_campaigns(brands, creators)
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timezone, timedelta
from typing import Any

import numpy as np

from src.config import settings

logger = logging.getLogger(__name__)

# Agreed-budget ranges (cents) per follower tier
_BUDGET_RANGES: dict[str, tuple[int, int]] = {
    "nano":  (50_000,      200_000),
    "micro": (200_000,   1_000_000),
    "macro": (1_000_000, 5_000_000),
    "mega":  (5_000_000, 20_000_000),
}

# Enterprise brands run disproportionately more campaigns
_BRAND_TIER_CAMPAIGN_WEIGHT: dict[str, float] = {
    "SMB": 1.0,
    "Mid-Market": 2.0,
    "Enterprise": 4.0,
}

_STATUS_WEIGHTS = [0.85, 0.10, 0.05]  # completed, cancelled, active
_STATUSES = ["completed", "cancelled", "active"]


def generate_campaigns(
    brands: list[dict[str, Any]],
    creators: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Generate synthetic ``raw_campaigns`` rows.

    For each creator, sample a Poisson(λ) count of campaigns.  Each campaign
    is assigned to a brand weighted by tier and has status sampled from the
    85/10/5 distribution.

    Args:
        brands: Brand dicts from ``generate_brands()``.  Must contain
            ``brand_external_id`` and ``tier``.
        creators: Creator dicts from ``generate_creators()``.  Must contain
            ``creator_external_id``, ``follower_tier``, and ``created_at``.

    Returns:
        list[dict]: Campaign dicts with keys matching the non-serial columns
            of ``raw_campaigns`` plus ``brand_external_id`` and
            ``creator_external_id`` for FK resolution in the loader.

    Raises:
        ValueError: If either ``brands`` or ``creators`` is empty.
    """
    if not brands:
        raise ValueError("brands list must not be empty")
    if not creators:
        raise ValueError("creators list must not be empty")

    rng = np.random.default_rng(settings.seeds.numpy_seed + 20)
    random.seed(settings.seeds.python_random_seed + 20)

    lam: float = settings.distributions.campaigns_per_creator_lambda
    start_dt = datetime.fromisoformat(settings.scale.start_date).replace(
        tzinfo=timezone.utc
    )
    window_days = settings.scale.months * 30

    # Build brand index for weighted sampling
    brand_ext_ids = [b["brand_external_id"] for b in brands]
    brand_weights = np.array(
        [_BRAND_TIER_CAMPAIGN_WEIGHT[b["tier"]] for b in brands], dtype=float
    )
    brand_weights /= brand_weights.sum()

    campaigns: list[dict[str, Any]] = []

    for creator in creators:
        creator_ext_id: str = creator["creator_external_id"]
        follower_tier: str = creator["follower_tier"]
        creator_created = datetime.fromisoformat(creator["created_at"]).replace(
            tzinfo=timezone.utc
        )

        num_campaigns = int(rng.poisson(lam))
        if num_campaigns == 0:
            continue

        budget_lo, budget_hi = _BUDGET_RANGES[follower_tier]

        for _ in range(num_campaigns):
            brand_ext_id = str(rng.choice(brand_ext_ids, p=brand_weights))

            # Campaign created_at: after creator join date, within window
            earliest = max(start_dt, creator_created)
            remaining_days = max(1, window_days - (earliest - start_dt).days)
            offset_days = int(rng.integers(0, remaining_days))
            created_at = earliest + timedelta(days=offset_days)

            status = str(rng.choice(_STATUSES, p=_STATUS_WEIGHTS))

            agreed_budget_cents = int(rng.integers(budget_lo, budget_hi + 1))

            completed_at: str | None = None
            if status == "completed":
                completion_offset = int(rng.integers(7, 61))
                completed_at = (created_at + timedelta(days=completion_offset)).isoformat()

            campaigns.append(
                {
                    "brand_external_id": brand_ext_id,
                    "creator_external_id": creator_ext_id,
                    "agreed_budget_cents": agreed_budget_cents,
                    "status": status,
                    "created_at": created_at.isoformat(),
                    "completed_at": completed_at,
                }
            )

    logger.info(
        "Generated %d campaigns for %d creators (mean %.1f each).",
        len(campaigns),
        len(creators),
        len(campaigns) / max(len(creators), 1),
    )
    return campaigns
