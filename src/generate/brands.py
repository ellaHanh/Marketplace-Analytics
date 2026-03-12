"""Brand dimension generator for the Marketplace Analytics & FP&A Sandbox.

Produces a list of brand dicts matching the ``dim_brand`` schema.  All
randomness is controlled by seeds from ``settings`` so every run with the same
config produces identical output.

Usage (called by main.py via loader):
    from src.generate.brands import generate_brands
    brands = generate_brands()   # list[dict]
"""

from __future__ import annotations

import logging
import random
from datetime import datetime, timezone, timedelta
from typing import Any

import numpy as np
from faker import Faker

from src.config import settings

logger = logging.getLogger(__name__)

# Industry distribution: 40/25/20/15
_INDUSTRIES = ["Retail", "Beauty & Wellness", "Tech & SaaS", "Food & Beverage"]
_INDUSTRY_WEIGHTS = [0.40, 0.25, 0.20, 0.15]

# Tier distribution: 60/30/10
_TIERS = ["SMB", "Mid-Market", "Enterprise"]
_TIER_WEIGHTS = [0.60, 0.30, 0.10]


def generate_brands() -> list[dict[str, Any]]:
    """Generate synthetic ``dim_brand`` rows.

    Brands are assigned a Salesforce-style external ID, a random industry,
    and a tier.  Their ``created_at`` is spread across the 6 months *before*
    the generation ``start_date`` to simulate pre-existing accounts.

    Returns:
        list[dict]: One dict per brand, keys matching the ``dim_brand``
            column names (excluding the serial ``brand_id``).

    Raises:
        ValueError: If ``num_brands`` is not a positive integer.
    """
    n: int = settings.scale.num_brands
    if n <= 0:
        raise ValueError(f"num_brands must be positive, got {n}")

    faker_seed: int = settings.seeds.faker_seed
    rng = np.random.default_rng(settings.seeds.numpy_seed)
    random.seed(settings.seeds.python_random_seed)

    fake = Faker()
    Faker.seed(faker_seed)

    start_dt = datetime.fromisoformat(settings.scale.start_date).replace(
        tzinfo=timezone.utc
    )
    # Brands exist up to 6 months before generation window starts
    pre_window_start = start_dt - timedelta(days=6 * 30)
    pre_window_days = (start_dt - pre_window_start).days  # ~180

    industries = rng.choice(
        _INDUSTRIES,
        size=n,
        p=_INDUSTRY_WEIGHTS,
        replace=True,
    ).tolist()
    tiers = rng.choice(
        _TIERS,
        size=n,
        p=_TIER_WEIGHTS,
        replace=True,
    ).tolist()

    brands: list[dict[str, Any]] = []
    for i in range(n):
        offset_days = int(rng.integers(0, pre_window_days))
        created_at = pre_window_start + timedelta(days=offset_days)

        brands.append(
            {
                "brand_external_id": f"BRD-{i + 1:06d}",
                "brand_name": fake.company(),
                "industry": industries[i],
                "tier": tiers[i],
                "created_at": created_at.isoformat(),
            }
        )

    logger.info("Generated %d brand rows.", len(brands))
    return brands
