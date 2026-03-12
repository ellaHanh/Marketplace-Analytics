"""Creator dimension generator for the Marketplace Analytics & FP&A Sandbox.

Produces a list of creator dicts matching the ``dim_creator`` schema.
``created_at`` values are spread across the first 3 months of the generation
window to simulate creators joining the platform over time.

Usage:
    from src.generate.creators import generate_creators
    creators = generate_creators()   # list[dict]
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

# Follower-tier distribution: 50/30/15/5
_FOLLOWER_TIERS = ["nano", "micro", "macro", "mega"]
_FOLLOWER_WEIGHTS = [0.50, 0.30, 0.15, 0.05]

# Category: uniform across 5 categories
_CATEGORIES = ["beauty", "gaming", "lifestyle", "food", "fitness"]
_CATEGORY_WEIGHTS = [0.20, 0.20, 0.20, 0.20, 0.20]


def generate_creators() -> list[dict[str, Any]]:
    """Generate synthetic ``dim_creator`` rows.

    Args:
        None (all parameters from ``settings``).

    Returns:
        list[dict]: One dict per creator, keys matching the ``dim_creator``
            column names (excluding the serial ``creator_id``).

    Raises:
        ValueError: If ``num_creators`` is not a positive integer.
    """
    n: int = settings.scale.num_creators
    if n <= 0:
        raise ValueError(f"num_creators must be positive, got {n}")

    rng = np.random.default_rng(settings.seeds.numpy_seed + 1)  # distinct sub-seed
    random.seed(settings.seeds.python_random_seed + 1)

    fake = Faker()
    Faker.seed(settings.seeds.faker_seed + 1)

    start_dt = datetime.fromisoformat(settings.scale.start_date).replace(
        tzinfo=timezone.utc
    )
    # Creators join over the first 3 months of the window
    onboard_window_days = 3 * 30

    follower_tiers = rng.choice(
        _FOLLOWER_TIERS,
        size=n,
        p=_FOLLOWER_WEIGHTS,
        replace=True,
    ).tolist()
    categories = rng.choice(
        _CATEGORIES,
        size=n,
        p=_CATEGORY_WEIGHTS,
        replace=True,
    ).tolist()

    creators: list[dict[str, Any]] = []
    for i in range(n):
        offset_days = int(rng.integers(0, onboard_window_days))
        created_at = start_dt + timedelta(days=offset_days)

        creators.append(
            {
                "creator_external_id": f"CRT-{i + 1:08d}",
                "creator_name": fake.name(),
                "follower_tier": follower_tiers[i],
                "category": categories[i],
                "created_at": created_at.isoformat(),
            }
        )

    logger.info("Generated %d creator rows.", len(creators))
    return creators
