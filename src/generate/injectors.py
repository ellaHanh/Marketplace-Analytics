"""Messy data injectors for the Marketplace Analytics & FP&A Sandbox.

Runs *after* all generators produce clean data, mutating rows in-place to
simulate real-world data quality issues.  Each injector is independent and
rate-controlled from ``settings.injection_rates``.  Injectors run in a fixed
order to avoid conflicting mutations on the same row.

Public interface:
    run_all_injectors(events, payments, payouts) -> (events, payments, payouts)

Individual injectors are also importable for targeted unit testing.
"""

from __future__ import annotations

import logging
import random
from typing import Any

import numpy as np

from src.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sample_indices(rng: np.random.Generator, n: int, rate: float) -> set[int]:
    """Return a set of unique indices sampled at ``rate`` from ``range(n)``.

    Args:
        rng: NumPy random generator.
        n: Total population size.
        rate: Fraction of rows to select; clamped to [0, 1].

    Returns:
        set[int]: Selected indices (may be empty).
    """
    count = int(round(n * max(0.0, min(1.0, rate))))
    if count == 0 or n == 0:
        return set()
    return set(rng.choice(n, size=count, replace=False).tolist())


# ---------------------------------------------------------------------------
# Individual injectors
# ---------------------------------------------------------------------------


def inject_missing_brand_id(
    events: list[dict[str, Any]],
    rng: np.random.Generator,
    rate: float,
) -> int:
    """Set ``brand_external_id = None`` on a sample of subscription events.

    Args:
        events: List of subscription event dicts (mutated in-place).
        rng: NumPy random generator.
        rate: Fraction of rows to affect.

    Returns:
        int: Number of rows mutated.
    """
    indices = _sample_indices(rng, len(events), rate)
    for i in indices:
        events[i]["brand_external_id"] = None
    logger.info("[inject_missing_brand_id] mutated %d / %d rows", len(indices), len(events))
    return len(indices)


def inject_duplicate_events(
    events: list[dict[str, Any]],
    rng: np.random.Generator,
    rate: float,
) -> int:
    """Copy a sample of subscription event rows with identical ``raw_event_id``.

    Args:
        events: List of subscription event dicts (extended in-place).
        rng: NumPy random generator.
        rate: Fraction of rows to duplicate.

    Returns:
        int: Number of duplicate rows appended.
    """
    indices = _sample_indices(rng, len(events), rate)
    duplicates = [dict(events[i]) for i in indices]
    events.extend(duplicates)
    logger.info(
        "[inject_duplicate_events] duplicated %d / %d rows (list now %d)",
        len(indices), len(indices) + len(events) - len(duplicates), len(events),
    )
    return len(duplicates)


def inject_null_campaign_id(
    payments: list[dict[str, Any]],
    rng: np.random.Generator,
    rate: float,
) -> int:
    """Set ``campaign_idx = None`` on a sample of payments (marks test transactions).

    Args:
        payments: List of payment dicts (mutated in-place).
        rng: NumPy random generator.
        rate: Fraction of rows to affect.

    Returns:
        int: Number of rows mutated.
    """
    indices = _sample_indices(rng, len(payments), rate)
    for i in indices:
        payments[i]["campaign_idx"] = None
    logger.info("[inject_null_campaign_id] mutated %d / %d rows", len(indices), len(payments))
    return len(indices)


def inject_partial_refunds(
    payments: list[dict[str, Any]],
    rng: np.random.Generator,
    rate: float,
) -> int:
    """Set ``amount_refunded_cents`` to 10–50% of gross on succeeded payments.

    Args:
        payments: List of payment dicts (mutated in-place).
        rng: NumPy random generator.
        rate: Fraction of *succeeded* rows to affect.

    Returns:
        int: Number of rows mutated.
    """
    succeeded_indices = [i for i, p in enumerate(payments) if p["status"] == "succeeded"]
    target_indices = _sample_indices(rng, len(succeeded_indices), rate)
    actual_indices = [succeeded_indices[i] for i in target_indices]
    for i in actual_indices:
        gross = payments[i]["amount_gross_cents"]
        refund_fraction = float(rng.uniform(0.10, 0.51))
        payments[i]["amount_refunded_cents"] = int(round(gross * refund_fraction))
    logger.info("[inject_partial_refunds] mutated %d / %d succeeded rows", len(actual_indices), len(succeeded_indices))
    return len(actual_indices)


def inject_status_case_drift(
    payments: list[dict[str, Any]],
    rng: np.random.Generator,
    rate: float,
) -> int:
    """Replace ``'succeeded'`` with ``'Succeeded'`` on a sample of payments.

    Args:
        payments: List of payment dicts (mutated in-place).
        rng: NumPy random generator.
        rate: Fraction of rows to affect.

    Returns:
        int: Number of rows mutated.
    """
    indices = _sample_indices(rng, len(payments), rate)
    count = 0
    for i in indices:
        if payments[i]["status"] == "succeeded":
            payments[i]["status"] = "Succeeded"
            count += 1
    logger.info("[inject_status_case_drift] mutated %d rows", count)
    return count


def inject_payout_mismatch(
    payments: list[dict[str, Any]],
    payouts: list[dict[str, Any]],
    rng: np.random.Generator,
    rate: float,
) -> int:
    """Reduce ``amount_paid_cents`` to reflect refund delta on a sample of payouts.

    Only affects payouts whose linked payment has ``amount_refunded_cents > 0``.

    Args:
        payments: List of payment dicts (read-only reference).
        payouts: List of payout dicts (mutated in-place); index-aligned with payments.
        rng: NumPy random generator.
        rate: Fraction of eligible rows to affect.

    Returns:
        int: Number of rows mutated.
    """
    eligible = [
        i for i, p in enumerate(payouts)
        if i < len(payments) and payments[i]["amount_refunded_cents"] > 0
    ]
    target_indices = _sample_indices(rng, len(eligible), rate)
    actual_indices = [eligible[i] for i in target_indices]
    for i in actual_indices:
        refund = payments[i]["amount_refunded_cents"]
        payouts[i]["amount_paid_cents"] = max(0, payouts[i]["amount_paid_cents"] - refund)
    logger.info("[inject_payout_mismatch] mutated %d eligible payout rows", len(actual_indices))
    return len(actual_indices)


def inject_unresolvable_entities(
    payments: list[dict[str, Any]],
    payouts: list[dict[str, Any]],
    rng: np.random.Generator,
    rate: float,
) -> int:
    """Set brand/creator external IDs to non-existent values on a sample of rows.

    Args:
        payments: List of payment dicts (mutated in-place).
        payouts: List of payout dicts (mutated in-place).
        rng: NumPy random generator.
        rate: Fraction of combined rows to affect.

    Returns:
        int: Number of rows mutated across both lists.
    """
    all_rows: list[dict[str, Any]] = payments + payouts
    indices = _sample_indices(rng, len(all_rows), rate)
    for i in indices:
        all_rows[i]["brand_external_id"] = "BRD-GHOST"
    logger.info("[inject_unresolvable_entities] mutated %d rows", len(indices))
    return len(indices)


def inject_timezone_drift(
    events: list[dict[str, Any]],
    rng: np.random.Generator,
    rate: float,
) -> int:
    """Strip timezone info from ``event_at`` and set ``_tz_coerced = True``.

    Args:
        events: List of subscription event dicts (mutated in-place).
        rng: NumPy random generator.
        rate: Fraction of rows to affect.

    Returns:
        int: Number of rows mutated.
    """
    indices = _sample_indices(rng, len(events), rate)
    for i in indices:
        raw = events[i].get("event_at", "")
        if raw and "+" in raw:
            events[i]["event_at"] = raw.split("+")[0]  # strip tz offset
        elif raw and raw.endswith("Z"):
            events[i]["event_at"] = raw[:-1]  # strip trailing Z
        events[i]["_tz_coerced"] = True
    logger.info("[inject_timezone_drift] mutated %d / %d rows", len(indices), len(events))
    return len(indices)


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------


def run_all_injectors(
    events: list[dict[str, Any]],
    payments: list[dict[str, Any]],
    payouts: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Run all injectors in the fixed order specified by the plan.

    All rates are read from ``settings.injection_rates``.  Each injector logs
    the number of rows it mutated.

    Args:
        events: Subscription events (from ``generate_subscription_events``).
        payments: Payment rows (from ``generate_payments``).
        payouts: Payout rows (from ``generate_payouts``).

    Returns:
        tuple: ``(events, payments, payouts)`` — the same list objects, mutated
            in-place and returned for convenience.
    """
    rng = np.random.default_rng(settings.seeds.numpy_seed + 99)
    rates = settings.injection_rates

    inject_missing_brand_id(events, rng, rates.missing_brand_external_id)
    inject_duplicate_events(events, rng, rates.duplicate_event_id)
    inject_null_campaign_id(payments, rng, rates.null_campaign_id)
    inject_partial_refunds(payments, rng, rates.partial_refund_on_succeeded)
    inject_status_case_drift(payments, rng, rates.status_case_drift)
    inject_payout_mismatch(payments, payouts, rng, rates.payout_mismatch)
    inject_unresolvable_entities(payments, payouts, rng, rates.unresolvable_entity)
    inject_timezone_drift(events, rng, rates.timezone_drift)

    logger.info(
        "Injection complete — events: %d, payments: %d, payouts: %d",
        len(events), len(payments), len(payouts),
    )
    return events, payments, payouts
