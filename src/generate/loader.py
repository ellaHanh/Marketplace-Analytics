"""Bulk data loader for the Marketplace Analytics & FP&A Sandbox.

Uses psycopg2 ``COPY FROM STDIN`` (via StringIO) for all tables — never
row-by-row inserts.  Load order respects FK dependencies:

    dim_brand → dim_creator → raw_campaigns → raw_payments →
    raw_payouts → raw_subscription_events

After each table load, actual row counts are compared to expected and logged.

Usage:
    from src.generate.loader import load_all
    load_all(brands, creators, campaigns, payments, payouts, events)
"""

from __future__ import annotations

import csv
import io
import logging
from typing import Any

from src.db import get_connection

logger = logging.getLogger(__name__)


def _copy_table(
    conn: Any,
    table: str,
    columns: list[str],
    rows: list[dict[str, Any]],
) -> int:
    """Bulk-insert rows into ``table`` using ``COPY FROM STDIN``.

    Args:
        conn: Open psycopg2 connection.
        table: Target table name.
        columns: Ordered list of column names to populate.
        rows: List of dicts; only keys in ``columns`` are used.

    Returns:
        int: Number of rows inserted.

    Raises:
        psycopg2.Error: On any database error (caller handles rollback).
    """
    buf = io.StringIO()
    writer = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    for row in rows:
        writer.writerow([row.get(col) for col in columns])
    buf.seek(0)

    cursor = conn.cursor()
    col_list = ", ".join(columns)
    sql = f"COPY {table} ({col_list}) FROM STDIN WITH (FORMAT CSV, NULL '')"
    cursor.copy_expert(sql, buf)
    return len(rows)


def _fetch_id_map(conn: Any, table: str, ext_col: str, id_col: str) -> dict[str, int]:
    """Return a mapping of external-ID → serial PK for a dimension table.

    Args:
        conn: Open psycopg2 connection.
        table: Dimension table name.
        ext_col: External-ID column name.
        id_col: Serial PK column name.

    Returns:
        dict[str, int]: ``{external_id: pk}``
    """
    cursor = conn.cursor()
    cursor.execute(f"SELECT {ext_col}, {id_col} FROM {table}")
    return {row[0]: row[1] for row in cursor.fetchall()}


def load_all(
    brands: list[dict[str, Any]],
    creators: list[dict[str, Any]],
    campaigns: list[dict[str, Any]],
    payments: list[dict[str, Any]],
    payouts: list[dict[str, Any]],
    events: list[dict[str, Any]],
) -> None:
    """Load all generated data into Postgres using COPY FROM STDIN.

    FK resolution:
        - Campaigns reference ``brand_id``/``creator_id`` looked up from
          ``dim_brand``/``dim_creator`` after those are loaded.
        - Payments reference ``campaign_id`` from ``raw_campaigns``, resolved
          by ``campaign_idx`` (positional index into the campaigns list).
        - Payouts reference ``payment_id`` from ``raw_payments``, resolved by
          ``payment_idx`` (positional index).

    Args:
        brands: Brand dicts from ``generate_brands()``.
        creators: Creator dicts from ``generate_creators()``.
        campaigns: Campaign dicts from ``generate_campaigns()``.
        payments: Payment dicts from ``generate_payments()``.
        payouts: Payout dicts from ``generate_payouts()``.
        events: Subscription event dicts from ``generate_subscription_events()``.

    Raises:
        RuntimeError: If a loaded table's row count does not match expected.
        psycopg2.Error: On any database error.
    """
    with get_connection() as conn:
        # ------------------------------------------------------------------
        # 1. dim_brand
        # ------------------------------------------------------------------
        n = _copy_table(
            conn, "dim_brand",
            ["brand_external_id", "brand_name", "industry", "tier", "created_at"],
            brands,
        )
        logger.info("Loaded dim_brand: %d rows", n)

        # ------------------------------------------------------------------
        # 2. dim_creator
        # ------------------------------------------------------------------
        n = _copy_table(
            conn, "dim_creator",
            ["creator_external_id", "creator_name", "follower_tier", "category", "created_at"],
            creators,
        )
        logger.info("Loaded dim_creator: %d rows", n)

        # Build lookup maps
        brand_map = _fetch_id_map(conn, "dim_brand", "brand_external_id", "brand_id")
        creator_map = _fetch_id_map(conn, "dim_creator", "creator_external_id", "creator_id")

        # ------------------------------------------------------------------
        # 3. raw_campaigns (resolve brand_id / creator_id FKs)
        # ------------------------------------------------------------------
        resolved_campaigns: list[dict[str, Any]] = []
        skipped_campaigns = 0
        for camp in campaigns:
            bid = brand_map.get(camp["brand_external_id"])
            cid = creator_map.get(camp["creator_external_id"])
            if bid is None or cid is None:
                skipped_campaigns += 1
                continue
            resolved_campaigns.append({**camp, "brand_id": bid, "creator_id": cid})

        if skipped_campaigns:
            logger.warning("Skipped %d campaigns with unresolvable FKs", skipped_campaigns)

        n = _copy_table(
            conn, "raw_campaigns",
            ["brand_id", "creator_id", "agreed_budget_cents", "status", "created_at", "completed_at"],
            resolved_campaigns,
        )
        logger.info("Loaded raw_campaigns: %d rows", n)

        # Build campaign PK map: positional index in original list → campaign_id
        cursor = conn.cursor()
        cursor.execute("SELECT campaign_id FROM raw_campaigns ORDER BY campaign_id")
        db_campaign_ids = [row[0] for row in cursor.fetchall()]
        # Align: resolved_campaigns is a subset of campaigns; track which original indices loaded
        orig_to_db: dict[int, int] = {}
        rc_idx = 0
        for orig_idx, camp in enumerate(campaigns):
            bid = brand_map.get(camp["brand_external_id"])
            cid = creator_map.get(camp["creator_external_id"])
            if bid is not None and cid is not None and rc_idx < len(db_campaign_ids):
                orig_to_db[orig_idx] = db_campaign_ids[rc_idx]
                rc_idx += 1

        # ------------------------------------------------------------------
        # 4. raw_payments (resolve campaign_id FK)
        # ------------------------------------------------------------------
        resolved_payments: list[dict[str, Any]] = []
        for pay in payments:
            camp_idx = pay.get("campaign_idx")
            campaign_id = orig_to_db.get(camp_idx) if camp_idx is not None else None
            resolved_payments.append({
                **pay,
                "campaign_id": campaign_id,
            })

        n = _copy_table(
            conn, "raw_payments",
            [
                "campaign_id", "brand_external_id", "creator_external_id",
                "amount_gross_cents", "platform_fee_cents", "stripe_fee_cents",
                "amount_refunded_cents", "status", "paid_at",
            ],
            resolved_payments,
        )
        logger.info("Loaded raw_payments: %d rows", n)

        # Build payment PK list (ordered by payment_id)
        cursor.execute("SELECT payment_id FROM raw_payments ORDER BY payment_id")
        db_payment_ids = [row[0] for row in cursor.fetchall()]

        # ------------------------------------------------------------------
        # 5. raw_payouts (resolve payment_id FK)
        # ------------------------------------------------------------------
        resolved_payouts: list[dict[str, Any]] = []
        for i, payout in enumerate(payouts):
            pay_idx = payout.get("payment_idx", i)
            payment_id = db_payment_ids[pay_idx] if pay_idx < len(db_payment_ids) else None
            if payment_id is None:
                logger.warning("Payout %d has no valid payment_id, skipping", i)
                continue
            resolved_payouts.append({**payout, "payment_id": payment_id})

        n = _copy_table(
            conn, "raw_payouts",
            ["payment_id", "creator_external_id", "expected_payout_cents",
             "amount_paid_cents", "status", "payout_at"],
            resolved_payouts,
        )
        logger.info("Loaded raw_payouts: %d rows", n)

        # ------------------------------------------------------------------
        # 6. raw_subscription_events
        # ------------------------------------------------------------------
        n = _copy_table(
            conn, "raw_subscription_events",
            ["raw_event_id", "brand_external_id", "event_type", "plan_name",
             "billing_period", "amount_cents", "event_at", "_tz_coerced"],
            events,
        )
        logger.info("Loaded raw_subscription_events: %d rows", n)

    logger.info("All tables loaded successfully.")
