"""Marketplace Analytics & FP&A Sandbox — Orchestrator.

Runs the full pipeline end-to-end:

    Step 1: Load and validate config
    Step 2: Reset database (only with --reset flag)
    Step 3: Generate raw synthetic data
    Step 4: Run staging pipeline
    Step 5: Run mart pipeline
    Step 6: Run validation suite
    Step 7: Launch Streamlit dashboard (only with --dashboard flag)

Usage:
    python main.py                    # run full pipeline, keep existing DB
    python main.py --reset            # drop + recreate DB, then run pipeline
    python main.py --reset --dashboard  # reset, run, open dashboard
    python main.py --help
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Callable

# ---------------------------------------------------------------------------
# Logging setup (before any src imports so all child loggers inherit format)
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")


def _step(name: str, fn: Callable[[], None]) -> float:
    """Execute a pipeline step with start/end logging and elapsed-time tracking.

    Args:
        name: Human-readable step label (appears in log output).
        fn: Zero-argument callable that performs the step.

    Returns:
        float: Elapsed wall-clock seconds.

    Raises:
        Exception: Re-raises any exception from ``fn`` after logging.
    """
    logger.info("▶ %s …", name)
    t0 = time.perf_counter()
    try:
        fn()
    except Exception as exc:
        logger.error("✗ %s failed: %s", name, exc)
        raise
    elapsed = time.perf_counter() - t0
    logger.info("✓ %s  (%.1fs)", name, elapsed)
    return elapsed


def _parse_args() -> argparse.Namespace:
    """Parse CLI arguments.

    Returns:
        argparse.Namespace: Parsed flags.
    """
    parser = argparse.ArgumentParser(
        prog="main.py",
        description="Marketplace Analytics & FP&A Sandbox pipeline orchestrator.",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Drop and recreate the database before running the pipeline.",
    )
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Launch the Streamlit dashboard after the pipeline completes.",
    )
    return parser.parse_args()


def main() -> None:
    """Run the full pipeline, respecting CLI flags for optional steps."""
    args = _parse_args()
    pipeline_start = time.perf_counter()
    logger.info("=" * 60)
    logger.info("Marketplace Analytics Pipeline — %s", datetime.now().isoformat(sep=" ", timespec="seconds"))
    logger.info("=" * 60)

    # ------------------------------------------------------------------
    # Step 1: Load config
    # ------------------------------------------------------------------
    def load_config() -> None:
        from src.config import settings  # noqa: F401 — validates on import
        logger.info(
            "Config OK — %d brands, %d creators, %d months from %s",
            settings.scale.num_brands,
            settings.scale.num_creators,
            settings.scale.months,
            settings.scale.start_date,
        )

    _step("Step 1: Load config", load_config)

    # ------------------------------------------------------------------
    # Step 2: Reset database (optional)
    # ------------------------------------------------------------------
    if args.reset:
        def reset_db() -> None:
            from src.db import reset_database
            reset_database()

        _step("Step 2: Reset database", reset_db)
    else:
        logger.info("⏭  Step 2: Reset database skipped (use --reset to enable)")

    # ------------------------------------------------------------------
    # Step 3: Generate raw data
    # ------------------------------------------------------------------
    def generate_data() -> None:
        from src.generate.brands import generate_brands
        from src.generate.creators import generate_creators
        from src.generate.subscriptions import generate_subscription_events
        from src.generate.campaigns import generate_campaigns
        from src.generate.payments import generate_payments
        from src.generate.payouts import generate_payouts
        from src.generate.injectors import run_all_injectors
        from src.generate.loader import load_all

        brands = generate_brands()
        creators = generate_creators()
        events = generate_subscription_events(brands)
        campaigns = generate_campaigns(brands, creators)
        payments = generate_payments(campaigns)
        payouts = generate_payouts(payments)

        events, payments, payouts = run_all_injectors(events, payments, payouts)

        load_all(brands, creators, campaigns, payments, payouts, events)

    _step("Step 3: Generate raw data", generate_data)

    # ------------------------------------------------------------------
    # Step 4: Run staging pipeline
    # ------------------------------------------------------------------
    def run_staging() -> None:
        from src.pipeline.staging import run_staging_pipeline
        run_staging_pipeline()

    _step("Step 4: Run staging pipeline", run_staging)

    # ------------------------------------------------------------------
    # Step 5: Run mart pipeline
    # ------------------------------------------------------------------
    def run_marts() -> None:
        from src.pipeline.marts import build_daily_financials
        from src.pipeline.mrr_waterfall import build_mrr_waterfall
        build_daily_financials()
        build_mrr_waterfall()

    _step("Step 5: Run mart pipeline", run_marts)

    # ------------------------------------------------------------------
    # Step 6: Run validation
    # ------------------------------------------------------------------
    def run_validation() -> None:
        from src.validate.checks import run_all_checks
        results = run_all_checks()
        failed = [r for r in results if r["status"] == "fail"]
        for r in results:
            icon = "✓" if r["status"] == "pass" else "✗"
            logger.info("%s  %s — %s", icon, r["name"], r.get("detail", ""))
        if failed:
            logger.error("%d validation assertion(s) failed.", len(failed))
            sys.exit(1)

    _step("Step 6: Run validation", run_validation)

    # ------------------------------------------------------------------
    # Step 7: Dashboard (optional)
    # ------------------------------------------------------------------
    if args.dashboard:
        logger.info("▶ Step 7: Launch dashboard …")
        dashboard_path = Path(__file__).parent / "src" / "dashboard" / "app.py"
        subprocess.run(["streamlit", "run", str(dashboard_path)], check=True)
    else:
        logger.info("⏭  Step 7: Dashboard skipped (use --dashboard to enable)")

    total_elapsed = time.perf_counter() - pipeline_start
    logger.info("=" * 60)
    logger.info("Pipeline complete in %.1fs", total_elapsed)
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
