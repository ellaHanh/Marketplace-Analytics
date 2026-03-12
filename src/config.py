"""Configuration module for the Marketplace Analytics & FP&A Sandbox.

Loads DATABASE_URL from the environment (or a .env file) and all other
parameters from config/settings.yaml.  Import ``settings`` from this module
everywhere — never read yaml or os.environ directly.

Usage:
    from src.config import settings

    db_url  = settings.database_url
    seed    = settings.seeds.faker_seed
    brands  = settings.scale.num_brands
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


_YAML_PATH = Path(__file__).parent.parent / "config" / "settings.yaml"


def _load_yaml() -> dict[str, Any]:
    """Load and return the raw settings.yaml as a plain dict.

    Returns:
        dict: Parsed YAML contents.

    Raises:
        FileNotFoundError: If settings.yaml does not exist at the expected path.
        yaml.YAMLError: If the file is not valid YAML.
    """
    if not _YAML_PATH.exists():
        raise FileNotFoundError(f"settings.yaml not found at {_YAML_PATH}")
    with _YAML_PATH.open() as fh:
        return yaml.safe_load(fh)


# ---------------------------------------------------------------------------
# Nested models
# ---------------------------------------------------------------------------


from pydantic import BaseModel  # noqa: E402 (after stdlib imports)


class DatabaseConfig(BaseModel):
    """Database-level configuration (schema name only; URL comes from env)."""

    schema_name: str = Field(alias="schema", default="public")

    model_config = {"populate_by_name": True}


class SeedsConfig(BaseModel):
    """Random-number seeds for fully deterministic runs."""

    faker_seed: int = 42
    numpy_seed: int = 42
    python_random_seed: int = 42


class ScaleConfig(BaseModel):
    """Dataset-size knobs."""

    num_brands: int = 400
    num_creators: int = 3000
    months: int = 15
    start_date: str = "2023-01-01"


class FeesConfig(BaseModel):
    """Platform and payment-processor fee rates."""

    platform_take_rate: float = 0.10
    stripe_percentage: float = 0.029
    stripe_fixed_cents: int = 30

    @field_validator("platform_take_rate", "stripe_percentage")
    @classmethod
    def must_be_fraction(cls, v: float) -> float:
        """Validate rate is in (0, 1)."""
        if not (0 < v < 1):
            raise ValueError(f"Rate must be between 0 and 1, got {v}")
        return v


class InjectionRatesConfig(BaseModel):
    """Probabilities that control messy-data injection per row."""

    missing_brand_external_id: float = 0.03
    duplicate_event_id: float = 0.02
    null_campaign_id: float = 0.02
    partial_refund_on_succeeded: float = 0.05
    status_case_drift: float = 0.10
    payout_mismatch: float = 0.05
    unresolvable_entity: float = 0.01
    timezone_drift: float = 0.05


class DistributionsConfig(BaseModel):
    """Parameters governing statistical distributions used in generation."""

    gmv_pareto_alpha: float = 1.16
    payout_delay_mean_days: float = 8.0
    payout_delay_std_days: float = 5.0
    payout_delay_min_days: int = 1
    payout_delay_max_days: int = 45
    campaigns_per_creator_lambda: float = 3.0
    installment_payment_rate: float = 0.35


# ---------------------------------------------------------------------------
# Top-level settings
# ---------------------------------------------------------------------------


class Settings(BaseSettings):
    """Root settings object.  All config lives here.

    DATABASE_URL is read from the environment / .env file.  Every other
    parameter is sourced from config/settings.yaml and exposed as typed nested
    models.

    Attributes:
        database_url: Full SQLAlchemy-compatible connection string.
        database: Schema-level settings.
        seeds: RNG seeds for deterministic generation.
        scale: Dataset-size parameters.
        fees: Fee rates.
        injection_rates: Messy-data injection probabilities.
        distributions: Statistical distribution parameters.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = Field(
        default="postgresql://user:pass@localhost:5432/marketplace",
        alias="DATABASE_URL",
    )

    # Populated from yaml in model_post_init
    database: DatabaseConfig = DatabaseConfig()
    seeds: SeedsConfig = SeedsConfig()
    scale: ScaleConfig = ScaleConfig()
    fees: FeesConfig = FeesConfig()
    injection_rates: InjectionRatesConfig = InjectionRatesConfig()
    distributions: DistributionsConfig = DistributionsConfig()

    def model_post_init(self, __context: Any) -> None:
        """Load yaml after pydantic initialises env-sourced fields."""
        raw = _load_yaml()
        object.__setattr__(self, "database", DatabaseConfig(**raw.get("database", {})))
        object.__setattr__(self, "seeds", SeedsConfig(**raw.get("seeds", {})))
        object.__setattr__(self, "scale", ScaleConfig(**raw.get("scale", {})))
        object.__setattr__(self, "fees", FeesConfig(**raw.get("fees", {})))
        object.__setattr__(
            self,
            "injection_rates",
            InjectionRatesConfig(**raw.get("injection_rates", {})),
        )
        object.__setattr__(
            self,
            "distributions",
            DistributionsConfig(**raw.get("distributions", {})),
        )


settings: Settings = Settings()
