-- =============================================================================
-- Marketplace Analytics & FP&A Sandbox — Schema DDL
-- Apply with: psql $DATABASE_URL -f sql/schema.sql
-- =============================================================================

-- ---------------------------------------------------------------------------
-- 1. dim_date
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_date (
    date_day        DATE        NOT NULL,
    year            SMALLINT    NOT NULL,
    quarter         SMALLINT    NOT NULL,
    month           SMALLINT    NOT NULL,
    week            SMALLINT    NOT NULL,
    day_of_week     SMALLINT    NOT NULL,    -- 0 = Sunday
    is_weekend      BOOLEAN     NOT NULL,
    month_start     DATE        NOT NULL,
    month_end       DATE        NOT NULL,
    CONSTRAINT pk_dim_date PRIMARY KEY (date_day)
);

-- ---------------------------------------------------------------------------
-- 2. dim_brand
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_brand (
    brand_id            SERIAL      NOT NULL,
    brand_external_id   VARCHAR(20) NOT NULL,
    brand_name          TEXT        NOT NULL,
    industry            TEXT        NOT NULL,
    tier                TEXT        NOT NULL
                            CHECK (tier IN ('SMB', 'Mid-Market', 'Enterprise')),
    created_at          TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_dim_brand PRIMARY KEY (brand_id),
    CONSTRAINT uq_dim_brand_external_id UNIQUE (brand_external_id)
);

CREATE INDEX IF NOT EXISTS idx_dim_brand_tier ON dim_brand (tier);

-- ---------------------------------------------------------------------------
-- 3. dim_creator
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_creator (
    creator_id              SERIAL      NOT NULL,
    creator_external_id     VARCHAR(24) NOT NULL,
    creator_name            TEXT        NOT NULL,
    follower_tier           TEXT        NOT NULL
                                CHECK (follower_tier IN ('nano', 'micro', 'macro', 'mega')),
    category                TEXT        NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_dim_creator PRIMARY KEY (creator_id),
    CONSTRAINT uq_dim_creator_external_id UNIQUE (creator_external_id)
);

CREATE INDEX IF NOT EXISTS idx_dim_creator_follower_tier ON dim_creator (follower_tier);

-- ---------------------------------------------------------------------------
-- 4. raw_subscription_events
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_subscription_events (
    event_id                BIGSERIAL   NOT NULL,
    raw_event_id            TEXT        NOT NULL,    -- may be duplicated (injected)
    brand_external_id       TEXT,                    -- nullable (injected)
    event_type              TEXT        NOT NULL
                                CHECK (event_type IN (
                                    'subscription_created', 'renewal',
                                    'cancellation', 'upgrade', 'downgrade'
                                )),
    plan_name               TEXT        NOT NULL,
    billing_period          TEXT        NOT NULL
                                CHECK (billing_period IN ('monthly', 'annual')),
    amount_cents            INTEGER     NOT NULL CHECK (amount_cents >= 0),
    event_at                TIMESTAMPTZ,             -- nullable after tz-strip injection
    _tz_coerced             BOOLEAN     NOT NULL DEFAULT FALSE,
    CONSTRAINT pk_raw_subscription_events PRIMARY KEY (event_id)
);

CREATE INDEX IF NOT EXISTS idx_rse_brand_ext_id   ON raw_subscription_events (brand_external_id);
CREATE INDEX IF NOT EXISTS idx_rse_event_type      ON raw_subscription_events (event_type);
CREATE INDEX IF NOT EXISTS idx_rse_event_at        ON raw_subscription_events (event_at);

-- ---------------------------------------------------------------------------
-- 5. raw_campaigns
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_campaigns (
    campaign_id         BIGSERIAL   NOT NULL,
    brand_id            INTEGER     NOT NULL REFERENCES dim_brand (brand_id),
    creator_id          INTEGER     NOT NULL REFERENCES dim_creator (creator_id),
    agreed_budget_cents BIGINT      NOT NULL CHECK (agreed_budget_cents > 0),
    status              TEXT        NOT NULL
                            CHECK (status IN ('active', 'completed', 'cancelled')),
    created_at          TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ,
    CONSTRAINT pk_raw_campaigns PRIMARY KEY (campaign_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_campaigns_brand_id   ON raw_campaigns (brand_id);
CREATE INDEX IF NOT EXISTS idx_raw_campaigns_creator_id ON raw_campaigns (creator_id);
CREATE INDEX IF NOT EXISTS idx_raw_campaigns_status     ON raw_campaigns (status);

-- ---------------------------------------------------------------------------
-- 6. raw_payments
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_payments (
    payment_id              BIGSERIAL   NOT NULL,
    campaign_id             BIGINT      REFERENCES raw_campaigns (campaign_id),  -- nullable (injected)
    brand_external_id       TEXT,                                                -- nullable (injected)
    creator_external_id     TEXT,                                                -- nullable (injected)
    amount_gross_cents      BIGINT      NOT NULL CHECK (amount_gross_cents > 0),
    platform_fee_cents      BIGINT      NOT NULL CHECK (platform_fee_cents >= 0),
    stripe_fee_cents        BIGINT      NOT NULL CHECK (stripe_fee_cents >= 0),
    amount_refunded_cents   BIGINT      NOT NULL DEFAULT 0
                                CHECK (amount_refunded_cents >= 0),
    status                  TEXT        NOT NULL,   -- raw; may have case drift
    paid_at                 TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_raw_payments PRIMARY KEY (payment_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_payments_campaign_id ON raw_payments (campaign_id);
CREATE INDEX IF NOT EXISTS idx_raw_payments_paid_at     ON raw_payments (paid_at);
CREATE INDEX IF NOT EXISTS idx_raw_payments_status      ON raw_payments (status);

-- ---------------------------------------------------------------------------
-- 7. raw_payouts
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_payouts (
    payout_id           BIGSERIAL   NOT NULL,
    payment_id          BIGINT      NOT NULL REFERENCES raw_payments (payment_id),
    creator_external_id TEXT,
    expected_payout_cents BIGINT    NOT NULL,
    amount_paid_cents   BIGINT      NOT NULL CHECK (amount_paid_cents >= 0),
    status              TEXT        NOT NULL
                            CHECK (status IN ('paid', 'failed', 'pending')),
    payout_at           TIMESTAMPTZ NOT NULL,
    CONSTRAINT pk_raw_payouts PRIMARY KEY (payout_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_payouts_payment_id ON raw_payouts (payment_id);
CREATE INDEX IF NOT EXISTS idx_raw_payouts_payout_at  ON raw_payouts (payout_at);

-- ---------------------------------------------------------------------------
-- 8. stg_subscriptions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_subscriptions (
    subscription_id     TEXT        NOT NULL,   -- SHA256(brand_id, plan_name, start_date)
    brand_id            INTEGER     NOT NULL REFERENCES dim_brand (brand_id),
    plan_name           TEXT        NOT NULL,
    billing_period      TEXT        NOT NULL,
    start_date          DATE        NOT NULL,
    end_date            DATE,                   -- NULL = still active
    mrr_cents           BIGINT      NOT NULL CHECK (mrr_cents > 0),
    _source_event_ids   BIGINT[]    NOT NULL,   -- contributing raw event_ids
    CONSTRAINT pk_stg_subscriptions PRIMARY KEY (subscription_id)
);

CREATE INDEX IF NOT EXISTS idx_stg_sub_brand_id    ON stg_subscriptions (brand_id);
CREATE INDEX IF NOT EXISTS idx_stg_sub_start_date  ON stg_subscriptions (start_date);
CREATE INDEX IF NOT EXISTS idx_stg_sub_end_date    ON stg_subscriptions (end_date);

-- ---------------------------------------------------------------------------
-- 9. stg_unmatched_events  (quarantine table)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_unmatched_events (
    unmatched_id    BIGSERIAL   NOT NULL,
    source_table    TEXT        NOT NULL,
    source_row_id   BIGINT      NOT NULL,
    reason          TEXT        NOT NULL,
    raw_payload     JSONB       NOT NULL,
    quarantined_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT pk_stg_unmatched_events PRIMARY KEY (unmatched_id)
);

CREATE INDEX IF NOT EXISTS idx_stg_unmatched_source ON stg_unmatched_events (source_table, reason);

-- ---------------------------------------------------------------------------
-- 10. stg_ledger_entries
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_ledger_entries (
    ledger_id       BIGSERIAL   NOT NULL,
    payment_id      BIGINT      NOT NULL REFERENCES raw_payments (payment_id),
    entry_type      TEXT        NOT NULL
                        CHECK (entry_type IN (
                            'brand_charge', 'platform_fee_revenue',
                            'stripe_processing_fee', 'refund_adjustment',
                            'creator_payout'
                        )),
    amount_cents    BIGINT      NOT NULL,    -- positive = revenue; negative = cost
    entry_date      DATE        NOT NULL,
    CONSTRAINT pk_stg_ledger_entries PRIMARY KEY (ledger_id)
);

CREATE INDEX IF NOT EXISTS idx_ledger_payment_id  ON stg_ledger_entries (payment_id);
CREATE INDEX IF NOT EXISTS idx_ledger_entry_date  ON stg_ledger_entries (entry_date);
CREATE INDEX IF NOT EXISTS idx_ledger_entry_type  ON stg_ledger_entries (entry_type);

-- ---------------------------------------------------------------------------
-- 11. mart_daily_financials
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart_daily_financials (
    entry_date              DATE    NOT NULL,
    gmv_cents               BIGINT  NOT NULL DEFAULT 0,
    net_gmv_cents           BIGINT  NOT NULL DEFAULT 0,
    platform_revenue_cents  BIGINT  NOT NULL DEFAULT 0,
    stripe_fees_cents       BIGINT  NOT NULL DEFAULT 0,
    creator_payouts_cents   BIGINT  NOT NULL DEFAULT 0,
    gross_margin_cents      BIGINT  NOT NULL DEFAULT 0,
    take_rate_gross         NUMERIC(10, 6),
    take_rate_net           NUMERIC(10, 6),
    CONSTRAINT pk_mart_daily_financials PRIMARY KEY (entry_date)
);

CREATE INDEX IF NOT EXISTS idx_mdf_entry_date ON mart_daily_financials (entry_date);

-- ---------------------------------------------------------------------------
-- 12. mart_monthly_subscriptions  (MRR waterfall)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS mart_monthly_subscriptions (
    brand_id            INTEGER NOT NULL REFERENCES dim_brand (brand_id),
    month_start_date    DATE    NOT NULL,
    mrr_start_cents     BIGINT  NOT NULL DEFAULT 0,
    mrr_new_cents       BIGINT  NOT NULL DEFAULT 0,
    mrr_expansion_cents BIGINT  NOT NULL DEFAULT 0,
    mrr_contraction_cents BIGINT NOT NULL DEFAULT 0,
    mrr_churned_cents   BIGINT  NOT NULL DEFAULT 0,
    mrr_end_cents       BIGINT  NOT NULL DEFAULT 0,
    CONSTRAINT pk_mart_monthly_subs PRIMARY KEY (brand_id, month_start_date),
    CONSTRAINT fk_mms_brand FOREIGN KEY (brand_id) REFERENCES dim_brand (brand_id)
);

CREATE INDEX IF NOT EXISTS idx_mms_month_start ON mart_monthly_subscriptions (month_start_date);
CREATE INDEX IF NOT EXISTS idx_mms_brand_id    ON mart_monthly_subscriptions (brand_id);
