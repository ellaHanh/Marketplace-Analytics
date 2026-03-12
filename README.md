# Marketplace Analytics & FP&A Sandbox

A self-contained analytics sandbox that generates a realistic influencer-marketing
marketplace dataset, runs a multi-stage data pipeline, and surfaces financial KPIs
in a Streamlit dashboard — all from a single `make run`.

---

<div align="left">

## 🛒 Workflow 

![Architecture](./MarketplaceAnalyticsWorkflowDiagram.gif)

## 📊 Live Dashboard Demo

[![Dashboard Demo](./marketplace_analytics_dashboard.gif)](#)

**GMV, MRR, NRR, Cohort Retention, Data Quality Metrics**

</div>

<br>

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Python pipeline  (src/)                                             │
│                                                                      │
│  generate/          staging/            marts/           validate/   │
│  ─────────          ────────            ──────           ─────────   │
│  brands.py  ──►  stage_subscriptions ──► build_daily_  run_all_     │
│  creators.py     stage_payments          financials()  checks()     │
│  subscriptions   stage_payouts       ──► build_mrr_    V1–V7 JSON   │
│  campaigns.py    build_ledger()          waterfall()   report       │
│  payments.py                                                         │
│  payouts.py      injectors.py (8 anomaly injectors)                 │
│  loader.py       ▲ applied before load                              │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │  PostgreSQL                                                  │    │
│  │  dim_brand  dim_creator  dim_date                           │    │
│  │  raw_subscription_events  raw_campaigns                     │    │
│  │  raw_payments  raw_payouts                                  │    │
│  │  stg_subscriptions  stg_payments  stg_payouts              │    │
│  │  stg_ledger_entries  stg_unmatched_events                  │    │
│  │  mart_daily_financials  mart_monthly_subscriptions         │    │
│  └─────────────────────────────────────────────────────────────┘    │
│                          ▲                                           │
│  src/dashboard/app.py ───┘   (Streamlit, read-only)                 │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Metrics Produced

| Metric | Definition | Source table |
|---|---|---|
| **GMV** | Gross merchandise value — total brand charges | `mart_daily_financials.gmv_cents` |
| **Net GMV** | GMV minus refunds | `mart_daily_financials.net_gmv_cents` |
| **Platform Revenue** | Marketplace take-rate fees collected | `mart_daily_financials.platform_revenue_cents` |
| **Gross Margin** | Platform revenue − Stripe fees − creator payouts | `mart_daily_financials.gross_margin_cents` |
| **Take Rate (gross)** | Platform revenue / GMV | `mart_daily_financials.take_rate_gross` |
| **Take Rate (net)** | Platform revenue / Net GMV | `mart_daily_financials.take_rate_net` |
| **MRR** | Monthly recurring revenue from active subscriptions | `mart_monthly_subscriptions.mrr_end_cents` |
| **New MRR** | MRR from first-ever subscription month | `mrr_new_cents` |
| **Expansion MRR** | MRR growth vs prior month (existing brands) | `mrr_expansion_cents` |
| **Contraction MRR** | MRR decline vs prior month | `mrr_contraction_cents` |
| **Churned MRR** | MRR lost from brands that cancelled | `mrr_churned_cents` |
| **NRR** | (prior_mrr + expansion − contraction − churn) / prior_mrr | Derived from `mart_monthly_subscriptions` |

---

## Messy Data Patterns

The injector layer deliberately corrupts the synthetic data to model real-world
pipeline quality problems:

| Injector | Rate | Pattern | How it breaks |
|---|---|---|---|
| `inject_missing_brand_id` | 3% of events | `brand_external_id = NULL` | Entity resolution fails → quarantined |
| `inject_duplicate_events` | 2% of events | Identical `raw_event_id` | Must deduplicate before staging |
| `inject_null_campaign_id` | 2% of payments | `campaign_id = NULL` | Marks as test transaction; excluded from GMV |
| `inject_partial_refunds` | 5% of succeeded | `amount_refunded_cents > 0` | Adds a 5th ledger row; adjusts net GMV |
| `inject_status_case_drift` | 10% of payments | `'Succeeded'` not `'succeeded'` | Staging must LOWER(TRIM()) all statuses |
| `inject_payout_mismatch` | 5% of refund-eligible | Payout not adjusted for refund | Creates `has_payout_discrepancy = TRUE` |
| `inject_unresolvable_entities` | 1% of events | `brand_external_id = 'BRD-GHOST'` | JOIN to dim_brand fails → quarantined |
| `inject_timezone_drift` | 5% of events | UTC offset stripped | `_tz_coerced` flag set; reattached in staging |

---

## How to Run

### Prerequisites

- Python 3.12+
- PostgreSQL 14+ running locally (see [PostgreSQL Setup](#postgresql-setup) below)
- A `.env` file with `DATABASE_URL` (copy from `.env.example`)
- **Shell environment:**
  - **Windows**: Git Bash or WSL (Ubuntu/Debian). PowerShell is unsupported for `make` commands.
  - **macOS**: bash or zsh (both have `make` pre-installed or via `brew install make`)
  - **Linux/Unix**: bash or equivalent (make usually pre-installed)

```bash
git clone https://github.com/ellaHanh/Marketplace-Analytics.git
cd .
cp .env.example .env          # see PostgreSQL Setup section below for credentials
make install                  # pip install -r requirements.txt
make reset-db                 # drop + recreate schema (requires Postgres running)
make run                      # generate → stage → mart → validate
make test                     # run T1–T7 against ephemeral DB
streamlit run src/dashboard/app.py
```

### PostgreSQL Setup

**Install PostgreSQL** (platform-specific):

**Windows (WSL):**
```bash
sudo apt update && sudo apt install -y postgresql postgresql-contrib
sudo pg_ctlcluster 14 main start      # Start the daemon
sudo service postgresql status        # Verify it's running
```

**macOS:**
```bash
brew install postgresql
brew services start postgresql
```

**Linux (apt-based):**
```bash
sudo apt update && sudo apt install -y postgresql postgresql-contrib
sudo systemctl start postgresql
```

**Create database and user:**
```bash
sudo -u postgres psql << EOF
CREATE DATABASE marketplace_dev;
CREATE USER marketplace_user WITH PASSWORD 'marketplace_password';
GRANT ALL PRIVILEGES ON DATABASE marketplace_dev TO marketplace_user;
EOF
```

**Update `.env` with credentials:**
```bash
cat > .env << EOF
DATABASE_URL=postgresql://marketplace_user:marketplace_password@localhost:5432/marketplace_dev
EOF
```

**Test the connection:**
```bash
psql postgresql://marketplace_user:marketplace_password@localhost:5432/marketplace_dev -c "SELECT 1"
```

If successful, you're ready to run `make reset-db` and the pipeline.

### make targets

| Target | Action |
|---|---|
| `make install` | Install all Python dependencies |
| `make reset-db` | Drop all tables, re-apply schema.sql + dim_date.sql |
| `make run` | Full pipeline: generate → stage → mart → validate |
| `make test` | Run pytest T1–T7 (requires local Postgres on port 5433) |
| `make clean` | Remove `.pyc`, `__pycache__`, `reports/` |

---

## How to Regenerate

All seeds and scale parameters live in `config/settings.yaml`. Change them and
rerun `make reset-db && make run` to get a fully reproducible dataset:

```yaml
seeds:
  faker: 42
  numpy: 42
  python: 42

scale:
  n_brands: 400
  n_creators: 3000
  n_months: 15
  start_date: "2023-01-01"

fees:
  take_rate: 0.10          # platform fee on each campaign payment
  stripe_pct: 0.029
  stripe_fixed: 30         # cents

injection_rates:
  missing_brand_id: 0.03
  duplicate_events: 0.02
  null_campaign_id: 0.02
  partial_refunds: 0.05
  status_case_drift: 0.10
  payout_mismatch: 0.05
  unresolvable_entities: 0.01
  timezone_drift: 0.05
```

---

## Sample Metrics (seed=42, n_brands=400, n_months=15)

| Metric | Value |
|---|---|
| Total brands | 400 |
| Total creators | 3 000 |
| Raw subscription events | ~3 000 |
| Raw campaigns | ~9 200 |
| Raw payments | ~10 500 |
| Staged subscriptions | ~2 900 |
| Quarantined events | ~130 |
| Ledger entries | ~47 000 |
| `mart_daily_financials` rows | ~450 (one per date) |
| `mart_monthly_subscriptions` rows | ~4 500 (brand × month) |
| V1–V7 assertions | All pass |
| Payout discrepancy rate | ~5% |

---

## Design Decisions

**Why PostgreSQL?**
Single-node Postgres is sufficient for this dataset size (< 1 M rows) and lets
us use window functions (LAG/LEAD), `GENERATE_SERIES`, `SHA256`, and `COPY FROM
STDIN` — all without external orchestration dependencies.

**Annual plan churn recognition**
A cancelled annual subscription's churn is recognised in the *first calendar
month after the `end_date`*, not in the cancellation month.  This matches
SaaS accounting convention: the customer has paid for and received service
through `end_date`; MRR only drops to zero when service would have renewed.
The waterfall spine extends one month past `MAX(end_date)` to capture this row.

**1 : 1 payout mapping**
Each `raw_payments` row has exactly one `raw_payouts` row (even for
instalment payments).  This simplifies the ledger fan-out: the
`creator_payout` ledger entry always uses `raw_payouts.amount_paid_cents`
and the discrepancy check is a straight subtraction rather than a
multi-row aggregation.

**Idempotent mart builds**
Both `mart_daily_financials` and `mart_monthly_subscriptions` are
`TRUNCATE`-then-`INSERT`.  Re-running the pipeline on the same data always
produces bit-identical results; there is no incremental / merge logic.
