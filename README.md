# Marketplace Analytics & FP&A Sandbox

A self-contained analytics sandbox that simulates a creator-commerce marketplace — generating realistic brand and creator data, running a production-grade data pipeline, and surfacing financial KPIs in an interactive dashboard. The full environment spins up from a single `make run`.


> **Context:** This project tackles core challenges SaaS platforms face when evolving into two-sided marketplaces — from GMV-driven revenue models and MRR waterfall accounting to data pipeline integrity and financial validation — all delivered in a fully reproducible codebase.


<div align="left">

## 🛒 Workflow 

![Architecture](./MarketplaceAnalyticsWorkflowDiagram.gif)

## 📊 Live Dashboard Demo

[![Dashboard Demo](./marketplace_analytics_dashboard.gif)](#)

**GMV, MRR, NRR, Cohort Retention, Data Quality Metrics**

</div>

<br>


---

## What Problem Does This Solve?

When a SaaS business evolves into a marketplace, its financial model changes fundamentally. Subscription ARR is no longer the only revenue line — GMV, platform take rates, creator payouts, and transaction-level discrepancies all become first-class metrics. The systems that track them (CRM, billing, payments) produce messy, inconsistent data that must be cleaned, reconciled, and validated before any analysis can be trusted.

This sandbox replicates that exact environment:

- A synthetic dataset mimics real-world messiness — case drift, missing IDs, partial refunds, timezone stripping, ghost entities — the same problems found in Stripe, Salesforce, and NetSuite exports
- A multi-layer pipeline cleans and stages the data, fans it out into a double-entry-style ledger, and builds financial marts
- Seven automated data-quality assertions (V1–V7) validate every run and write a timestamped JSON report
- A Streamlit dashboard surfaces GMV, MRR waterfall, NRR, cohort retention, and data quality metrics in one view

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Python pipeline  (src/)                                             │
│                                                                      │
│  generate/          staging/            marts/           validate/   │
│  ─────────          ────────            ──────           ─────────   │
│  brands.py  ──►  stage_subscriptions ──► build_daily_  run_all_      │
│  creators.py     stage_payments          financials()  checks()      │
│  subscriptions   stage_payouts       ──► build_mrr_    V1–V7 JSON    │
│  campaigns.py    build_ledger()          waterfall()   report        │
│  payments.py                                                         │
│  payouts.py      injectors.py (8 anomaly injectors)                  │
│  loader.py       ▲ applied before load                               │
│                                                                      │
│  ┌─────────────────────────────────────────────────────────────┐     │
│  │  PostgreSQL                                                 │     │
│  │  dim_brand  dim_creator  dim_date                           │     │
│  │  raw_subscription_events  raw_campaigns                     │     │
│  │  raw_payments  raw_payouts                                  │     │
│  │  stg_subscriptions  stg_payments  stg_payouts               │     │
│  │  stg_ledger_entries  stg_unmatched_events                   │     │
│  │  mart_daily_financials  mart_monthly_subscriptions          │     │
│  └─────────────────────────────────────────────────────────────┘     │
│                          ▲                                           │
│  src/dashboard/app.py ───┘   (Streamlit, read-only)                  │
└──────────────────────────────────────────────────────────────────────┘
```

---

## Pipeline Walkthrough

The pipeline runs in six sequential stages. Each stage maps directly to something a real marketplace analytics function would own.

---

### Stage 1 — Dimension Tables (Reference Data)

**What happens:** Three reference tables are created and populated before any transactional data is loaded. These are the "master records" everything else joins to.

| Table | What it represents in the real world |
|---|---|
| `dim_brand` | The brand side of the marketplace — companies running influencer campaigns. Includes industry, tier (SMB / Mid-Market / Enterprise), and a stable external ID used by the CRM (Salesforce). This mirrors how a real billing system uses a Salesforce account ID as a foreign key. |
| `dim_creator` | The creator side — influencers and content producers. Segmented by follower tier (nano → mega) and content category. In a real platform, this table would be populated from product sign-up events or a talent database. |
| `dim_date` | A standard date spine covering the full analysis window. Pre-computing calendar attributes (month start/end, quarter, is_weekend) here means every downstream query avoids repeating date logic — a common pattern in production data warehouses. |

**Why it matters:** Without clean reference data, entity resolution breaks downstream. This stage enforces the constraint that every transaction must resolve to a known brand and creator before it enters the pipeline.

---

### Stage 2 — Raw Transactional Data (With Intentional Messiness)

**What happens:** Four raw tables are populated from the synthetic generator. Before loading, eight anomaly injectors deliberately corrupt a fraction of the data to simulate real-world data quality problems.

| Table | What it represents in the real world |
|---|---|
| `raw_subscription_events` | SaaS subscription lifecycle events sourced from a billing system (e.g., Stripe Billing or Chargebee). Contains `subscription_created`, `renewal`, `cancellation`, `upgrade`, and `downgrade` events. In practice, these arrive as webhook streams and must be deduplicated and sequenced before they can drive MRR. |
| `raw_campaigns` | Campaign records created in a marketplace platform — the agreement between a brand and a creator on deliverables and budget. Equivalent to an opportunity or order object in Salesforce. Status transitions (active → completed → cancelled) affect whether associated payments are included in GMV. |
| `raw_payments` | Payment transactions processed through Stripe. Each row captures gross amount, platform fee, Stripe processing fee, and refund amount. This is the primary source of GMV and take-rate metrics — but only after cleaning. |
| `raw_payouts` | Creator payouts linked 1:1 to payments. Represents the money flowing out to the creator side of the marketplace. The gap between expected payout and actual payout (introduced by the mismatch injector) models the reconciliation work that finance teams do on creator payment runs. |

**The eight anomaly injectors** (applied before load):

| Injector | Rate | What it simulates |
|---|---|---|
| `missing_brand_id` | 3% | A Salesforce/billing system sync gap — the brand ID didn't propagate to the event |
| `duplicate_events` | 2% | Webhook retry storms — the same event delivered more than once |
| `null_campaign_id` | 2% | Test or manual transactions that bypass the campaign workflow |
| `partial_refunds` | 5% | Disputed campaign deliverables resulting in partial brand credits |
| `status_case_drift` | 10% | Mixed-case enum values across API versions (`'Succeeded'` vs `'succeeded'`) |
| `payout_mismatch` | 5% | Creator payouts not adjusted when a refund was issued against the parent payment |
| `unresolvable_entities` | 1% | Ghost brand IDs from deleted or migrated accounts |
| `timezone_drift` | 5% | Timestamps with UTC offset stripped, requiring coercion before date math |

These are not hypothetical edge cases — they are the exact failure modes that appear when pulling data from Stripe, Salesforce, and internal product databases in production.

---

### Stage 3 — Staging Layer (Cleaning & Normalisation)

**What happens:** Raw data is cleaned, resolved, and standardised into staging tables. Rows that cannot be resolved are quarantined rather than silently dropped.

| Table | Transformation applied |
|---|---|
| `stg_subscriptions` | Deduplicates on `raw_event_id` (handles webhook retries), resolves `brand_external_id` to `dim_brand.brand_id`, derives subscription spans (start/end dates), and computes `mrr_cents` — dividing annual plan amounts by 12 to normalise to monthly. A SHA-256 hash of `(brand_id, plan_name, start_date)` produces a stable, deduplicated `subscription_id`. |
| `stg_payments` | Applies `LOWER(TRIM(status))` to normalise case drift. Flags test transactions (`campaign_id IS NULL`). Resolves brand and creator IDs. Test transactions are kept but flagged so they can be excluded from GMV without being lost. |
| `stg_payouts` | Joins to `stg_payments`, computes `discrepancy_cents` (the difference between what a creator was owed and what was paid), and sets a boolean `has_payout_discrepancy` flag. This is the field that drives the V4 payout reconciliation assertion. |
| `stg_ledger_entries` | The pipeline's most important staging output. Each payment fans out into 4–5 double-entry-style ledger rows: a brand charge (positive), a platform fee (positive), a Stripe fee (negative), a creator payout (negative), and — when a refund exists — a refund adjustment (negative). This structure means the ledger always balances and every downstream financial metric can be derived from a single `SUM` grouped by `entry_type`. |
| `stg_unmatched_events` | Quarantine table. Rows that fail entity resolution land here with a reason code (`missing_brand_external_id`, `unresolvable_brand_external_id`). In a real platform, this table feeds a data quality alert and a daily reconciliation report — you never silently discard data. |

---

### Stage 4 — Marts (Financial Reporting Layer)

**What happens:** Staging tables are aggregated into two analytical marts — the primary data sources for dashboards and financial reports. Both are `TRUNCATE`-then-`INSERT`, making every run fully idempotent.

| Table | What it produces |
|---|---|
| `mart_daily_financials` | One row per calendar date. Pivots `stg_ledger_entries` by `entry_type` to produce GMV, net GMV, platform revenue, Stripe fees, creator payouts, gross margin, and both take-rate variants. This is the table a finance team would use for daily revenue reporting, board packages, and budget-vs-actual variance analysis. |
| `mart_monthly_subscriptions` | One row per `(brand, month)` pair. Implements the standard SaaS MRR waterfall using a LAG window function: classifies each month's MRR change as new, expansion, contraction, or churn. A LEAD-based continuity invariant (`mrr_end[t] == mrr_start[t+1]`) is enforced after every build — if it fails, the pipeline halts before writing a broken waterfall to the dashboard. |

**Annual plan churn recognition:** A cancelled annual subscription's churn appears in the first month *after* the `end_date`, not in the cancellation month. This matches SaaS revenue recognition convention — the brand has paid for and received service through the end date; MRR only drops when service would have renewed. The mart spine is extended one month past `MAX(end_date)` specifically to capture this row.

---

### Stage 5 — Validation (Data Quality Assertions)

**What happens:** Seven assertions run in sequence after every pipeline execution. No assertion short-circuits the others — all seven always run so the full health of the pipeline is visible in every report.

| Assertion | What it checks | Why it matters |
|---|---|---|
| V1 — GMV completeness | `SUM(raw_payments.amount_gross_cents)` == `SUM(mart_daily_financials.gmv_cents)` | Confirms no payments were lost or double-counted in the staging and ledger steps |
| V2 — Ledger balance | Margin entries in ledger == `gross_margin_cents` in mart | Validates the double-entry fan-out — the ledger must always foot to the mart |
| V3 — MRR invariant | `mrr_end[t]` == `mrr_start[t+1]` for all consecutive brand-months | A broken waterfall is invisible to the eye but produces wrong NRR and churn figures |
| V4 — Payout discrepancy rate | Rate of `has_payout_discrepancy = TRUE` is between 3% and 7% | Validates that the payout mismatch injector fired at the expected rate — monitors pipeline integrity |
| V5 — Unmatched events exist | `COUNT(stg_unmatched_events) > 0` | Confirms the quarantine pipeline ran and messy data was handled, not silently dropped |
| V6 — No test transactions in mart | Test payments (`campaign_id IS NULL`) do not appear in GMV | Finance teams require clean separation between live revenue and test/internal transactions |
| V7 — Take rate range | All `take_rate_gross` values are within [5%, 20%] | Catches fee configuration errors that would flow through silently to board-level revenue reporting |

Results are written to `reports/validation_{timestamp}.json`. The orchestrator exits with code `1` if any assertion fails — compatible with CI/CD pipeline gates.

---

### Stage 6 — Dashboard (Analytical Surface)

**What happens:** A Streamlit dashboard reads from the mart tables and the latest validation report. All four sections have KPI cards with month-over-month delta.

| Section | Metrics surfaced |
|---|---|
| GMV & Revenue | Monthly GMV trend, platform revenue, gross margin, take rate (dual-axis chart) |
| MRR Waterfall | New / expansion / contraction / churned MRR stacked bar + MRR end-of-month line |
| NRR & Cohort Retention | Net revenue retention % over time + cohort retention heatmap (acquisition month × age in months) |
| Data Quality | V1–V7 pass/fail table from latest validation report + quarantine breakdown by reason code |

---

## Metrics Reference

| Metric | Definition | Source |
|---|---|---|
| **GMV** | Total gross value of brand charges processed | `mart_daily_financials.gmv_cents` |
| **Net GMV** | GMV minus refunds | `mart_daily_financials.net_gmv_cents` |
| **Platform Revenue** | Marketplace take-rate fees collected from brands | `mart_daily_financials.platform_revenue_cents` |
| **Gross Margin** | Platform revenue − Stripe fees − creator payouts | `mart_daily_financials.gross_margin_cents` |
| **Take Rate (gross)** | Platform revenue / GMV | `mart_daily_financials.take_rate_gross` |
| **Take Rate (net)** | Platform revenue / Net GMV | `mart_daily_financials.take_rate_net` |
| **MRR** | Monthly recurring revenue from active subscriptions | `mart_monthly_subscriptions.mrr_end_cents` |
| **New MRR** | MRR from a brand's first subscription month | `mrr_new_cents` |
| **Expansion MRR** | MRR increase vs prior month for existing brands | `mrr_expansion_cents` |
| **Contraction MRR** | MRR decrease vs prior month (not to zero) | `mrr_contraction_cents` |
| **Churned MRR** | MRR lost from brands that cancelled entirely | `mrr_churned_cents` |
| **NRR** | `(prior_mrr + expansion − contraction − churn) / prior_mrr` | Derived in dashboard from `mart_monthly_subscriptions` |

---

## How to Run

### Prerequisites

- Python 3.12+
- PostgreSQL 14+ running locally (see [PostgreSQL Setup](#postgresql-setup) below)
- A `.env` file with `DATABASE_URL` (copy from `.env.example`)
- **Shell environment:**
  - **Windows**: Git Bash or WSL (Ubuntu/Debian). PowerShell is unsupported for `make` commands.
  - **macOS**: bash or zsh (both have `make` pre-installed or via `brew install make`)
  - **Linux/Unix**: bash or equivalent

```bash
git clone https://github.com/ellaHanh/Marketplace-Analytics.git
cd marketplace
cp .env.example .env          # edit with your database credentials
make install                  # pip install -r requirements.txt
make reset-db                 # drop + recreate schema
make run                      # generate → stage → mart → validate
make test                     # run T1–T7 against ephemeral DB
streamlit run src/dashboard/app.py
```

### PostgreSQL Setup

**Windows (WSL):**
```bash
sudo apt update && sudo apt install -y postgresql postgresql-contrib
sudo pg_ctlcluster 14 main start
sudo service postgresql status
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

**Update `.env`:**
```bash
DATABASE_URL=postgresql://marketplace_user:marketplace_password@localhost:5432/marketplace_dev
```

**Verify:**
```bash
psql postgresql://marketplace_user:marketplace_password@localhost:5432/marketplace_dev -c "SELECT 1"
```

### `make` targets

| Target | Action |
|---|---|
| `make install` | Install all Python dependencies |
| `make reset-db` | Drop all tables, re-apply schema.sql + dim_date.sql |
| `make run` | Full pipeline: generate → stage → mart → validate |
| `make test` | Run pytest T1–T7 (requires local Postgres on port 5433) |
| `make clean` | Remove `.pyc`, `__pycache__`, `reports/` |

---

## How to Regenerate

All seeds and scale parameters live in `config/settings.yaml`. Change any value and run `make reset-db && make run` for a fully reproducible dataset:

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

## Sample Output (seed=42, 400 brands, 15 months)

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

## Tests

Seven pytest tests cover the critical pipeline logic using a fully ephemeral database — no test ever touches the development database.

| Test | What it proves |
|---|---|
| T1 — Monthly plan waterfall | Correct new / stable / churn row classification over 3 months + continuity invariant holds |
| T2 — Annual plan waterfall | Pre-computed MRR normalised correctly; churn row appears in month *after* end date |
| T3 — Ledger, no refund | Exactly 4 signed ledger entries with correct amounts |
| T4 — Ledger, with refund | Exactly 5 entries; `refund_adjustment` is negative |
| T5 — Status case normalisation | All `'Succeeded'` variants resolved to `'succeeded'` after staging |
| T6 — NULL brand quarantine | Unresolvable events land in `stg_unmatched_events`, not `stg_subscriptions` |
| T7 — Test transaction exclusion | Null-campaign payments excluded from `mart_daily_financials.gmv_cents`; V6 passes |

```bash
make test
# 7 passed in 8.9s
```

---

## Design Decisions

**Why PostgreSQL?**
Single-node Postgres handles this dataset size (< 1M rows) comfortably and makes the full feature set available without external dependencies: window functions (LAG/LEAD for waterfall classification), `GENERATE_SERIES` for the date spine, `SHA256` for deterministic subscription IDs, and `COPY FROM STDIN` for high-throughput bulk loads.

**Annual plan churn recognition**
Churn is recognised in the first calendar month *after* `end_date`, not at cancellation. This matches SaaS revenue accounting: the brand has paid for service through the end date, so MRR only drops when the renewal would have occurred. The waterfall spine is explicitly extended one month past `MAX(end_date)` to ensure this row is always generated.

**1:1 payout mapping**
Each payment row has exactly one payout row. This simplifies the ledger fan-out — the `creator_payout` entry always uses `raw_payouts.amount_paid_cents` directly, and the discrepancy check is a single subtraction. A many-to-one mapping (multiple payouts per payment) would require aggregation before the ledger step, adding join complexity without analytical benefit at this scale.

**Idempotent mart builds**
Both mart tables are `TRUNCATE`-then-`INSERT`. Re-running the pipeline on the same data always produces bit-identical results. There is no incremental or merge logic, which eliminates an entire class of subtle reprocessing bugs and makes the pipeline trivially restartable after any failure.

**No-short-circuit validation**
All seven assertions always run, even when earlier ones fail. This is deliberate: in a real pipeline, you want the full health picture in every report, not just the first failure. The JSON report and the non-zero exit code together make it easy to integrate with a CI/CD gate or a Slack alert.
