"""Streamlit dashboard for the Marketplace Analytics & FP&A Sandbox.

Displays four sections populated from mart and validation tables:

    1. GMV & Revenue   — monthly GMV trend with dual-axis take-rate overlay.
    2. MRR Waterfall   — stacked bar of new/expansion/contraction/churn + MRR line.
    3. NRR & Churn     — net revenue retention line + cohort retention heatmap.
    4. Data Quality    — V1–V7 validation results + anomaly counts bar chart.

Usage::

    streamlit run src/dashboard/app.py

Prerequisites:
    The pipeline must have been run at least once (``make run``) so that
    mart tables and a validation report exist.
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

# Add repo root to Python path for imports
repo_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(repo_root))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import text

from src.db import get_engine

logger = logging.getLogger(__name__)

_REPORTS_DIR = Path(__file__).parent.parent.parent / "reports"

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Marketplace Analytics",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.title("📊 Marketplace Analytics & FP&A Sandbox")
st.caption("Live from mart tables — run `make run` to refresh data.")


# ---------------------------------------------------------------------------
# Data loaders (cached per session)
# ---------------------------------------------------------------------------


@st.cache_data(ttl=300)
def _load_daily_financials() -> pd.DataFrame:
    """Load mart_daily_financials and aggregate to monthly granularity.

    Returns:
        pd.DataFrame: Monthly aggregated financials with derived take-rate columns.
    """
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(
            """
            SELECT
                DATE_TRUNC('month', entry_date)::DATE    AS month,
                SUM(gmv_cents)                           AS gmv_cents,
                SUM(net_gmv_cents)                       AS net_gmv_cents,
                SUM(platform_revenue_cents)              AS platform_revenue_cents,
                SUM(stripe_fees_cents)                   AS stripe_fees_cents,
                SUM(creator_payouts_cents)               AS creator_payouts_cents,
                SUM(gross_margin_cents)                  AS gross_margin_cents,
                ROUND(
                    SUM(platform_revenue_cents)::NUMERIC
                    / NULLIF(SUM(gmv_cents), 0),
                    4
                )                                        AS take_rate_gross
            FROM mart_daily_financials
            GROUP BY 1
            ORDER BY 1
            """
        ), conn)
    df["month"] = pd.to_datetime(df["month"])
    for col in ["gmv_cents", "platform_revenue_cents", "gross_margin_cents"]:
        df[col.replace("_cents", "_k")] = df[col] / 100_000  # → $k
    return df


@st.cache_data(ttl=300)
def _load_mrr_waterfall() -> pd.DataFrame:
    """Load mart_monthly_subscriptions aggregated across all brands.

    Returns:
        pd.DataFrame: Monthly MRR movements and end-of-month MRR totals.
    """
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(
            """
            SELECT
                month_start_date                        AS month,
                SUM(mrr_new_cents)                      AS new_cents,
                SUM(mrr_expansion_cents)                AS expansion_cents,
                -SUM(mrr_contraction_cents)             AS contraction_cents,
                -SUM(mrr_churned_cents)                 AS churned_cents,
                SUM(mrr_end_cents)                      AS mrr_end_cents
            FROM mart_monthly_subscriptions
            GROUP BY 1
            ORDER BY 1
            """
        ), conn)
    df["month"] = pd.to_datetime(df["month"])
    for col in ["new_cents", "expansion_cents", "contraction_cents",
                "churned_cents", "mrr_end_cents"]:
        df[col.replace("_cents", "_k")] = df[col] / 100_000
    return df


@st.cache_data(ttl=300)
def _load_nrr() -> pd.DataFrame:
    """Compute net revenue retention (NRR) from the MRR waterfall.

    NRR = (prior_mrr + expansion - contraction - churned) / prior_mrr.

    Returns:
        pd.DataFrame: Monthly NRR percentages and active brand counts.
    """
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(
            """
            WITH monthly AS (
                SELECT
                    month_start_date                    AS month,
                    SUM(mrr_start_cents)                AS mrr_start,
                    SUM(mrr_expansion_cents)            AS expansion,
                    SUM(mrr_contraction_cents)          AS contraction,
                    SUM(mrr_churned_cents)              AS churned,
                    COUNT(*) FILTER (WHERE mrr_end_cents > 0) AS active_brands
                FROM mart_monthly_subscriptions
                GROUP BY 1
            )
            SELECT
                month,
                active_brands,
                ROUND(
                    (mrr_start + expansion - contraction - churned)::NUMERIC
                    / NULLIF(mrr_start, 0) * 100,
                    2
                ) AS nrr_pct
            FROM monthly
            ORDER BY 1
            """
        ), conn)
    df["month"] = pd.to_datetime(df["month"])
    return df


@st.cache_data(ttl=300)
def _load_cohort_retention() -> pd.DataFrame:
    """Build a brand cohort retention matrix (acquisition month × age in months).

    Returns:
        pd.DataFrame: Pivot table with cohort months as index, age as columns,
            values are retention percentages (0–100).
    """
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql(text(
            """
            WITH first_month AS (
                SELECT brand_id, MIN(month_start_date) AS cohort_month
                FROM mart_monthly_subscriptions
                WHERE mrr_end_cents > 0
                GROUP BY brand_id
            ),
            cohort_data AS (
                SELECT
                    fm.cohort_month,
                    ms.month_start_date,
                    (EXTRACT(YEAR FROM AGE(ms.month_start_date, fm.cohort_month)) * 12
                     + EXTRACT(MONTH FROM AGE(ms.month_start_date, fm.cohort_month))
                    )::INT AS month_index,
                    COUNT(*) FILTER (WHERE ms.mrr_end_cents > 0) AS retained,
                    COUNT(DISTINCT fm.brand_id) AS cohort_size
                FROM mart_monthly_subscriptions ms
                JOIN first_month fm ON fm.brand_id = ms.brand_id
                GROUP BY 1, 2, 3
            )
            SELECT
                cohort_month,
                month_index,
                ROUND(retained::NUMERIC / NULLIF(cohort_size, 0) * 100, 1) AS retention_pct
            FROM cohort_data
            WHERE month_index <= 11
            ORDER BY 1, 2
            """
        ), conn)
    df["cohort_month"] = pd.to_datetime(df["cohort_month"]).dt.strftime("%Y-%m")
    pivot = df.pivot(index="cohort_month", columns="month_index", values="retention_pct")
    pivot.columns = [f"M+{c}" for c in pivot.columns]
    return pivot


@st.cache_data(ttl=300)
def _load_unmatched_summary() -> pd.DataFrame:
    """Count quarantined rows by reason code from stg_unmatched_events.

    Returns:
        pd.DataFrame: reason → count, sorted descending.
    """
    engine = get_engine()
    with engine.connect() as conn:
        return pd.read_sql(text(
            "SELECT reason, COUNT(*) AS count "
            "FROM stg_unmatched_events GROUP BY 1 ORDER BY 2 DESC"
        ), conn)


def _load_latest_validation() -> list[dict]:
    """Load the most recent validation JSON report from reports/.

    Returns:
        list[dict]: List of assertion result dicts, or an empty list if no
            report exists.
    """
    reports = sorted(_REPORTS_DIR.glob("validation_*.json"), reverse=True)
    if not reports:
        return []
    return json.loads(reports[0].read_text()).get("assertions", [])


# ---------------------------------------------------------------------------
# Section 1: GMV & Revenue
# ---------------------------------------------------------------------------

st.header("1 · GMV & Revenue")

try:
    fin_df = _load_daily_financials()

    if fin_df.empty:
        st.info("No data yet — run `make run` to populate mart_daily_financials.")
    else:
        # KPI cards
        last = fin_df.iloc[-1]
        prev = fin_df.iloc[-2] if len(fin_df) > 1 else last
        c1, c2, c3 = st.columns(3)
        c1.metric(
            "GMV (latest month)",
            f"${last['gmv_k']:.1f}k",
            f"{((last['gmv_cents'] - prev['gmv_cents']) / max(prev['gmv_cents'], 1) * 100):.1f}%",
        )
        c2.metric(
            "Platform Revenue",
            f"${last['platform_revenue_k']:.1f}k",
            f"{((last['platform_revenue_cents'] - prev['platform_revenue_cents']) / max(prev['platform_revenue_cents'], 1) * 100):.1f}%",
        )
        c3.metric(
            "Gross Margin",
            f"${last['gross_margin_k']:.1f}k",
        )

        # Dual-axis chart: GMV bars + take-rate line
        fig = go.Figure()
        fig.add_bar(
            x=fin_df["month"], y=fin_df["gmv_k"],
            name="GMV ($k)", marker_color="#4F81BD", opacity=0.8,
        )
        fig.add_scatter(
            x=fin_df["month"], y=fin_df["take_rate_gross"],
            name="Take Rate (gross)", yaxis="y2",
            line=dict(color="#C0504D", width=2), mode="lines+markers",
        )
        fig.update_layout(
            yaxis=dict(title="GMV ($k)"),
            yaxis2=dict(title="Take Rate", overlaying="y", side="right",
                        tickformat=".1%"),
            legend=dict(orientation="h", y=-0.2),
            height=380, margin=dict(t=20),
        )
        st.plotly_chart(fig, use_container_width=True)

except Exception as exc:
    st.error(f"Failed to load GMV data: {exc}")


# ---------------------------------------------------------------------------
# Section 2: MRR Waterfall
# ---------------------------------------------------------------------------

st.header("2 · MRR Waterfall")

try:
    mrr_df = _load_mrr_waterfall()

    if mrr_df.empty:
        st.info("No MRR data yet — run `make run` to populate mart_monthly_subscriptions.")
    else:
        last_mrr = mrr_df.iloc[-1]
        prev_mrr = mrr_df.iloc[-2] if len(mrr_df) > 1 else last_mrr
        mc1, mc2 = st.columns(2)
        mc1.metric(
            "MRR (latest month)",
            f"${last_mrr['mrr_end_k']:.1f}k",
            f"{((last_mrr['mrr_end_cents'] - prev_mrr['mrr_end_cents']) / max(prev_mrr['mrr_end_cents'], 1) * 100):.1f}%",
        )
        mc2.metric(
            "Net New MRR",
            f"${(last_mrr['new_k'] + last_mrr['expansion_k'] + last_mrr['contraction_k'] + last_mrr['churned_k']):.1f}k",
        )

        fig2 = go.Figure()
        for col, label, color in [
            ("new_k", "New", "#70AD47"),
            ("expansion_k", "Expansion", "#4BACC6"),
            ("contraction_k", "Contraction", "#F79646"),
            ("churned_k", "Churned", "#C0504D"),
        ]:
            fig2.add_bar(x=mrr_df["month"], y=mrr_df[col], name=label,
                         marker_color=color)
        fig2.add_scatter(
            x=mrr_df["month"], y=mrr_df["mrr_end_k"],
            name="MRR End ($k)", yaxis="y2",
            line=dict(color="#1F3864", width=2), mode="lines",
        )
        fig2.update_layout(
            barmode="relative",
            yaxis=dict(title="MRR Movement ($k)"),
            yaxis2=dict(title="MRR End ($k)", overlaying="y", side="right"),
            legend=dict(orientation="h", y=-0.2),
            height=380, margin=dict(t=20),
        )
        st.plotly_chart(fig2, use_container_width=True)

except Exception as exc:
    st.error(f"Failed to load MRR waterfall: {exc}")


# ---------------------------------------------------------------------------
# Section 3: NRR & Churn
# ---------------------------------------------------------------------------

st.header("3 · NRR & Cohort Retention")
col_a, col_b = st.columns(2)

with col_a:
    try:
        nrr_df = _load_nrr()
        if nrr_df.empty:
            st.info("No NRR data.")
        else:
            fig3 = go.Figure()
            fig3.add_scatter(
                x=nrr_df["month"], y=nrr_df["nrr_pct"],
                mode="lines+markers", name="NRR %",
                line=dict(color="#4F81BD", width=2),
            )
            fig3.add_hline(y=100, line_dash="dash", line_color="gray",
                           annotation_text="100%")
            fig3.update_layout(
                yaxis=dict(title="NRR %"),
                height=340, margin=dict(t=20),
                title="Net Revenue Retention",
            )
            st.plotly_chart(fig3, use_container_width=True)
    except Exception as exc:
        st.error(f"Failed to load NRR: {exc}")

with col_b:
    try:
        cohort_df = _load_cohort_retention()
        if cohort_df.empty:
            st.info("No cohort data.")
        else:
            fig4 = px.imshow(
                cohort_df,
                color_continuous_scale="Blues",
                aspect="auto",
                title="Cohort Retention (%) — acquisition month × age",
                labels=dict(color="Retention %"),
            )
            fig4.update_layout(height=340, margin=dict(t=40))
            st.plotly_chart(fig4, use_container_width=True)
    except Exception as exc:
        st.error(f"Failed to load cohort data: {exc}")


# ---------------------------------------------------------------------------
# Section 4: Data Quality
# ---------------------------------------------------------------------------

st.header("4 · Data Quality")
dq_col1, dq_col2 = st.columns(2)

with dq_col1:
    st.subheader("V1–V7 Validation Results")
    assertions = _load_latest_validation()
    if not assertions:
        st.warning("No validation report found. Run `make run` first.")
    else:
        rows = []
        for a in assertions:
            icon = "✅" if a["status"] == "pass" else "❌"
            rows.append({
                "Check": f"{icon} {a['name']}",
                "Status": a["status"].upper(),
                "Detail": a["detail"].get("description", ""),
            })
        st.dataframe(
            pd.DataFrame(rows).set_index("Check"),
            use_container_width=True,
        )

with dq_col2:
    st.subheader("Quarantine Breakdown")
    try:
        unmatched_df = _load_unmatched_summary()
        if unmatched_df.empty:
            st.info("No quarantined rows found.")
        else:
            fig5 = px.bar(
                unmatched_df, x="count", y="reason",
                orientation="h", title="Unmatched Events by Reason",
                color="count", color_continuous_scale="Reds",
            )
            fig5.update_layout(
                height=320, margin=dict(t=40),
                showlegend=False,
                yaxis=dict(title=""),
                xaxis=dict(title="Row count"),
            )
            st.plotly_chart(fig5, use_container_width=True)
    except Exception as exc:
        st.error(f"Failed to load quarantine data: {exc}")
