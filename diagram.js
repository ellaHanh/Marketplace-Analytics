import { useState } from "react";

const phases = [
  {
    id: 0,
    label: "Phase 0",
    title: "Environment",
    color: "#6366f1",
    nodes: [
      { id: "config", label: "settings.yaml", sub: "seeds · scale · fees · rates" },
      { id: "schema", label: "schema.sql", sub: "12 tables + indexes" },
      { id: "dimdate", label: "dim_date seed", sub: "2022-07-01 → 2024-07-31" },
    ],
  },
  {
    id: 1,
    label: "Phase 1",
    title: "Generate",
    color: "#0ea5e9",
    nodes: [
      { id: "brands", label: "dim_brand", sub: "400 brands · tier · industry" },
      { id: "creators", label: "dim_creator", sub: "3 000 creators · follower tier" },
      { id: "subs", label: "raw_subscription_events", sub: "~3 000 events · plan · billing" },
      { id: "campaigns", label: "raw_campaigns", sub: "~9 200 · budget · status" },
      { id: "payments", label: "raw_payments", sub: "~10 500 · gross · fees" },
      { id: "payouts", label: "raw_payouts", sub: "1:1 with payments · delay" },
    ],
  },
  {
    id: "inj",
    label: "Injectors",
    title: "8 Anomaly Patterns",
    color: "#f59e0b",
    nodes: [
      { id: "i1", label: "missing_brand_id", sub: "3% of events" },
      { id: "i2", label: "duplicate_events", sub: "2% of events" },
      { id: "i3", label: "null_campaign_id", sub: "2% → test txns" },
      { id: "i4", label: "partial_refunds", sub: "5% of succeeded" },
      { id: "i5", label: "status_case_drift", sub: "'Succeeded' not 'succeeded'" },
      { id: "i6", label: "payout_mismatch", sub: "5% of refund-eligible" },
      { id: "i7", label: "unresolvable_entity", sub: "1% ghost brands" },
      { id: "i8", label: "timezone_drift", sub: "5% strip UTC offset" },
    ],
  },
  {
    id: 2,
    label: "Phase 2",
    title: "Staging",
    color: "#10b981",
    nodes: [
      { id: "stgsub", label: "stg_subscriptions", sub: "dedup · resolve · MRR · SHA256" },
      { id: "stgpay", label: "stg_payments", sub: "LOWER(status) · is_test flag" },
      { id: "stgpout", label: "stg_payouts", sub: "discrepancy delta" },
      { id: "ledger", label: "stg_ledger_entries", sub: "4–5 rows per payment" },
      { id: "quarantine", label: "stg_unmatched_events", sub: "quarantine · reason codes" },
    ],
  },
  {
    id: 3,
    label: "Phase 3",
    title: "Marts",
    color: "#8b5cf6",
    nodes: [
      { id: "mdf", label: "mart_daily_financials", sub: "GMV · revenue · margin · take-rate" },
      { id: "mmrr", label: "mart_monthly_subscriptions", sub: "MRR waterfall · LAG/LEAD · invariant" },
    ],
  },
  {
    id: 4,
    label: "Phase 4",
    title: "Validate",
    color: "#ef4444",
    nodes: [
      { id: "v1", label: "V1 GMV completeness", sub: "raw == mart sum" },
      { id: "v2", label: "V2 Ledger balance", sub: "margin entries == mart" },
      { id: "v3", label: "V3 MRR invariant", sub: "end[t] == start[t+1]" },
      { id: "v4", label: "V4 Payout discrepancy", sub: "rate in [3%, 7%]" },
      { id: "v5", label: "V5 Unmatched exist", sub: "quarantine non-empty" },
      { id: "v6", label: "V6 No test in mart", sub: "NULL campaign excluded" },
      { id: "v7", label: "V7 Take-rate range", sub: "all rows in [5%, 20%]" },
    ],
  },
  {
    id: 5,
    label: "Phase 5",
    title: "Tests",
    color: "#06b6d4",
    nodes: [
      { id: "t12", label: "T1–T2 MRR waterfall", sub: "monthly · annual · churn" },
      { id: "t34", label: "T3–T4 Ledger fan-out", sub: "4 entries · 5 w/ refund" },
      { id: "t567", label: "T5–T7 Staging", sub: "case norm · quarantine · GMV excl." },
    ],
  },
  {
    id: 6,
    label: "Phase 6",
    title: "Dashboard",
    color: "#f97316",
    nodes: [
      { id: "d1", label: "GMV & Revenue", sub: "dual-axis · MoM delta" },
      { id: "d2", label: "MRR Waterfall", sub: "stacked bar · relative mode" },
      { id: "d3", label: "NRR & Cohort", sub: "retention % · heatmap" },
      { id: "d4", label: "Data Quality", sub: "V1–V7 table · anomaly chart" },
    ],
  },
];

const ledgerTypes = [
  { label: "brand_charge", sign: "+", color: "#10b981" },
  { label: "platform_fee_revenue", sign: "+", color: "#6366f1" },
  { label: "stripe_processing_fee", sign: "−", color: "#f59e0b" },
  { label: "refund_adjustment", sign: "−", color: "#ef4444", optional: true },
  { label: "creator_payout", sign: "−", color: "#8b5cf6" },
];

function PhaseCard({ phase, isActive, onClick }) {
  return (
    <div
      onClick={() => onClick(phase.id)}
      style={{
        cursor: "pointer",
        border: `1px solid ${isActive ? phase.color : "#2a2a3a"}`,
        borderRadius: 10,
        padding: "12px 16px",
        background: isActive ? `${phase.color}18` : "#13131f",
        transition: "all 0.2s",
        minWidth: 130,
        boxShadow: isActive ? `0 0 16px ${phase.color}40` : "none",
      }}
    >
      <div style={{ fontSize: 10, color: phase.color, fontFamily: "monospace", marginBottom: 2 }}>
        {phase.label}
      </div>
      <div style={{ fontSize: 13, fontWeight: 700, color: "#e2e8f0", letterSpacing: 0.3 }}>
        {phase.title}
      </div>
      <div style={{ fontSize: 10, color: "#64748b", marginTop: 3 }}>
        {phase.nodes.length} component{phase.nodes.length !== 1 ? "s" : ""}
      </div>
    </div>
  );
}

function Arrow({ color = "#2a2a3a", label }) {
  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 2, padding: "0 4px" }}>
      {label && (
        <div style={{ fontSize: 9, color: "#475569", fontFamily: "monospace", whiteSpace: "nowrap" }}>
          {label}
        </div>
      )}
      <div style={{ display: "flex", alignItems: "center", gap: 0 }}>
        <div style={{ width: 28, height: 1.5, background: color }} />
        <div style={{
          width: 0, height: 0,
          borderTop: "5px solid transparent",
          borderBottom: "5px solid transparent",
          borderLeft: `7px solid ${color}`,
        }} />
      </div>
    </div>
  );
}

function DetailPanel({ phase }) {
  if (!phase) return null;

  const isInjector = phase.id === "inj";

  return (
    <div style={{
      background: "#0d0d1a",
      border: `1px solid ${phase.color}50`,
      borderRadius: 12,
      padding: 20,
      marginTop: 20,
      animation: "fadeIn 0.2s ease",
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16 }}>
        <div style={{
          width: 8, height: 28, borderRadius: 4,
          background: phase.color,
          boxShadow: `0 0 12px ${phase.color}`,
        }} />
        <div>
          <div style={{ fontSize: 11, color: phase.color, fontFamily: "monospace" }}>{phase.label}</div>
          <div style={{ fontSize: 17, fontWeight: 800, color: "#f1f5f9" }}>{phase.title}</div>
        </div>
      </div>

      <div style={{
        display: "grid",
        gridTemplateColumns: "repeat(auto-fill, minmax(220px, 1fr))",
        gap: 10,
      }}>
        {phase.nodes.map((node) => (
          <div key={node.id} style={{
            background: "#13131f",
            border: `1px solid ${phase.color}30`,
            borderLeft: `3px solid ${phase.color}`,
            borderRadius: 7,
            padding: "10px 13px",
          }}>
            <div style={{ fontSize: 12, fontWeight: 700, color: "#e2e8f0", fontFamily: "monospace" }}>
              {isInjector && <span style={{ color: phase.color, marginRight: 5 }}>⚡</span>}
              {node.label}
            </div>
            <div style={{ fontSize: 11, color: "#64748b", marginTop: 3 }}>{node.sub}</div>
          </div>
        ))}
      </div>

      {phase.id === 2 && (
        <div style={{ marginTop: 16 }}>
          <div style={{ fontSize: 11, color: "#64748b", fontFamily: "monospace", marginBottom: 8 }}>
            LEDGER FAN-OUT — stg_payments → stg_ledger_entries
          </div>
          <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
            {ledgerTypes.map((e) => (
              <div key={e.label} style={{
                background: `${e.color}15`,
                border: `1px solid ${e.color}50`,
                borderRadius: 6,
                padding: "5px 10px",
                display: "flex",
                alignItems: "center",
                gap: 6,
              }}>
                <span style={{ color: e.color, fontWeight: 900, fontSize: 14 }}>{e.sign}</span>
                <span style={{ fontSize: 11, color: "#cbd5e1", fontFamily: "monospace" }}>{e.label}</span>
                {e.optional && <span style={{ fontSize: 9, color: "#475569" }}>(if refund)</span>}
              </div>
            ))}
          </div>
        </div>
      )}

      {phase.id === 4 && (
        <div style={{ marginTop: 14, padding: "10px 14px", background: "#0a1a0e", border: "1px solid #10b98130", borderRadius: 8 }}>
          <div style={{ fontSize: 11, color: "#10b981", fontFamily: "monospace" }}>
            reports/validation_&#123;timestamp&#125;.json · all 7 assertions run regardless of failures · exit code 1 on any fail
          </div>
        </div>
      )}

      {phase.id === 5 && (
        <div style={{ marginTop: 14, padding: "10px 14px", background: "#0a1520", border: "1px solid #06b6d430", borderRadius: 8 }}>
          <div style={{ fontSize: 11, color: "#06b6d4", fontFamily: "monospace" }}>
            pytest-postgresql · ephemeral DB per test · _engine singleton patched · 7/7 passed in 8.9s
          </div>
        </div>
      )}
    </div>
  );
}

export default function App() {
  const [active, setActive] = useState(null);

  const toggle = (id) => setActive((prev) => (prev === id ? null : id));
  const activePhase = phases.find((p) => p.id === active);

  const mainFlow = phases.filter((p) => p.id !== "inj");
  const injector = phases.find((p) => p.id === "inj");

  return (
    <div style={{
      minHeight: "100vh",
      background: "#080812",
      color: "#e2e8f0",
      fontFamily: "'DM Mono', 'Fira Code', monospace",
      padding: "28px 24px",
    }}>
      <style>{`
        @import url('https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@700;800&display=swap');
        @keyframes fadeIn { from { opacity: 0; transform: translateY(6px); } to { opacity: 1; transform: translateY(0); } }
        * { box-sizing: border-box; }
        ::-webkit-scrollbar { height: 4px; background: #13131f; }
        ::-webkit-scrollbar-thumb { background: #2a2a3a; border-radius: 4px; }
      `}</style>

      {/* Header */}
      <div style={{ marginBottom: 28 }}>
        <div style={{ fontSize: 11, color: "#475569", letterSpacing: 2, marginBottom: 4 }}>
          MARKETPLACE ANALYTICS · FP&A SANDBOX
        </div>
        <div style={{ fontSize: 24, fontFamily: "'Syne', sans-serif", fontWeight: 800, color: "#f1f5f9", letterSpacing: -0.5 }}>
          Pipeline & Workflow Diagram
        </div>
        <div style={{ fontSize: 11, color: "#475569", marginTop: 4 }}>
          Click any phase to expand · 7 phases · 12 DB tables · 8 injectors · 7 tests
        </div>
      </div>

      {/* Main horizontal flow */}
      <div style={{ overflowX: "auto", paddingBottom: 8 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 6, minWidth: "max-content" }}>
          {mainFlow.map((phase, i) => {
            const insertInjectorAfter = i === 1; // after Phase 1 "Generate"
            return (
              <>
                <PhaseCard
                  key={phase.id}
                  phase={phase}
                  isActive={active === phase.id}
                  onClick={toggle}
                />
                {insertInjectorAfter ? (
                  <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 4 }}>
                    <Arrow color={injector.color} label="inject anomalies" />
                    <PhaseCard
                      phase={injector}
                      isActive={active === injector.id}
                      onClick={toggle}
                    />
                  </div>
                ) : i < mainFlow.length - 1 ? (
                  <Arrow
                    key={`arrow-${i}`}
                    color={mainFlow[i + 1].color}
                    label={
                      i === 0 ? "DDL ready" :
                      i === 2 ? "normalise" :
                      i === 3 ? "aggregate" :
                      i === 4 ? "pass/fail" :
                      i === 5 ? "ephemeral DB" : undefined
                    }
                  />
                ) : null}
              </>
            );
          })}
        </div>
      </div>

      {/* Table lineage row */}
      <div style={{
        marginTop: 20,
        padding: "12px 16px",
        background: "#0d0d1a",
        border: "1px solid #1e1e2e",
        borderRadius: 10,
        overflowX: "auto",
      }}>
        <div style={{ fontSize: 10, color: "#475569", marginBottom: 10, letterSpacing: 1 }}>
          TABLE LINEAGE
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: 6, minWidth: "max-content", flexWrap: "wrap" }}>
          {[
            { label: "dim_brand\ndim_creator\ndim_date", color: "#6366f1" },
            { arrow: true, color: "#0ea5e9" },
            { label: "raw_subscription_events\nraw_campaigns\nraw_payments\nraw_payouts", color: "#0ea5e9" },
            { arrow: true, color: "#10b981" },
            { label: "stg_subscriptions\nstg_payments\nstg_payouts\nstg_ledger_entries\nstg_unmatched_events", color: "#10b981" },
            { arrow: true, color: "#8b5cf6" },
            { label: "mart_daily_financials\nmart_monthly_subscriptions", color: "#8b5cf6" },
            { arrow: true, color: "#ef4444" },
            { label: "reports/validation_*.json", color: "#ef4444" },
          ].map((item, i) =>
            item.arrow ? (
              <Arrow key={i} color={item.color} />
            ) : (
              <div key={i} style={{
                background: `${item.color}12`,
                border: `1px solid ${item.color}35`,
                borderRadius: 7,
                padding: "7px 11px",
              }}>
                {item.label.split("\n").map((l) => (
                  <div key={l} style={{ fontSize: 10, color: "#94a3b8", fontFamily: "monospace", lineHeight: 1.7 }}>{l}</div>
                ))}
              </div>
            )
          )}
        </div>
      </div>

      {/* Detail panel */}
      {activePhase && <DetailPanel phase={activePhase} />}

      {/* Legend */}
      <div style={{
        marginTop: 20,
        display: "flex",
        gap: 20,
        flexWrap: "wrap",
        paddingTop: 16,
        borderTop: "1px solid #1e1e2e",
      }}>
        {phases.map((p) => (
          <div
            key={p.id}
            onClick={() => toggle(p.id)}
            style={{ display: "flex", alignItems: "center", gap: 7, cursor: "pointer", opacity: active === p.id ? 1 : 0.6 }}
          >
            <div style={{ width: 8, height: 8, borderRadius: 2, background: p.color, boxShadow: active === p.id ? `0 0 8px ${p.color}` : "none" }} />
            <span style={{ fontSize: 10, color: "#94a3b8" }}>{p.label} · {p.title}</span>
          </div>
        ))}
      </div>
    </div>
  );
}
