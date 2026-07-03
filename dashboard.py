# -*- coding: utf-8 -*-
"""
FS3 ETL Mission Control Dashboard
----------------------------------
A Streamlit-based operational dashboard providing real-time visibility into
the ETL pipeline. No terminal needed — monitor, debug, and configure everything
from this single interface.

Six Pillars:
  1. Overview     — KPIs, 12-hour throughput chart, system health lights
  2. Machines     — CMM/SPO registry with ONLINE/IDLE/WARNING indicators
  3. Live Queue   — Real-time file state machine viewer
  4. DLQ          — Dead Letter Queue with Retry and Reason actions
  5. Audit Logs   — Searchable, filterable structured log table
  6. Settings     — Edit configs, toggle watchers, update paths

Usage:
  streamlit run ETL_Pipeline/dashboard.py
"""

import json
import shutil
import sys
from pathlib import Path
from datetime import datetime
from typing import Optional
from sqlalchemy import create_engine, text
import pandas as pd  # type: ignore[import-untyped]
import streamlit as st  # type: ignore[import-untyped]

# ── Path setup ───────────────────────────────────────────────────────────────
# Allows this file to import from the ETL_Pipeline package without installing it
ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

CONFIG_PATH = Path(__file__).parent / "configs" / "db_config.json"
MACHINES_CFG_PATH = Path(__file__).parent / "configs" / "machines.json"
PARTS_3D_CFG_PATH = Path(__file__).parent / "configs" / "parts_3d.json"


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG — Must be the very first Streamlit call
# ═══════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="FS3 ETL Mission Control",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL STYLES
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown(
    """
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;600;700&display=swap" rel="stylesheet">
<style>
    /* ══════════════════════════════════════════════════════════════════
       DESIGN TOKENS — "Elite Steel" theme: soft white/grey + navy-blue
       Built for manufacturing / plant-floor operations at a glance.
    ══════════════════════════════════════════════════════════════════ */
    :root {
        --bg-0: #eef1f6;
        --bg-1: #f6f8fb;
        --surface: #ffffff;
        --surface-2: #f7f9fc;
        --border: #e3e8f0;
        --border-strong: #cbd5e1;

        --text-hi: #0f1b33;
        --text-mid: #4b5875;
        --text-low: #8592ab;

        --accent: #1d4ed8;        /* navy blue — primary */
        --accent-2: #0891b2;      /* steel teal — secondary */
        --gold: #b45309;          /* elite / highlight */

        --ok: #059669;
        --ok-bg: #ecfdf5;
        --ok-bd: #a7e8cf;

        --warn: #b45309;
        --warn-bg: #fff8ec;
        --warn-bd: #f3d599;

        --crit: #be123c;
        --crit-bg: #fef1f4;
        --crit-bd: #f6c2d0;

        --info: #1d4ed8;
        --info-bg: #eef3ff;

        --neutral-bg: #eef1f6;
        --neutral: #64748b;

        --radius-lg: 16px;
        --radius-md: 12px;
        --radius-sm: 8px;
        --shadow-soft: 0 1px 2px rgba(15,27,51,0.04), 0 8px 24px rgba(15,27,51,0.06);
    }

    * { font-family: 'Inter', -apple-system, sans-serif; }

    /* ── Base canvas — !important because Streamlit's native theme
       (config.toml / user's browser theme setting) otherwise wins ── */
    html, body, .stApp,
    [data-testid="stAppViewContainer"],
    [data-testid="stMain"],
    [data-testid="stBottomBlockContainer"] {
        background:
            radial-gradient(1200px 600px at 90% -10%, rgba(29,78,216,0.05), transparent 60%),
            radial-gradient(1000px 500px at -10% 10%, rgba(8,145,178,0.04), transparent 55%),
            linear-gradient(180deg, var(--bg-1) 0%, var(--bg-0) 100%) !important;
    }
    [data-testid="stHeader"], [data-testid="stToolbar"] { background: transparent !important; }
    [data-testid="stDecoration"] { background: linear-gradient(90deg, var(--accent), var(--accent-2)) !important; }
    [data-testid="stSidebar"], [data-testid="stSidebarContent"] {
        background: var(--surface) !important;
        border-right: 1px solid var(--border);
    }
    .block-container { padding-top: 2rem; padding-bottom: 3rem; max-width: 1400px; }

    /* ── Typography ── */
    h1, h2, h3, h4 { font-family: 'Inter', sans-serif; letter-spacing: -0.3px; color: var(--text-hi) !important; }
    p, span, div, label { color: var(--text-mid); }
    ::selection { background: rgba(29,78,216,0.15); }

    .pillar-title {
        font-size: 1.5rem; font-weight: 800; color: var(--text-hi);
        display: flex; align-items: center; gap: 10px;
        padding: 2px 0 2px 16px;
        margin-bottom: 1.4rem;
        position: relative;
        letter-spacing: -0.3px;
    }
    .pillar-title::before {
        content: ""; position: absolute; left: 0; top: 2px; bottom: 2px;
        width: 4px; border-radius: 4px;
        background: linear-gradient(180deg, var(--accent), var(--accent-2));
        box-shadow: 0 0 10px rgba(29,78,216,0.25);
    }

    /* ── KPI Card ── */
    .kpi-card {
        background: var(--surface) !important;
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        padding: 1.3rem 1.4rem 1.15rem;
        text-align: left;
        box-shadow: var(--shadow-soft);
        position: relative;
        overflow: hidden;
        transition: border-color 0.2s ease, transform 0.2s ease, box-shadow 0.2s ease;
    }
    .kpi-card::before {
        content: ""; position: absolute; top: 0; left: 0; right: 0; height: 3px;
        background: linear-gradient(90deg, var(--accent), var(--accent-2));
        opacity: 0.9;
    }
    .kpi-card:hover { border-color: var(--border-strong); transform: translateY(-1px); box-shadow: 0 4px 16px rgba(15,27,51,0.09); }
    .kpi-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 2.15rem; font-weight: 700; color: var(--text-hi) !important;
        line-height: 1.1; font-variant-numeric: tabular-nums;
    }
    .kpi-label {
        font-size: 0.7rem; color: var(--text-low) !important; margin-top: 6px;
        text-transform: uppercase; letter-spacing: 1px; font-weight: 700;
    }
    .kpi-delta-good  { color: var(--ok) !important;   font-size: 0.78rem; font-weight: 700; margin-top: 6px; }
    .kpi-delta-bad   { color: var(--crit) !important; font-size: 0.78rem; font-weight: 700; margin-top: 6px; }

    /* ── Health Pill ── */
    .health-online, .health-idle, .health-warning, .health-unknown {
        display: inline-flex; align-items: center; gap: 6px;
        padding: 4px 12px 4px 9px; border-radius: 20px;
        font-size: 0.72rem; font-weight: 700; letter-spacing: 0.4px;
        border: 1px solid transparent;
    }
    .health-online  { background: var(--ok-bg) !important;   color: var(--ok) !important;   border-color: var(--ok-bd); }
    .health-idle    { background: var(--warn-bg) !important; color: var(--warn) !important; border-color: var(--warn-bd); }
    .health-warning { background: var(--crit-bg) !important; color: var(--crit) !important; border-color: var(--crit-bd); }
    .health-unknown { background: var(--neutral-bg) !important; color: var(--neutral) !important; border-color: var(--border-strong); }

    /* ── Status Pill (queue states) ── */
    .st-PENDING     { color: var(--warn) !important; font-weight: 700; }
    .st-PROCESSING  { color: var(--info) !important; font-weight: 700; }
    .st-COMPLETED   { color: var(--ok) !important;   font-weight: 700; }
    .st-FAILED      { color: var(--crit) !important; font-weight: 700; }

    /* ── DLQ row ── */
    .dlq-category {
        font-size: 0.7rem; font-weight: 700; letter-spacing: 0.3px;
        background: var(--crit-bg) !important; color: var(--crit) !important;
        border: 1px solid var(--crit-bd);
        border-radius: 6px; padding: 3px 9px;
    }

    /* ── Log level badges ── */
    .log-INFO     { color: var(--info) !important; font-weight: 600; font-size: 0.78rem; }
    .log-WARNING  { color: var(--warn) !important; font-weight: 600; font-size: 0.78rem; }
    .log-ERROR    { color: var(--crit) !important; font-weight: 700; font-size: 0.78rem; }
    .log-CRITICAL { color: #9f1239 !important; font-weight: 800; font-size: 0.78rem; }

    /* ── Section divider ── */
    .section-divider { border: none; border-top: 1px solid var(--border); margin: 1.8rem 0; }

    /* ── Sidebar branding ── */
    .brand-block { padding: 4px 2px 14px; }
    .brand-title {
        font-size: 1.28rem; font-weight: 800; letter-spacing: -0.3px;
        background: linear-gradient(90deg, var(--text-hi), var(--accent) 85%);
        -webkit-background-clip: text; background-clip: text; color: transparent;
        display: flex; align-items: center; gap: 9px;
    }
    .brand-sub {
        font-size: 0.72rem; color: var(--text-low) !important; margin-top: 3px;
        text-transform: uppercase; letter-spacing: 1.4px; font-weight: 700;
    }
    .status-card {
        background: var(--surface-2) !important; border: 1px solid var(--border);
        border-radius: var(--radius-md); padding: 12px 14px; margin-top: 8px;
    }
    .status-row { display: flex; align-items: center; justify-content: space-between; font-size: 0.8rem; }
    .status-dot { width: 8px; height: 8px; border-radius: 50%; display: inline-block; margin-right: 7px; }
    .status-ts { font-size: 0.7rem; color: var(--text-low) !important; margin-top: 8px; font-family: 'JetBrains Mono', monospace; }

    /* ── Streamlit widget restyling ── */
    [data-testid="stMetric"] {
        background: var(--surface) !important; border: 1px solid var(--border);
        border-radius: var(--radius-md); padding: 0.85rem 1rem;
        box-shadow: var(--shadow-soft);
    }
    [data-testid="stMetricLabel"] { color: var(--text-low) !important; font-size: 0.72rem !important; text-transform: uppercase; letter-spacing: 0.6px; }
    [data-testid="stMetricValue"] { color: var(--text-hi) !important; font-family: 'JetBrains Mono', monospace; }

    div[data-testid="stExpander"] {
        background: var(--surface) !important; border: 1px solid var(--border) !important;
        border-radius: var(--radius-md) !important; overflow: hidden;
        box-shadow: var(--shadow-soft);
    }
    div[data-testid="stExpander"] summary { font-weight: 600; color: var(--text-hi) !important; }

    .stButton button, .stDownloadButton button, .stFormSubmitButton button {
        background: var(--surface) !important; color: var(--text-hi) !important;
        border: 1px solid var(--border-strong) !important; border-radius: 9px !important;
        font-weight: 600 !important; transition: all 0.15s ease !important;
    }
    .stButton button:hover, .stDownloadButton button:hover, .stFormSubmitButton button:hover {
        border-color: var(--accent) !important; color: var(--accent) !important;
        box-shadow: 0 0 0 1px var(--accent) inset !important;
    }
    .stButton button p, .stDownloadButton button p, .stFormSubmitButton button p { color: inherit !important; }

    [data-testid="stTextInput"] input, [data-testid="stNumberInput"] input,
    [data-testid="stSelectbox"] div[data-baseweb="select"] > div, textarea {
        background: var(--surface) !important; border: 1px solid var(--border-strong) !important;
        color: var(--text-hi) !important; border-radius: 8px !important;
    }

    [data-testid="stDataFrame"] { border: 1px solid var(--border); border-radius: var(--radius-md); overflow: hidden; box-shadow: var(--shadow-soft); }
    .dataframe { font-size: 0.82rem; }

    /* ── Alert boxes (st.info / st.success / st.warning / st.error) ── */
    [data-testid="stAlertContentInfo"], [data-testid="stAlertContentInfo"] * {
        background: transparent !important; color: var(--info) !important;
    }
    [data-testid="stAlertContentSuccess"], [data-testid="stAlertContentSuccess"] * {
        background: transparent !important; color: var(--ok) !important;
    }
    [data-testid="stAlertContentWarning"], [data-testid="stAlertContentWarning"] * {
        background: transparent !important; color: var(--warn) !important;
    }
    [data-testid="stAlertContentError"], [data-testid="stAlertContentError"] * {
        background: transparent !important; color: var(--crit) !important;
    }
    div[data-baseweb="notification"] {
        background: var(--surface) !important; border: 1px solid var(--border) !important;
        border-radius: var(--radius-md) !important; box-shadow: var(--shadow-soft) !important;
    }

    hr { border-color: var(--border) !important; }

    [data-testid="stSidebarNav"] button { font-size: 0.9rem; }

    /* Radio nav -> tighten spacing, active item accent */
    [data-testid="stSidebar"] .stRadio > label { display: none; }
    [data-testid="stSidebar"] .stRadio [role="radiogroup"] { gap: 2px; }
    [data-testid="stSidebar"] .stRadio [role="radiogroup"] label {
        border-radius: 8px; padding: 4px 8px; transition: background 0.15s ease;
    }
    [data-testid="stSidebar"] .stRadio [role="radiogroup"] label:hover { background: var(--surface-2); }
    [data-testid="stSidebar"] .stRadio [role="radiogroup"] label span { color: var(--text-mid) !important; }
</style>
""",
    unsafe_allow_html=True,
)


# ═══════════════════════════════════════════════════════════════════════════════
# DATABASE — Lightweight direct connection for the dashboard
# (Not using the ETL pool — Streamlit runs in a separate process)
# ═══════════════════════════════════════════════════════════════════════════════


@st.cache_resource(ttl=0)
def get_db_config() -> dict:
    """Reads db_config.json and returns the connection dict."""
    if not CONFIG_PATH.exists():
        return {}
    with open(CONFIG_PATH, encoding="utf-8") as f:
        raw = json.load(f)
    return raw.get("db", {})


def get_engine():
    """
    Opens a short-lived connection for a single dashboard query.
    The dashboard uses direct connections (not the pool) because it runs
    in a separate process from the ETL daemon.
    Returns None if the DB is unreachable, so the UI degrades gracefully.
    """
    cfg = get_db_config()
    if not cfg:
        return None
    return create_engine(
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}@"
        f"{cfg['host']}:{cfg.get('port', 5432)}/{cfg['dbname']}"
    )


def run_query(sql: str, params=None) -> pd.DataFrame:
    """Executes a read-only query and returns a DataFrame. Returns empty DF on error."""
    engine = get_engine()
    if engine is None:
        return pd.DataFrame()
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(sql, conn, params=params)
        return df
    except Exception:
        return pd.DataFrame()
    finally:
        engine.dispose()


def run_write(sql: str, params=None) -> bool:
    """Executes a write query (UPDATE/INSERT/DELETE). Returns True on success."""
    engine = get_engine()
    if engine is None:
        return False
    try:
        with engine.begin() as conn:
            conn.execute(text(sql), params or {})
        return True
    except Exception:
        return False
    finally:
        engine.dispose()


def db_reachable() -> bool:
    """Returns True if the database is reachable."""
    conn = get_engine()
    if conn:
        conn.dispose()
        return True
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# SIDEBAR NAVIGATION
# ═══════════════════════════════════════════════════════════════════════════════

with st.sidebar:
    st.markdown(
        """
        <div class="brand-block">
            <div class="brand-title">🏭 FS3 MISSION CONTROL</div>
            <div class="brand-sub">Manufacturing ETL Platform</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        '<hr class="section-divider" style="margin:0 0 1rem;">', unsafe_allow_html=True
    )

    page = st.radio(
        "Navigate",
        options=[
            "📊 Overview",
            "🖥️ Machine Registry",
            "📂 Live File Queue",
            "💀 Dead Letter Queue",
            "📋 Audit Logs",
            "⚙️ Settings",
        ],
        label_visibility="collapsed",
    )

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    # DB health indicator in sidebar
    is_db_up = db_reachable()
    db_color = "var(--ok)" if is_db_up else "var(--crit)"
    db_text = "Connected" if is_db_up else "Unreachable"
    st.markdown(
        f"""
        <div class="status-card">
            <div class="status-row">
                <span>🗄️&nbsp; Database</span>
                <span style="color:{db_color};font-weight:700;">
                    <span class="status-dot" style="background:{db_color};box-shadow:0 0 6px {db_color};"></span>{db_text}
                </span>
            </div>
            <div class="status-ts">Last refresh &nbsp;{datetime.now().strftime("%H:%M:%S")}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<div style='height:14px'></div>", unsafe_allow_html=True)

    if st.button("🔄  Refresh Dashboard", width="stretch"):
        st.cache_data.clear()
        st.rerun()

    # Auto-refresh toggle
    auto_refresh = st.toggle("Auto-refresh (30s)", value=False)
    if auto_refresh:
        import time

        time.sleep(30)
        st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# HELPER: KPI card renderer
# ═══════════════════════════════════════════════════════════════════════════════


def kpi_card(label: str, value, delta: Optional[str] = None, delta_good: bool = True):
    delta_html = ""
    if delta:
        cls = "kpi-delta-good" if delta_good else "kpi-delta-bad"
        delta_html = f'<div class="{cls}">{delta}</div>'
    st.markdown(
        f"""
    <div class="kpi-card">
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{label}</div>
        {delta_html}
    </div>
    """,
        unsafe_allow_html=True,
    )


def health_pill(status: str) -> str:
    """Returns an HTML health pill badge."""
    status = (status or "UNKNOWN").upper()
    mapping = {
        "ONLINE": ("health-online", "● ONLINE"),
        "IDLE": ("health-idle", "◔ IDLE"),
        "WARNING": ("health-warning", "⚠ WARNING"),
    }
    cls, label = mapping.get(status, ("health-unknown", status))
    return f'<span class="{cls}">{label}</span>'


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════

if page == "📊 Overview":
    st.markdown(
        '<div class="pillar-title">📊 Overview — Real-Time Metrics</div>',
        unsafe_allow_html=True,
    )

    # ── KPIs ──────────────────────────────────────────────────────────────────
    kpi_df = run_query("SELECT * FROM fs3.v_etl_today_kpis LIMIT 1;")

    if kpi_df.empty or not is_db_up:
        st.warning(
            "⚠️ Cannot reach the database. Fill in `configs/db_config.json` and ensure PostgreSQL is running."
        )
        kpi_row = {}
    else:
        kpi_row = kpi_df.iloc[0].to_dict()

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        kpi_card("Total Processed Today", kpi_row.get("total_processed_today", "—"))
    with c2:
        ng = kpi_row.get("total_failed_today", "—")
        kpi_card("NG Count Today", ng, delta_good=False)
    with c3:
        avg_sec = kpi_row.get("avg_processing_sec")
        kpi_card("Avg Processing Time", f"{avg_sec:.1f}s" if avg_sec else "—")
    with c4:
        kpi_card("Queue Depth", kpi_row.get("queue_depth", "—"))
    with c5:
        dlq = kpi_row.get("dlq_depth", "—")
        kpi_card(
            "DLQ Depth",
            dlq,
            delta="Needs review" if (dlq and dlq > 0) else None,
            delta_good=False,
        )

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    # ── System Health Lights ───────────────────────────────────────────────────
    st.markdown("#### 🔦 System Health")
    h1, h2, h3, h4 = st.columns(4)

    with h1:
        db_status = "🟢 Connected" if is_db_up else "🔴 Unreachable"
        st.metric("🗄️ Database", db_status)
    with h2:
        # Check if any machine watcher has been seen in the last 5 minutes
        watcher_df = run_query(
            "SELECT COUNT(*) AS cnt FROM fs3.etl_machine_registry "
            "WHERE watcher_started_at >= NOW() - INTERVAL '5 minutes';"
        )
        watcher_active = not watcher_df.empty and watcher_df.iloc[0]["cnt"] > 0
        st.metric("👁️ Folder Watcher", "🟢 Active" if watcher_active else "🟡 Unknown")
    with h3:
        dlq_depth = kpi_row.get("dlq_depth", 0) or 0
        st.metric("💀 DLQ Status", "🔴 Has Failures" if dlq_depth > 0 else "🟢 Clear")
    with h4:
        recent_critical = run_query(
            "SELECT COUNT(*) AS cnt FROM fs3.etl_audit_log "
            "WHERE level = 'CRITICAL' AND logged_at >= NOW() - INTERVAL '1 hour';"
        )
        crit_count = 0 if recent_critical.empty else recent_critical.iloc[0]["cnt"]
        st.metric(
            "🚨 Critical Alerts (1h)",
            f"{'🔴 ' if crit_count > 0 else '🟢 '}{crit_count}",
        )

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    # ── 12-Hour Throughput Chart ───────────────────────────────────────────────
    st.markdown("#### 📈 12-Hour Shift Throughput")

    throughput_df = run_query(
        "SELECT * FROM fs3.v_etl_hourly_throughput ORDER BY hour_bucket;"
    )

    if not throughput_df.empty:
        throughput_df["hour_bucket"] = pd.to_datetime(
            throughput_df["hour_bucket"]
        ).dt.strftime("%H:%M")
        throughput_df = throughput_df.set_index("hour_bucket")
        throughput_df.columns = ["✅ Completed", "❌ Failed"]

        st.bar_chart(throughput_df, color=["#1d4ed8", "#be123c"], height=300)
    else:
        st.info(
            "No throughput data yet for the last 12 hours. The chart will populate once files are processed."
        )

    st.markdown('<hr class="section-divider">', unsafe_allow_html=True)

    # ── Recent Activity Feed ───────────────────────────────────────────────────
    st.markdown("#### 🕒 Recent Activity (Last 20 Events)")
    recent_df = run_query("""
        SELECT
            TO_CHAR(logged_at, 'HH24:MI:SS') AS time,
            level,
            machine_id,
            vehicle_model || ' / ' || COALESCE(op_no, '—') AS context,
            SUBSTRING(message, 1, 120) AS message
        FROM fs3.etl_audit_log
        ORDER BY logged_at DESC
        LIMIT 20;
    """)

    if not recent_df.empty:
        st.dataframe(recent_df, width="stretch", hide_index=True)
    else:
        st.info(
            "No recent events. The audit log will populate once the pipeline starts processing files."
        )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — MACHINE REGISTRY
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "🖥️ Machine Registry":
    st.markdown(
        '<div class="pillar-title">🖥️ Machine Registry</div>', unsafe_allow_html=True
    )

    search = st.text_input("🔍 Search by Machine ID", placeholder="e.g. KTSF-0453")

    machines_df = run_query("SELECT * FROM fs3.v_machine_health ORDER BY machine_id;")

    if machines_df.empty:
        st.info(
            "No machines are registered yet. The Job Runner registers machines automatically on first startup. "
            "Check that `ETL_Pipeline/main.py` has been run at least once."
        )
    else:
        if search:
            machines_df = machines_df[
                machines_df["machine_id"].str.contains(search, case=False, na=False)
            ]

        for _, row in machines_df.iterrows():
            with st.expander(
                f"{'🟢' if row['health_status'] == 'ONLINE' else '🟡' if row['health_status'] == 'IDLE' else '🔴'} "
                f"  {row['machine_id']}  —  {row.get('machine_description', '')}"
            ):
                c1, c2, c3, c4 = st.columns(4)
                c1.markdown(
                    f"**Health Status**<br>{health_pill(row['health_status'])}",
                    unsafe_allow_html=True,
                )
                c2.metric("Pipeline Type", row.get("pipeline_type", "—"))
                c3.metric("Total Processed", int(row.get("total_processed", 0) or 0))
                c4.metric("Total Failed", int(row.get("total_failed", 0) or 0))

                st.caption(f"📁 Source Path: `{row.get('source_path', '—')}`")
                last_seen = row.get("last_seen_at")
                st.caption(
                    f"⏱️ Last Seen: {pd.to_datetime(last_seen).strftime('%Y-%m-%d %H:%M:%S') if last_seen else 'Never'}"
                )

                if row.get("schema_drift_flag"):
                    st.error(
                        "⚠️ Schema Drift Detected — review the audit log for this machine for SCHEMA_DRIFT errors."
                    )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 3 — LIVE FILE QUEUE
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "📂 Live File Queue":
    st.markdown(
        '<div class="pillar-title">📂 Live File Queue</div>', unsafe_allow_html=True
    )

    # Status filter
    col_f1, col_f2, col_f3 = st.columns([2, 2, 3])
    with col_f1:
        status_filter = st.selectbox(
            "Filter by Status",
            ["ALL", "PENDING", "PROCESSING", "COMPLETED", "FAILED"],
            index=0,
        )
    with col_f2:
        model_filter = st.text_input(
            "Filter by Vehicle Model", placeholder="RC, NX, ..."
        )
    with col_f3:
        hours_back = st.slider("Show files from last N hours", 1, 48, 12)

    where_clauses = [f"started_at >= NOW() - INTERVAL '{hours_back} hours'"]
    if status_filter != "ALL":
        where_clauses.append(f"status = '{status_filter}'")
    if model_filter:
        where_clauses.append(f"vehicle_model ILIKE '%{model_filter}%'")
    where_sql = " AND ".join(where_clauses)

    queue_df = run_query(f"""
        SELECT
            id,
            file_name,
            machine_id,
            vehicle_model,
            op_no,
            status,
            current_step,
            TO_CHAR(started_at, 'YYYY-MM-DD HH24:MI:SS') AS started_at,
            CASE
                WHEN processing_sec IS NOT NULL THEN ROUND(processing_sec, 1) || 's'
                ELSE '—'
            END AS duration,
            SUBSTRING(error_message, 1, 80) AS error_preview
        FROM fs3.etl_pipeline_log
        WHERE {where_sql}
        ORDER BY started_at DESC
        LIMIT 200;
    """)

    # State machine progress visual
    st.markdown(
        "**State Machine:** `PENDING` → `PROCESSING` → `COMPLETED` &nbsp;|&nbsp; `FAILED`"
    )
    p1, p2, p3, p4 = st.columns(4)
    for col, s in [
        (p1, "PENDING"),
        (p2, "PROCESSING"),
        (p3, "COMPLETED"),
        (p4, "FAILED"),
    ]:
        count_df = run_query(
            f"SELECT COUNT(*) AS cnt FROM fs3.etl_pipeline_log "
            f"WHERE status = %s AND started_at >= NOW() - INTERVAL '{hours_back} hours';",
            params=(s,),
        )
        cnt = 0 if count_df.empty else int(count_df.iloc[0]["cnt"])
        col.metric(s, cnt)

    st.markdown("---")

    if queue_df.empty:
        st.info(
            f"No files found for the selected filters in the last {hours_back} hours."
        )
    else:
        st.dataframe(queue_df, width="stretch", hide_index=True, height=500)
        st.caption(
            f"Showing {len(queue_df)} file(s). Use filters above to narrow down."
        )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — DEAD LETTER QUEUE
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "💀 Dead Letter Queue":
    st.markdown(
        '<div class="pillar-title">💀 Dead Letter Queue (DLQ)</div>',
        unsafe_allow_html=True,
    )

    col_cat, col_mc = st.columns(2)
    with col_cat:
        cat_filter = st.selectbox(
            "Filter by Error Category",
            [
                "ALL",
                "SCHEMA_DRIFT",
                "DB_TIMEOUT",
                "VALIDATION_FAILED",
                "FILE_NOT_FOUND",
                "PARSE_ERROR",
                "UNKNOWN",
            ],
        )
    with col_mc:
        mc_filter = st.text_input("Filter by Machine ID", placeholder="KTSF-0453")

    where_parts = ["dlq_status = 'AWAITING_REVIEW'"]
    if cat_filter != "ALL":
        where_parts.append(f"error_category = '{cat_filter}'")
    if mc_filter:
        where_parts.append(f"machine_id ILIKE '%{mc_filter}%'")

    dlq_df = run_query(f"""
        SELECT
            id,
            pipeline_log_id,
            file_name,
            file_path,
            machine_id,
            vehicle_model,
            op_no,
            failed_step,
            error_category,
            retry_count,
            max_retries,
            TO_CHAR(queued_at, 'YYYY-MM-DD HH24:MI:SS') AS queued_at,
            SUBSTRING(error_message, 1, 100) AS error_preview,
            traceback
        FROM fs3.etl_dlq
        WHERE {" AND ".join(where_parts)}
        ORDER BY queued_at DESC;
    """)

    if dlq_df.empty:
        st.success("✅ Dead Letter Queue is clear — no failed files awaiting review.")
    else:
        st.warning(f"⚠️ {len(dlq_df)} file(s) are in the Dead Letter Queue.")

        for _, row in dlq_df.iterrows():
            with st.expander(
                f"[{row.get('error_category', '?')}]  {row.get('file_name', '—')}  "
                f"({row.get('vehicle_model', '?')} / {row.get('op_no', '?')})  "
                f"— Failed at: {row.get('failed_step', '?')}  "
                f"| Retries: {row.get('retry_count', 0)}/{row.get('max_retries', 3)}"
            ):
                c1, c2, c3 = st.columns([3, 1, 1])

                with c1:
                    st.markdown(f"**Machine:** `{row.get('machine_id', '—')}`")
                    st.markdown(f"**Queued At:** `{row.get('queued_at', '—')}`")
                    st.caption(
                        f"Error: {row.get('error_preview', 'No error message available.')}"
                    )

                with c2:
                    if st.button("🔁 Retry", key=f"retry_{row['id']}"):
                        # ═══════════════════════════════════════════════════════
                        #  ELITE FIX — Scenario 6: Full DLQ Retry Sequence
                        # ═══════════════════════════════════════════════════════
                        #  Step 1: Guard — check retry limit
                        #  Step 2: Find machine source path from machines.json
                        #  Step 3: Physically move file _NG → source_path
                        #  Step 4: Reset etl_pipeline_log.status → PENDING
                        #  Step 5: Set etl_dlq.dlq_status → RETRYING
                        #  The DLQ watcher thread in job_runner picks it up next poll.
                        # ═══════════════════════════════════════════════════════
                        dlq_id = int(row["id"])
                        pipeline_log_id = row.get("pipeline_log_id")
                        file_name_dlq = str(row.get("file_name", ""))
                        ng_file_path = str(row.get("file_path", ""))
                        machine_id_dlq = str(row.get("machine_id", ""))
                        retry_count_cur = int(row.get("retry_count", 0))
                        max_retries_dlq = int(row.get("max_retries", 3))

                        # ── Guard: hard retry cap ────────────────────────────
                        if retry_count_cur >= max_retries_dlq:
                            st.error(
                                f"⛔ Max retries ({max_retries_dlq}) reached. "
                                f"Manual intervention required — inspect the file and fix "
                                f"the root cause before retrying."
                            )
                            st.stop()

                        # ── Find source path for this machine ────────────────
                        machine_conf = None
                        try:
                            with open(MACHINES_CFG_PATH, encoding="utf-8") as _f:
                                _mc_data = json.load(_f)
                            machine_conf = next(
                                (
                                    m
                                    for m in _mc_data.get("watch_directories", [])
                                    if m.get("machine_id") == machine_id_dlq
                                ),
                                None,
                            )
                        except Exception as cfg_err:
                            st.error(f"Failed to read machines.json: {cfg_err}")
                            st.stop()

                        if not machine_conf:
                            st.error(
                                f"Machine ID '{machine_id_dlq}' not found in machines.json. "
                                f"Cannot determine source path for file move."
                            )
                            st.stop()

                        source_path_dir = machine_conf.get("source_path", "")
                        dest_path = Path(source_path_dir) / file_name_dlq
                        ng_path = Path(ng_file_path)

                        # ── Step 3: Physically move file _NG → source_path ───
                        file_move_ok = False
                        if ng_path.exists():
                            try:
                                dest_path.parent.mkdir(parents=True, exist_ok=True)
                                shutil.move(str(ng_path), str(dest_path))
                                file_move_ok = True
                                st.info(
                                    f"📁 File moved: `{ng_path.name}` → `{source_path_dir}`"
                                )
                            except Exception as move_err:
                                st.error(
                                    f"❌ File move failed: {move_err}\n\n"
                                    f"Cannot retry until the file is accessible at the _NG path."
                                )
                                st.stop()
                        else:
                            # File may have been moved already (idempotent retry click)
                            if dest_path.exists():
                                file_move_ok = True
                                st.warning(
                                    "⚠️ File already at source path (may have been moved previously). "
                                    "Proceeding with DB reset."
                                )
                            else:
                                st.error(
                                    "❌ File not found at _NG path: `{ng_file_path}` "
                                    f"and not at source: `{dest_path}`. "
                                    f"Locate the file manually before retrying."
                                )
                                st.stop()

                        if file_move_ok:
                            # ── Step 4: Reset pipeline log → PENDING ─────────
                            reset_ok = (
                                run_write(
                                    """
                                UPDATE fs3.etl_pipeline_log
                                SET status        = 'PENDING',
                                    current_step  = 'DLQ_RETRY_QUEUED',
                                    error_message = NULL,
                                    updated_at    = NOW()
                                WHERE id = %s;
                                """,
                                    params=(int(pipeline_log_id),),
                                )
                                if pipeline_log_id
                                else True
                            )

                            # ── Step 5: Mark DLQ as RETRYING + bump counter ──
                            dlq_ok = run_write(
                                """
                                UPDATE fs3.etl_dlq
                                SET dlq_status   = 'RETRYING',
                                    retry_count   = retry_count + 1,
                                    last_retry_at = NOW()
                                WHERE id = %s;
                                """,
                                params=(dlq_id,),
                            )

                            if reset_ok and dlq_ok:
                                st.success(
                                    f"✅ **Retry queued successfully.**\n\n"
                                    f"- File moved back to `{source_path_dir}`\n"
                                    f"- Pipeline log reset to `PENDING`\n"
                                    f"- DLQ status → `RETRYING`\n\n"
                                    f"The Job Runner's DLQ watcher will pick it up within "
                                    f"{15}s and resubmit it to the pipeline."
                                )
                                st.rerun()
                            else:
                                st.error(
                                    "⚠️ File was moved but the DB update failed. "
                                    "Check database connectivity. "
                                    "You may need to manually reset the pipeline_log status."
                                )

                with c3:
                    if st.button("📋 Reason", key=f"reason_{row['id']}"):
                        st.session_state[
                            f"show_tb_{row['id']}"
                        ] = not st.session_state.get(f"show_tb_{row['id']}", False)

                if st.session_state.get(f"show_tb_{row['id']}", False):
                    tb = (
                        row.get("traceback")
                        or "No traceback was captured for this failure."
                    )
                    st.code(tb, language="python")


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 5 — AUDIT LOGS
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "📋 Audit Logs":
    st.markdown('<div class="pillar-title">📋 Audit Logs</div>', unsafe_allow_html=True)

    # Filters
    f1, f2, f3, f4 = st.columns(4)
    with f1:
        level_filter = st.multiselect(
            "Log Level",
            ["INFO", "WARNING", "ERROR", "CRITICAL"],
            default=["WARNING", "ERROR", "CRITICAL"],
        )
    with f2:
        model_filter_log = st.text_input("Vehicle Model", placeholder="RC, NX, ...")
    with f3:
        op_filter = st.text_input("Operation (OP010...)", placeholder="OP010")
    with f4:
        log_hours = st.slider("Last N hours", 1, 72, 24)

    where_log = [f"logged_at >= NOW() - INTERVAL '{log_hours} hours'"]
    if level_filter:
        lvl_str = ", ".join([f"'{level}'" for level in level_filter])
        where_log.append(f"level IN ({lvl_str})")
    if model_filter_log:
        where_log.append(f"vehicle_model ILIKE '%{model_filter_log}%'")
    if op_filter:
        where_log.append(f"op_no ILIKE '%{op_filter}%'")

    log_df = run_query(f"""
        SELECT
            TO_CHAR(logged_at, 'YYYY-MM-DD HH24:MI:SS') AS logged_at,
            level,
            machine_id,
            vehicle_model,
            op_no,
            current_step,
            message,
            pipeline_log_id
        FROM fs3.etl_audit_log
        WHERE {" AND ".join(where_log)}
        ORDER BY logged_at DESC
        LIMIT 500;
    """)

    st.caption(f"Showing up to 500 entries from the last {log_hours} hours.")

    if log_df.empty:
        st.info("No log entries matching the current filters.")
    else:
        # Color-code by level using column_config
        st.dataframe(
            log_df,
            width="stretch",
            hide_index=True,
            height=600,
            column_config={
                "level": st.column_config.TextColumn("Level", width="small"),
                "machine_id": st.column_config.TextColumn("Machine", width="medium"),
                "vehicle_model": st.column_config.TextColumn("Model", width="small"),
                "op_no": st.column_config.TextColumn("Op", width="small"),
                "current_step": st.column_config.TextColumn("Step", width="medium"),
                "message": st.column_config.TextColumn("Message", width="large"),
                "pipeline_log_id": st.column_config.NumberColumn(
                    "Log ID", width="small"
                ),
            },
        )

        # Download button
        csv = log_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️ Export to CSV",
            data=csv,
            file_name=f"etl_audit_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
        )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 6 — SETTINGS
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "⚙️ Settings":
    st.markdown(
        '<div class="pillar-title">⚙️ Settings — System Configuration</div>',
        unsafe_allow_html=True,
    )

    tab_machines, tab_parts, tab_db = st.tabs(
        ["🖥️ Machines", "🔩 Parts (3D)", "🗄️ Database"]
    )

    # ── MACHINES CONFIG ──────────────────────────────────────────────────────
    with tab_machines:
        st.caption(
            "Edit machine watch directories. Changes take effect on next restart."
        )

        if MACHINES_CFG_PATH.exists():
            with open(MACHINES_CFG_PATH, encoding="utf-8") as f:
                machines_cfg = json.load(f)

            for i, machine in enumerate(machines_cfg.get("watch_directories", [])):
                with st.expander(
                    f"Machine: {machine.get('machine_id', f'Machine {i + 1}')}"
                ):
                    col_a, col_b = st.columns(2)
                    with col_a:
                        new_id = st.text_input(
                            "Machine ID",
                            machine.get("machine_id", ""),
                            key=f"mc_id_{i}",
                        )
                        new_path = st.text_input(
                            "Source Path",
                            machine.get("source_path", ""),
                            key=f"mc_path_{i}",
                        )
                        _ptype_options = ["3D_HD", "3D_BL", "CHECKSHEET"]
                        _ptype_raw = str(
                            machine.get("pipeline_type", "3D_HD") or "3D_HD"
                        )
                        _ptype_default = (
                            _ptype_raw if _ptype_raw in _ptype_options else "3D_HD"
                        )
                        new_type = st.selectbox(
                            "Pipeline Type",
                            _ptype_options,
                            index=_ptype_options.index(_ptype_default),
                            key=f"mc_type_{i}",
                        )
                    with col_b:
                        new_interval = st.number_input(
                            "Poll Interval (sec)",
                            value=machine.get("poll_interval_sec", 5),
                            min_value=1,
                            max_value=300,
                            key=f"mc_interval_{i}",
                        )
                        new_workers = st.number_input(
                            "Max Workers",
                            value=machine.get("max_workers", 2),
                            min_value=1,
                            max_value=20,
                            key=f"mc_workers_{i}",
                        )
                        new_enabled = st.toggle(
                            "Enabled",
                            value=machine.get("enabled", True),
                            key=f"mc_enabled_{i}",
                        )

                    if st.button(f"💾 Save Machine {new_id}", key=f"save_mc_{i}"):
                        machines_cfg["watch_directories"][i].update(
                            {
                                "machine_id": new_id,
                                "source_path": new_path,
                                "pipeline_type": new_type,
                                "poll_interval_sec": new_interval,
                                "max_workers": new_workers,
                                "enabled": new_enabled,
                            }
                        )
                        with open(MACHINES_CFG_PATH, "w", encoding="utf-8") as f:
                            json.dump(machines_cfg, f, indent=2, ensure_ascii=False)
                        st.success(
                            f"✅ Machine '{new_id}' saved. Restart the ETL daemon for changes to take effect."
                        )
        else:
            st.error(
                "machines.json not found. Ensure the ETL_Pipeline/configs/ folder exists."
            )

    # ── PARTS 3D CONFIG ──────────────────────────────────────────────────────
    with tab_parts:
        st.caption(
            "Edit part matching rules. Each rule maps a filename pattern to a vehicle model and parser settings. "
            "Changes take effect on next restart."
        )

        if PARTS_3D_CFG_PATH.exists():
            with open(PARTS_3D_CFG_PATH, encoding="utf-8") as f:
                parts_cfg = json.load(f)

            mappings = parts_cfg.get("mappings", [])

            for i, rule in enumerate(mappings):
                with st.expander(
                    f"Rule {i + 1}: {rule.get('match_pattern', '?')} → {rule.get('vehicle_model', '?')} / {rule.get('op_no', '?')}"
                ):
                    c1, c2, c3 = st.columns(3)
                    with c1:
                        new_pattern = st.text_input(
                            "Match Pattern (regex)",
                            rule.get("match_pattern", ""),
                            key=f"p_pat_{i}",
                        )
                        new_model = st.text_input(
                            "Vehicle Model",
                            rule.get("vehicle_model", ""),
                            key=f"p_model_{i}",
                        )
                        new_part = st.text_input(
                            "Parts Code", rule.get("parts_code", ""), key=f"p_part_{i}"
                        )
                    with c2:
                        new_op = st.text_input(
                            "Operation No.", rule.get("op_no", ""), key=f"p_op_{i}"
                        )
                        new_vpo = st.text_input(
                            "VPO Code", rule.get("vpo_code", ""), key=f"p_vpo_{i}"
                        )
                        _side_options = ["R", "L", "C"]
                        _side_raw = str(rule.get("side", "R") or "R").strip().upper()
                        _side_default = _side_raw if _side_raw in _side_options else "R"
                        new_side = st.selectbox(
                            "Side",
                            _side_options,
                            index=_side_options.index(_side_default),
                            key=f"p_side_{i}",
                        )
                    with c3:
                        parser = rule.get("parser", {})
                        _ftype_options = ["excel", "csv"]
                        _ftype_raw = (
                            str(parser.get("file_type", "excel") or "excel")
                            .strip()
                            .lower()
                        )
                        _ftype_default = (
                            _ftype_raw if _ftype_raw in _ftype_options else "excel"
                        )
                        new_ftype = st.selectbox(
                            "File Type",
                            _ftype_options,
                            index=_ftype_options.index(_ftype_default),
                            key=f"p_ft_{i}",
                        )
                        new_hrow = st.number_input(
                            "Header Row",
                            value=parser.get("header_row", 0),
                            min_value=0,
                            key=f"p_hr_{i}",
                        )
                        new_drow = st.number_input(
                            "Data Start Row",
                            value=parser.get("data_start_row", 1),
                            min_value=0,
                            key=f"p_dr_{i}",
                        )
                        new_vcol = st.number_input(
                            "Value Column Index",
                            value=parser.get("value_column_idx", 5),
                            min_value=0,
                            key=f"p_vc_{i}",
                        )
                        new_mcol = st.number_input(
                            "Measure No. Column",
                            value=parser.get("measure_no_col_idx", 0),
                            min_value=0,
                            key=f"p_mc_{i}",
                        )

                    if st.button(f"💾 Save Rule {i + 1}", key=f"save_rule_{i}"):
                        parts_cfg["mappings"][i].update(
                            {
                                "match_pattern": new_pattern,
                                "vehicle_model": new_model,
                                "parts_code": new_part,
                                "op_no": new_op,
                                "vpo_code": new_vpo,
                                "side": new_side,
                                "parser": {
                                    "file_type": new_ftype,
                                    "header_row": int(new_hrow),
                                    "data_start_row": int(new_drow),
                                    "value_column_idx": int(new_vcol),
                                    "measure_no_col_idx": int(new_mcol),
                                    "excel_sheet_index": parser.get(
                                        "excel_sheet_index", -1
                                    ),
                                },
                            }
                        )
                        with open(PARTS_3D_CFG_PATH, "w", encoding="utf-8") as f:
                            json.dump(parts_cfg, f, indent=2, ensure_ascii=False)
                        st.success(
                            "✅ Rule saved. Restart the ETL daemon for changes to take effect."
                        )

            st.markdown("---")
            st.info(
                "💡 To add a completely new rule, edit `ETL_Pipeline/configs/parts_3d.json` directly and follow the format shown in the existing rules. See README.md for a step-by-step guide."
            )
        else:
            st.error("parts_3d.json not found.")

    # ── DATABASE CONFIG ──────────────────────────────────────────────────────
    with tab_db:
        st.caption("Update database connection credentials.")

        db_cfg = get_db_config()

        with st.form("db_config_form"):
            new_host = st.text_input("Host", db_cfg.get("host", ""))
            new_port = st.number_input(
                "Port",
                value=int(db_cfg.get("port", 5432)),
                min_value=1,
                max_value=65535,
            )
            new_db = st.text_input("Database Name", db_cfg.get("dbname", ""))
            new_user = st.text_input("Username", db_cfg.get("user", ""))
            new_pass = st.text_input(
                "Password", db_cfg.get("password", ""), type="password"
            )
            new_min = st.number_input(
                "Pool Min Connections", value=2, min_value=1, max_value=50
            )
            new_max = st.number_input(
                "Pool Max Connections", value=15, min_value=1, max_value=100
            )

            submitted = st.form_submit_button("💾 Save Database Config")
            if submitted:
                new_cfg = {
                    "db": {
                        "host": new_host,
                        "port": new_port,
                        "dbname": new_db,
                        "user": new_user,
                        "password": new_pass,
                    },
                    "pool": {"min_conn": new_min, "max_conn": new_max},
                }
                with open(CONFIG_PATH, "w", encoding="utf-8") as f:
                    json.dump(new_cfg, f, indent=2)
                st.success(
                    "✅ Database config saved. Restart both the ETL daemon and this dashboard to apply."
                )
                st.cache_resource.clear()
