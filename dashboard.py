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

UI: "Industrial Corporate" Power BI-style theme — flat, dense, bordered
cards, navy/slate/white palette, Inter + JetBrains Mono, zero glassmorphism.

Usage:
  streamlit run ETL_Pipeline/dashboard.py
"""

import json
import shutil
import sys
import time
from contextlib import contextmanager
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
    page_title="FS3 Mission Control | ETL",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL STYLE — "Industrial Corporate" Power BI theme
# Built for manufacturing / plant-floor operations. Flat surfaces, 1px borders,
# no shadows, no glassmorphism, dense monospace data.
# ═══════════════════════════════════════════════════════════════════════════════
st.markdown(
    """
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@500;600;700&display=swap" rel="stylesheet">
<style>
    /* ══════════════════════════════════════════════════════════════════
       DESIGN TOKENS
       ══════════════════════════════════════════════════════════════════ */
    :root {
        --primary: #002D62;
        --primary-hover: #0A3C75;
        --secondary: #64748B;
        --bg-main: #F8FAFC;
        --bg-surface: #FFFFFF;
        --bg-zebra: #F8FAFC;
        --border: #E2E8F0;
        --text-hi: #0F172A;
        --text-mid: #334155;
        --text-low: #64748B;

        --ok: #10B981;
        --ok-bg: #F0FDF4;
        --ok-bd: #DCFCE7;

        --warn: #D97706;
        --warn-bg: #FFFBEB;
        --warn-bd: #FEF3C7;

        --crit: #EF4444;
        --crit-bg: #FEF2F2;
        --crit-bd: #FEE2E2;

        --info: #3B82F6;
        --info-bg: #EFF6FF;
        --info-bd: #DBEAFE;

        --radius: 2px;
        --font-ui: 'Inter', -apple-system, sans-serif;
        --font-mono: 'JetBrains Mono', monospace;
    }

    * { font-family: var(--font-ui); }

    /* Base Canvas Reset */
    html, body, .stApp,
    [data-testid="stAppViewContainer"],
    [data-testid="stMain"],
    [data-testid="stBottomBlockContainer"] {
        background-color: var(--bg-main) !important;
        background-image: none !important;
        font-family: var(--font-ui) !important;
        font-size: 13px !important;
    }

    [data-testid="stHeader"], [data-testid="stToolbar"] { background: transparent !important; }
    [data-testid="stDecoration"] { background: var(--primary) !important; height: 3px !important; }

    .block-container {
        padding-top: 1rem !important;
        padding-bottom: 2rem !important;
        max-width: 100% !important;
    }

    /* ── Slim dark-navy sidebar ─────────────────────────────────────────── */
    [data-testid="stSidebar"] {
        background-color: var(--primary) !important;
        border-right: 1px solid var(--border) !important;
    }
    /* Cap width only while expanded — leaving this unconstrained when
       aria-expanded="false" lets Streamlit's own collapse animation shrink
       the sidebar to 0 instead of the layout reserving dead blank space. */
    [data-testid="stSidebar"][aria-expanded="true"] {
        min-width: 250px !important;
        max-width: 280px !important;
    }
    [data-testid="stSidebarContent"] {
        background-color: var(--primary) !important;
        padding-top: 1rem !important;
    }
    [data-testid="stSidebarContent"] * { color: #E2E8F0 !important; }

    .brand-block { padding: 4px 2px 14px; }
    .brand-title {
        font-size: 14px !important;
        font-weight: 800 !important;
        color: #FFFFFF !important;
        display: flex;
        align-items: center;
        gap: 9px;
        letter-spacing: 0.3px;
    }
    .brand-sub {
        font-size: 10.5px !important;
        color: #94A3B8 !important;
        margin-top: 3px;
        text-transform: uppercase;
        letter-spacing: 1px;
        font-weight: 700;
    }

    /* ── Nav buttons (replaces st.radio — more DOM-stable across Streamlit
       versions; "active" page uses type="primary", others type="secondary") ── */
    [data-testid="stSidebar"] div[data-testid="stButton"] { margin-bottom: 3px !important; }
    [data-testid="stSidebar"] .stButton button,
    [data-testid="stSidebar"] button[data-testid^="stBaseButton"] {
        background-color: transparent !important;
        border: 1px solid transparent !important;
        color: #E2E8F0 !important;
        border-radius: var(--radius) !important;
        width: 100% !important;
        font-weight: 600 !important;
        font-size: 13px !important;
        text-align: left !important;
        justify-content: flex-start !important;
        padding: 7px 12px !important;
        box-shadow: none !important;
    }
    [data-testid="stSidebar"] .stButton button p,
    [data-testid="stSidebar"] button[data-testid^="stBaseButton"] p,
    [data-testid="stSidebar"] .stButton button div,
    [data-testid="stSidebar"] button[data-testid^="stBaseButton"] div {
        color: inherit !important;
        font-weight: inherit !important;
        font-size: inherit !important;
        text-align: left !important;
        justify-content: flex-start !important;
    }
    [data-testid="stSidebar"] .stButton button:hover,
    [data-testid="stSidebar"] button[data-testid^="stBaseButton"]:hover {
        background-color: var(--primary-hover) !important;
        border-color: var(--primary-hover) !important;
        color: #FFFFFF !important;
    }
    /* Active nav item = type="primary" button */
    [data-testid="stSidebar"] .stButton button[kind="primary"],
    [data-testid="stSidebar"] button[data-testid="stBaseButton-primary"] {
        background-color: var(--secondary) !important;
        border: 1px solid var(--secondary) !important;
        border-left: 3px solid #FFFFFF !important;
        color: #FFFFFF !important;
    }
    [data-testid="stSidebar"] .stButton button[kind="primary"]:hover,
    [data-testid="stSidebar"] button[data-testid="stBaseButton-primary"]:hover {
        background-color: var(--secondary) !important;
        border-color: var(--secondary) !important;
    }

    .status-card {
        background-color: var(--primary-hover) !important;
        border: 1px solid #104C8C !important;
        border-radius: var(--radius) !important;
        padding: 10px 12px !important;
        margin-top: 8px !important;
    }
    .status-row {
        display: flex;
        align-items: center;
        justify-content: space-between;
        font-size: 11px !important;
    }
    .status-dot {
        width: 8px;
        height: 8px;
        border-radius: 50%;
        display: inline-block;
        margin-right: 6px;
    }
    .status-ts {
        font-size: 10px !important;
        color: #94A3B8 !important;
        margin-top: 6px !important;
        font-family: var(--font-mono) !important;
    }
    [data-testid="stSidebar"] button {
        background-color: transparent !important;
        border: 1px solid #64748B !important;
        color: #FFFFFF !important;
        border-radius: var(--radius) !important;
        width: 100% !important;
        font-weight: 600 !important;
        font-size: 12px !important;
        padding: 4px 8px !important;
    }
    [data-testid="stSidebar"] button:hover {
        background-color: var(--primary-hover) !important;
        border-color: #FFFFFF !important;
    }
    [data-testid="stSidebar"] hr { border-color: #104C8C !important; }

    /* ── Dense typography ───────────────────────────────────────────────── */
    h1, h2, h3, h4, h5, h6 {
        font-family: var(--font-ui) !important;
        font-weight: 700 !important;
        color: var(--primary) !important;
        margin-top: 0.5rem !important;
        margin-bottom: 0.5rem !important;
    }
    h1 { font-size: 1.4rem !important; }
    h2 { font-size: 1.2rem !important; }
    h3 { font-size: 1.05rem !important; }
    h4 { font-size: 0.95rem !important; }

    p, span, div, label { color: var(--text-mid); font-size: 13px !important; }

    .mono-text, .kpi-value, [data-testid="stMetricValue"] {
        font-family: var(--font-mono) !important;
        font-variant-numeric: tabular-nums;
    }

    /* Page / pillar title — top-of-page header for each of the six pillars */
    .pillar-title {
        font-family: var(--font-ui) !important;
        font-size: 18px !important;
        font-weight: 800 !important;
        color: var(--primary) !important;
        padding-bottom: 8px !important;
        margin-bottom: 12px !important;
        border-bottom: 2px solid var(--primary) !important;
        display: flex;
        align-items: center;
        gap: 8px;
    }

    /* ── Global Status Bar (top of every page) ──────────────────────────── */
    .global-status-bar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        flex-wrap: wrap;
        gap: 10px;
        background-color: var(--bg-surface);
        border: 1px solid var(--border);
        border-left: 4px solid var(--primary);
        border-radius: var(--radius);
        padding: 8px 14px;
        margin-bottom: 1rem;
    }
    .status-bar-group { display: flex; align-items: center; gap: 16px; flex-wrap: wrap; }
    .status-bar-item {
        display: flex;
        align-items: center;
        gap: 6px;
        font-size: 12px !important;
        color: var(--text-mid) !important;
        white-space: nowrap;
    }
    .status-bar-item b { color: var(--text-hi) !important; font-weight: 700 !important; }
    .status-bar-clock {
        font-family: var(--font-mono) !important;
        font-size: 12px !important;
        color: var(--text-low) !important;
        font-weight: 600 !important;
    }

    .status-pill {
        display: inline-flex;
        align-items: center;
        gap: 4px;
        padding: 2px 7px;
        border-radius: var(--radius);
        font-size: 10.5px !important;
        font-weight: 700 !important;
        text-transform: uppercase;
        letter-spacing: 0.3px;
        border: 1px solid transparent;
        font-family: var(--font-ui) !important;
        line-height: 1.6;
    }
    .status-pill-ok   { background-color: var(--ok-bg) !important;   color: var(--ok) !important;   border-color: var(--ok-bd) !important; }
    .status-pill-warn { background-color: var(--warn-bg) !important; color: var(--warn) !important; border-color: var(--warn-bd) !important; }
    .status-pill-crit { background-color: var(--crit-bg) !important; color: var(--crit) !important; border-color: var(--crit-bd) !important; }
    .status-pill-info { background-color: var(--info-bg) !important; color: var(--info) !important; border-color: var(--info-bd) !important; }
    .status-pill-off  { background-color: var(--bg-main) !important; color: var(--text-low) !important; border-color: var(--border) !important; }
    .pill-dot {
        width: 6px; height: 6px; border-radius: 50%; display: inline-block;
    }

    /* ── Horizontal KPI / stat cards ─────────────────────────────────────── */
    .kpi-card {
        background-color: var(--bg-surface) !important;
        border: 1px solid var(--border) !important;
        border-radius: var(--radius) !important;
        padding: 10px 12px 10px 14px !important;
        display: flex;
        flex-direction: column;
        justify-content: center;
        position: relative;
        overflow: hidden;
        box-shadow: none !important;
        margin-bottom: 0.5rem;
        min-height: 66px;
    }
    .kpi-card::before {
        content: "";
        position: absolute;
        top: 0; left: 0; bottom: 0;
        width: 4px;
    }
    .kpi-card-neutral::before { background-color: var(--primary) !important; }
    .kpi-card-ok::before      { background-color: var(--ok) !important; }
    .kpi-card-warn::before    { background-color: var(--warn) !important; }
    .kpi-card-error::before   { background-color: var(--crit) !important; }

    .kpi-label {
        font-family: var(--font-ui) !important;
        font-size: 10.5px !important;
        color: var(--text-low) !important;
        text-transform: uppercase !important;
        letter-spacing: 0.5px !important;
        margin-bottom: 3px !important;
        font-weight: 700 !important;
    }
    .kpi-value {
        font-family: var(--font-mono) !important;
        font-size: 21px !important;
        font-weight: 700 !important;
        color: var(--text-hi) !important;
        line-height: 1.15 !important;
    }
    .kpi-delta-good, .kpi-delta-bad {
        font-family: var(--font-ui) !important;
        font-size: 10px !important;
        font-weight: 700 !important;
        margin-top: 2px;
    }
    .kpi-delta-good { color: var(--ok) !important; }
    .kpi-delta-bad  { color: var(--crit) !important; }

    /* ── Card / container system ─────────────────────────────────────────── */
    div[data-testid="stVerticalBlockBorderWrapper"] {
        border: 1px solid var(--border) !important;
        border-radius: var(--radius) !important;
        background-color: var(--bg-surface) !important;
        box-shadow: none !important;
        margin-bottom: 1rem !important;
    }
    div[data-testid="stVerticalBlockBorderWrapper"] > div > div[data-testid="stVerticalBlock"] {
        gap: 0.5rem !important;
    }
    div[data-testid="stVerticalBlockBorderWrapper"] > div {
        padding: 12px !important;
    }
    .container-header {
        background-color: var(--bg-main) !important;
        border-bottom: 1px solid var(--border) !important;
        padding: 7px 12px !important;
        margin: -12px -12px 12px -12px !important;
        font-weight: 700 !important;
        color: var(--primary) !important;
        font-size: 12.5px !important;
        font-family: var(--font-ui) !important;
        text-transform: uppercase;
        letter-spacing: 0.4px;
        display: flex;
        align-items: center;
        gap: 6px;
    }

    /* Expanders */
    div[data-testid="stExpander"] {
        background-color: var(--bg-surface) !important;
        border: 1px solid var(--border) !important;
        border-radius: var(--radius) !important;
        box-shadow: none !important;
        margin-bottom: 0.5rem !important;
    }
    div[data-testid="stExpander"] summary {
        background-color: var(--bg-main) !important;
        border-bottom: 1px solid transparent !important;
        padding: 7px 12px !important;
        font-weight: 700 !important;
        font-size: 12.5px !important;
        color: var(--primary) !important;
    }
    div[data-testid="stExpander"] details[open] summary { border-bottom: 1px solid var(--border) !important; }
    div[data-testid="stExpander"] [data-testid="stExpanderDetails"] { padding: 12px !important; }

    /* Inputs */
    [data-testid="stTextInput"] input,
    [data-testid="stNumberInput"] input,
    [data-testid="stSelectbox"] div[data-baseweb="select"] > div,
    textarea {
        background-color: var(--bg-surface) !important;
        border: 1px solid var(--border) !important;
        color: var(--text-hi) !important;
        border-radius: var(--radius) !important;
        padding: 4px 8px !important;
        font-size: 12px !important;
        box-shadow: none !important;
    }
    [data-testid="stTextInput"] input:focus,
    [data-testid="stNumberInput"] input:focus,
    [data-testid="stSelectbox"] div[data-baseweb="select"] > div:focus,
    textarea:focus { border-color: var(--primary) !important; }

    /* Main-area buttons */
    .stButton button, .stDownloadButton button, .stFormSubmitButton button {
        background-color: var(--bg-surface) !important;
        color: var(--primary) !important;
        border: 1px solid var(--border) !important;
        border-radius: var(--radius) !important;
        font-weight: 600 !important;
        font-size: 12px !important;
        padding: 4px 12px !important;
        box-shadow: none !important;
        transition: all 0.15s ease !important;
    }
    .stButton button:hover, .stDownloadButton button:hover, .stFormSubmitButton button:hover {
        background-color: var(--bg-main) !important;
        border-color: var(--primary) !important;
        color: var(--primary) !important;
    }
    .stButton button p, .stDownloadButton button p, .stFormSubmitButton button p {
        color: inherit !important;
        font-size: 12px !important;
        font-weight: 600 !important;
    }
    /* Primary action buttons (e.g. Retry) get a solid navy fill */
    .stButton button[kind="primary"] {
        background-color: var(--primary) !important;
        color: #FFFFFF !important;
        border-color: var(--primary) !important;
    }
    .stButton button[kind="primary"] p { color: #FFFFFF !important; }

    /* Tabs */
    button[data-baseweb="tab"] {
        font-family: var(--font-ui) !important;
        font-size: 13px !important;
        font-weight: 600 !important;
        color: var(--secondary) !important;
        background-color: transparent !important;
        border: 1px solid transparent !important;
        border-bottom: 2px solid transparent !important;
        border-radius: 0px !important;
        padding: 8px 16px !important;
    }
    button[data-baseweb="tab"]:hover { color: var(--primary) !important; }
    button[data-baseweb="tab"][aria-selected="true"] {
        color: var(--primary) !important;
        border-bottom: 2px solid var(--primary) !important;
    }

    /* Dataframes */
    [data-testid="stDataFrame"] {
        border: 1px solid var(--border) !important;
        border-radius: var(--radius) !important;
        box-shadow: none !important;
    }
    [data-testid="stDataFrame"] * {
        font-family: var(--font-mono) !important;
        font-size: 12px !important;
    }

    /* Rectangular health / status badges */
    .health-online, .health-idle, .health-warning, .health-unknown {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 2px 8px !important;
        border-radius: var(--radius) !important;
        font-size: 11px !important;
        font-weight: 700 !important;
        text-transform: uppercase;
        border: 1px solid transparent;
        font-family: var(--font-ui) !important;
    }
    .health-online  { background: var(--ok-bg) !important;   color: var(--ok) !important;   border-color: var(--ok-bd) !important; }
    .health-idle    { background: var(--warn-bg) !important; color: var(--warn) !important; border-color: var(--warn-bd) !important; }
    .health-warning { background: var(--crit-bg) !important; color: var(--crit) !important; border-color: var(--crit-bd) !important; }
    .health-unknown { background: var(--bg-main) !important; color: var(--secondary) !important; border-color: var(--border) !important; }

    .dlq-category {
        font-size: 11px !important;
        font-weight: 700 !important;
        background: var(--crit-bg) !important;
        color: var(--crit) !important;
        border: 1px solid var(--crit-bd) !important;
        border-radius: var(--radius) !important;
        padding: 2px 6px !important;
    }

    .section-divider { border: none !important; border-top: 1px solid var(--border) !important; margin: 1rem 0 !important; }
    hr { border-color: var(--border) !important; }

    /* ── Alerts (st.info / st.success / st.warning / st.error) ───────────────
       Streamlit renders these via a BaseWeb "notification" wrapper with a
       colored fill by default. We flatten that to a white card with a
       colored left accent bar, matching the KPI-card language, and cover
       both the stAlert and the underlying baseweb testids for resilience
       across Streamlit versions. ── */
    [data-testid="stAlert"],
    div[data-baseweb="notification"] {
        border-radius: var(--radius) !important;
        border: 1px solid var(--border) !important;
        border-left: 4px solid var(--secondary) !important;
        background-color: var(--bg-surface) !important;
        background-image: none !important;
        box-shadow: none !important;
        padding: 8px 12px !important;
    }
    [data-testid="stAlert"] *,
    div[data-baseweb="notification"] * {
        font-size: 13px !important;
        background-color: transparent !important;
    }
    [data-testid="stAlertContentInfo"] { color: var(--info) !important; }
    [data-testid="stAlertContentSuccess"] { color: var(--ok) !important; }
    [data-testid="stAlertContentWarning"] { color: var(--warn) !important; }
    [data-testid="stAlertContentError"] { color: var(--crit) !important; }
    [data-testid="stAlertContentInfo"] *,
    [data-testid="stAlertContainer"]:has([data-testid="stAlertContentInfo"]) { border-left-color: var(--info) !important; }
    [data-testid="stAlertContentSuccess"] *,
    [data-testid="stAlertContainer"]:has([data-testid="stAlertContentSuccess"]) { border-left-color: var(--ok) !important; }
    [data-testid="stAlertContentWarning"] *,
    [data-testid="stAlertContainer"]:has([data-testid="stAlertContentWarning"]) { border-left-color: var(--warn) !important; }
    [data-testid="stAlertContentError"] *,
    [data-testid="stAlertContainer"]:has([data-testid="stAlertContentError"]) { border-left-color: var(--crit) !important; }
    [data-testid="stAlert"]:has([data-testid="stAlertContentInfo"]) { border-left-color: var(--info) !important; }
    [data-testid="stAlert"]:has([data-testid="stAlertContentSuccess"]) { border-left-color: var(--ok) !important; }
    [data-testid="stAlert"]:has([data-testid="stAlertContentWarning"]) { border-left-color: var(--warn) !important; }
    [data-testid="stAlert"]:has([data-testid="stAlertContentError"]) { border-left-color: var(--crit) !important; }

    /* ── Toggle / switch (Auto-refresh) — recolor from Streamlit default ──── */
    [data-testid="stToggle"] label div[data-testid="stWidgetLabel"] p { color: inherit !important; }
    [data-baseweb="switch"] { --toggle-off: var(--secondary); }

    /* ── Kill default dev chrome for a clean production surface ──────────── */
    [data-testid="stToolbarActions"],
    [data-testid="stDeployButton"],
    #MainMenu {
        display: none !important;
    }

    /* ── Top decoration strip — force navy over the default theme accent ─── */
    [data-testid="stDecoration"],
    div[data-testid="stDecoration"] > div,
    [data-testid="stHeader"]::before {
        background: var(--primary) !important;
        background-image: none !important;
        background-color: var(--primary) !important;
        height: 3px !important;
    }
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
# UI HELPERS — cards, KPI/stat tiles, badges, zebra tables
# ═══════════════════════════════════════════════════════════════════════════════


@contextmanager
def section_card(title: str, icon: str = ""):
    """
    Wraps a block of content in a flat, 1px-bordered 'report card' with a
    light-grey header strip — the Power BI-style container used across every
    pillar of the dashboard.

        with section_card("Today's KPIs", "📊"):
            st.columns(...)
    """
    box = st.container(border=True)
    with box:
        label = f"{icon} {title}" if icon else title
        st.markdown(
            f'<div class="container-header">{label}</div>', unsafe_allow_html=True
        )
        yield box


def kpi_card(
    label: str,
    value,
    status: str = "neutral",
    delta: Optional[str] = None,
    delta_good: bool = True,
):
    """
    Renders a dense KPI tile: small uppercase label on top, large bold mono
    value in the center, and a left-edge color strip driven by `status`
    ('neutral' = blue, 'ok' = green, 'warn' = amber, 'error' = red).
    """
    variant = {
        "neutral": "kpi-card-neutral",
        "ok": "kpi-card-ok",
        "warn": "kpi-card-warn",
        "error": "kpi-card-error",
    }.get(status, "kpi-card-neutral")

    delta_html = ""
    if delta:
        cls = "kpi-delta-good" if delta_good else "kpi-delta-bad"
        delta_html = f'<div class="{cls}">{delta}</div>'

    st.markdown(
        f"""
    <div class="kpi-card {variant}">
        <div class="kpi-label">{label}</div>
        <div class="kpi-value">{value}</div>
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


def status_pill(text_: str, level: str = "off") -> str:
    """Small rectangular status-bar pill: level in {'ok','warn','crit','info','off'}."""
    cls = {
        "ok": "status-pill-ok",
        "warn": "status-pill-warn",
        "crit": "status-pill-crit",
        "info": "status-pill-info",
        "off": "status-pill-off",
    }.get(level, "status-pill-off")
    return f'<span class="status-pill {cls}">{text_}</span>'


def zebra(df: pd.DataFrame):
    """
    Applies alternating white / light-steel row striping via a pandas Styler
    (st.dataframe renders through a canvas grid, so row striping has to be
    baked into cell background-color rather than done with CSS selectors).
    """
    if df.empty:
        return df

    def _stripe(row):
        color = (
            "background-color: #F8FAFC" if row.name % 2 else "background-color: #FFFFFF"
        )
        return [color] * len(row)

    return df.style.apply(_stripe, axis=1)


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

    NAV_ITEMS = [
        ("📊", "Overview"),
        ("🖥️", "Machine Registry"),
        ("📂", "Live File Queue"),
        ("💀", "Dead Letter Queue"),
        ("📋", "Audit Logs"),
        ("⚙️", "Settings"),
    ]
    if "active_page" not in st.session_state:
        st.session_state.active_page = "Overview"

    for _icon, _name in NAV_ITEMS:
        _is_active = st.session_state.active_page == _name
        if st.button(
            f"{_icon}  {_name}",
            key=f"nav_{_name}",
            width="stretch",
            type="primary" if _is_active else "secondary",
        ):
            st.session_state.active_page = _name
            st.rerun()

    page = st.session_state.active_page

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


# ═══════════════════════════════════════════════════════════════════════════════
# GLOBAL STATUS BAR — rendered above every pillar, plant-floor "at a glance" strip
# ═══════════════════════════════════════════════════════════════════════════════

# Folder watcher heartbeat: any machine that has checked in within 5 minutes
watcher_df = run_query(
    "SELECT COUNT(*) AS cnt FROM fs3.etl_machine_registry "
    "WHERE watcher_started_at >= NOW() - INTERVAL '5 minutes';"
)
watcher_active = not watcher_df.empty and watcher_df.iloc[0]["cnt"] > 0

# Open DLQ items awaiting review
dlq_open_df = run_query(
    "SELECT COUNT(*) AS cnt FROM fs3.etl_dlq WHERE dlq_status = 'AWAITING_REVIEW';"
)
dlq_open_count = 0 if dlq_open_df.empty else int(dlq_open_df.iloc[0]["cnt"])

db_bar_pill = (
    status_pill("CONNECTED", "ok") if is_db_up else status_pill("UNREACHABLE", "crit")
)
watcher_bar_pill = (
    status_pill("ACTIVE", "ok")
    if watcher_active
    else status_pill("NO HEARTBEAT", "warn")
)
dlq_bar_pill = (
    status_pill(f"{dlq_open_count} AWAITING REVIEW", "crit")
    if dlq_open_count > 0
    else status_pill("CLEAR", "ok")
)
autorefresh_bar_pill = (
    status_pill("ON · 30s", "info") if auto_refresh else status_pill("OFF", "off")
)

st.markdown(
    f"""
    <div class="global-status-bar">
        <div class="status-bar-group">
            <div class="status-bar-item">🗄️ Database&nbsp;{db_bar_pill}</div>
            <div class="status-bar-item">👁️ Folder Watcher&nbsp;{watcher_bar_pill}</div>
            <div class="status-bar-item">💀 DLQ&nbsp;{dlq_bar_pill}</div>
            <div class="status-bar-item">🔁 Auto-Refresh&nbsp;{autorefresh_bar_pill}</div>
        </div>
        <div class="status-bar-item status-bar-clock">🕒 SERVER TIME&nbsp;&nbsp;{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</div>
    </div>
    """,
    unsafe_allow_html=True,
)

if auto_refresh:
    time.sleep(30)
    st.rerun()


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 1 — OVERVIEW
# ═══════════════════════════════════════════════════════════════════════════════

if page == "Overview":
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

    with section_card("Today's KPIs", "📊"):
        c1, c2, c3, c4, c5 = st.columns(5)
        with c1:
            kpi_card(
                "Total Processed Today",
                kpi_row.get("total_processed_today", "—"),
                status="neutral",
            )
        with c2:
            ng = kpi_row.get("total_failed_today", "—")
            ng_status = "error" if isinstance(ng, (int, float)) and ng > 0 else "ok"
            kpi_card("NG Count Today", ng, status=ng_status)
        with c3:
            avg_sec = kpi_row.get("avg_processing_sec")
            kpi_card(
                "Avg Processing Time",
                f"{avg_sec:.1f}s" if avg_sec else "—",
                status="neutral",
            )
        with c4:
            kpi_card("Queue Depth", kpi_row.get("queue_depth", "—"), status="neutral")
        with c5:
            dlq = kpi_row.get("dlq_depth", "—")
            dlq_status = "error" if isinstance(dlq, (int, float)) and dlq > 0 else "ok"
            kpi_card(
                "DLQ Depth",
                dlq,
                status=dlq_status,
                delta="Needs review"
                if (isinstance(dlq, (int, float)) and dlq > 0)
                else None,
                delta_good=False,
            )

    # ── System Health Lights ───────────────────────────────────────────────────
    with section_card("System Health", "🔦"):
        h1, h2, h3, h4 = st.columns(4)
        with h1:
            kpi_card(
                "Database",
                "CONNECTED" if is_db_up else "UNREACHABLE",
                status="ok" if is_db_up else "error",
            )
        with h2:
            kpi_card(
                "Folder Watcher",
                "ACTIVE" if watcher_active else "UNKNOWN",
                status="ok" if watcher_active else "warn",
            )
        with h3:
            dlq_depth = kpi_row.get("dlq_depth", 0) or 0
            kpi_card(
                "DLQ Status",
                "HAS FAILURES" if dlq_depth > 0 else "CLEAR",
                status="error" if dlq_depth > 0 else "ok",
            )
        with h4:
            recent_critical = run_query(
                "SELECT COUNT(*) AS cnt FROM fs3.etl_audit_log "
                "WHERE level = 'CRITICAL' AND logged_at >= NOW() - INTERVAL '1 hour';"
            )
            crit_count = (
                0 if recent_critical.empty else int(recent_critical.iloc[0]["cnt"])
            )
            kpi_card(
                "Critical Alerts (1h)",
                crit_count,
                status="error" if crit_count > 0 else "ok",
            )

    # ── 12-Hour Throughput Chart ───────────────────────────────────────────────
    with section_card("12-Hour Shift Throughput", "📈"):
        throughput_df = run_query(
            "SELECT * FROM fs3.v_etl_hourly_throughput ORDER BY hour_bucket;"
        )

        if not throughput_df.empty:
            throughput_df["hour_bucket"] = pd.to_datetime(
                throughput_df["hour_bucket"]
            ).dt.strftime("%H:%M")
            throughput_df = throughput_df.set_index("hour_bucket")
            throughput_df.columns = ["✅ Completed", "❌ Failed"]

            st.bar_chart(throughput_df, color=["#002D62", "#EF4444"], height=280)
        else:
            st.info(
                "No throughput data yet for the last 12 hours. The chart will populate once files are processed."
            )

    # ── Recent Activity Feed ───────────────────────────────────────────────────
    with section_card("Recent Activity — Last 20 Events", "🕒"):
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
            st.dataframe(zebra(recent_df), width="stretch", hide_index=True)
        else:
            st.info(
                "No recent events. The audit log will populate once the pipeline starts processing files."
            )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 2 — MACHINE REGISTRY
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Machine Registry":
    st.markdown(
        '<div class="pillar-title">🖥️ Machine Registry</div>', unsafe_allow_html=True
    )

    with section_card("CMM / SPO Machine Directory", "🖥️"):
        search = st.text_input("🔍 Search by Machine ID", placeholder="e.g. KTSF-0453")

        machines_df = run_query(
            "SELECT * FROM fs3.v_machine_health ORDER BY machine_id;"
        )

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
                    c3.metric(
                        "Total Processed", int(row.get("total_processed", 0) or 0)
                    )
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

elif page == "Live File Queue":
    st.markdown(
        '<div class="pillar-title">📂 Live File Queue</div>', unsafe_allow_html=True
    )

    with section_card("Queue Filters", "🔍"):
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

    with section_card(
        "State Machine — PENDING → PROCESSING → COMPLETED (or FAILED)", "🔄"
    ):
        p1, p2, p3, p4 = st.columns(4)
        for col, s, stat in [
            (p1, "PENDING", "neutral"),
            (p2, "PROCESSING", "neutral"),
            (p3, "COMPLETED", "ok"),
            (p4, "FAILED", "error"),
        ]:
            count_df = run_query(
                f"SELECT COUNT(*) AS cnt FROM fs3.etl_pipeline_log "
                f"WHERE status = %s AND started_at >= NOW() - INTERVAL '{hours_back} hours';",
                params=(s,),
            )
            cnt = 0 if count_df.empty else int(count_df.iloc[0]["cnt"])
            with col:
                kpi_card(
                    s,
                    cnt,
                    status=stat if not (s == "FAILED" and cnt == 0) else "neutral",
                )

    with section_card(f"File Queue — Last {hours_back}h", "📂"):
        if queue_df.empty:
            st.info(
                f"No files found for the selected filters in the last {hours_back} hours."
            )
        else:
            st.dataframe(zebra(queue_df), width="stretch", hide_index=True, height=500)
            st.caption(
                f"Showing {len(queue_df)} file(s). Use filters above to narrow down."
            )


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE 4 — DEAD LETTER QUEUE
# ═══════════════════════════════════════════════════════════════════════════════

elif page == "Dead Letter Queue":
    st.markdown(
        '<div class="pillar-title">💀 Dead Letter Queue (DLQ)</div>',
        unsafe_allow_html=True,
    )

    with section_card("DLQ Filters", "🔍"):
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

    with section_card("Failed Files Awaiting Review", "💀"):
        if dlq_df.empty:
            st.success(
                "✅ Dead Letter Queue is clear — no failed files awaiting review."
            )
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

elif page == "Audit Logs":
    st.markdown('<div class="pillar-title">📋 Audit Logs</div>', unsafe_allow_html=True)

    with section_card("Log Filters", "🔍"):
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

    with section_card(f"Structured Log — Last {log_hours}h", "📋"):
        st.caption(f"Showing up to 500 entries from the last {log_hours} hours.")

        if log_df.empty:
            st.info("No log entries matching the current filters.")
        else:
            # NOTE: column_config formatting is used here, so this table is left
            # unstyled by the zebra() helper — combining a pandas Styler with
            # column_config is unreliable across Streamlit versions, and this
            # table's per-column widths/types matter more than row striping.
            st.dataframe(
                log_df,
                width="stretch",
                hide_index=True,
                height=600,
                column_config={
                    "level": st.column_config.TextColumn("Level", width="small"),
                    "machine_id": st.column_config.TextColumn(
                        "Machine", width="medium"
                    ),
                    "vehicle_model": st.column_config.TextColumn(
                        "Model", width="small"
                    ),
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

elif page == "Settings":
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
