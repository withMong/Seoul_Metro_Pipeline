"""Streamlit BI 공통 — StarRocks 연결 + 필터 + 스타일 (멀티페이지 재사용)."""
import os

import altair as alt
import pandas as pd
import pymysql
import streamlit as st

HOST = os.getenv("STARROCKS_HOST", "starrocks-fe")
PORT = int(os.getenv("STARROCKS_PORT", "9030"))
USER = os.getenv("STARROCKS_USER", "root")
HW = "iceberg_catalog.gold.subway_headway_by_station_tod"
FR = "iceberg_catalog.gold.subway_service_freshness"

# 서울 지하철 노선색
LINE_COLORS = {"1호선": "#0052A4", "2호선": "#00A84D", "9호선": "#BDB092"}
TB_ORDER = ["새벽", "출근", "점심", "퇴근", "밤"]


def line_scale() -> alt.Scale:
    return alt.Scale(domain=list(LINE_COLORS), range=list(LINE_COLORS.values()))


@st.cache_data(ttl=60)
def q(sql: str) -> pd.DataFrame:
    conn = pymysql.connect(host=HOST, port=PORT, user=USER, password="", charset="utf8mb4")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        return pd.DataFrame(rows, columns=cols)
    finally:
        conn.close()


def hw_filters() -> str:
    st.sidebar.header("필터")
    line = st.sidebar.selectbox("노선", ["(전체)", "1호선", "2호선", "9호선"])
    band = st.sidebar.selectbox("시간대", ["(전체)", "출근", "점심", "퇴근", "새벽", "밤"])
    min_n = st.sidebar.slider("최소 표본 수", 3, 30, 10)
    lf = "" if line == "(전체)" else f" AND line = '{line}'"
    bf = "" if band == "(전체)" else f" AND time_band = '{band}'"
    return f"headway_samples >= {min_n}{lf}{bf}"


def inject_css() -> None:
    st.markdown(
        """
        <style>
          #MainMenu, footer {visibility: hidden;}
          .block-container {padding-top: 1.6rem; padding-bottom: 2rem; max-width: 1200px;}
          /* KPI 카드 */
          div[data-testid="stMetric"] {
            background: #F7F9FC; border: 1px solid #E6EAF2; border-radius: 14px;
            padding: 16px 20px; box-shadow: 0 1px 2px rgba(16,24,40,.05);
          }
          div[data-testid="stMetric"] label p {color:#667085; font-weight:600;}
          div[data-testid="stMetricValue"] {font-size: 1.5rem; color:#101828; font-weight:700;}
          /* 배너 */
          .sm-banner {
            background: linear-gradient(135deg,#0052A4 0%,#3A7BD5 100%);
            color:#fff; padding: 22px 26px; border-radius: 18px; margin-bottom: 14px;
            box-shadow: 0 6px 18px rgba(0,82,164,.18);
          }
          .sm-banner h1 {margin:0; color:#fff; font-size: 1.55rem; font-weight: 800;}
          .sm-banner p {margin: 8px 0 0; opacity:.92; font-size: .95rem;}
          .sm-badge {
            display:inline-block; background:#E7F5EC; color:#067647;
            border:1px solid #ABEFC6; border-radius: 999px;
            padding: 4px 12px; font-size:.82rem; font-weight:600; margin-top: 4px;
          }
          h2, h3 {color:#101828;}
        </style>
        """,
        unsafe_allow_html=True,
    )


def banner(title: str, subtitle: str, badge: str | None = None) -> None:
    badge_html = f"<div class='sm-badge'>{badge}</div>" if badge else ""
    st.markdown(
        f"<div class='sm-banner'><h1>{title}</h1><p>{subtitle}</p>{badge_html}</div>",
        unsafe_allow_html=True,
    )
