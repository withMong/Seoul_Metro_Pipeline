"""파이프라인 freshness (페이지) — 수신 heartbeat / 끊김."""
import sys

sys.path.append("/app")

import altair as alt
import pandas as pd
import streamlit as st

from lib import FR, banner, inject_css, line_scale, q

st.set_page_config(page_title="파이프라인 freshness", page_icon="🚇", layout="wide")
inject_css()
banner("③ 파이프라인 freshness",
       "분당 수신 heartbeat. 수집 윈도우 동안 선이 끊김 없이 채워지면 파이프라인 건강.")

try:
    f1, f2 = st.columns([4, 1])
    with f2:
        hiccup = q(
            f"""
            SELECT SUM(CASE WHEN gap_min > 1.5 AND gap_min <= 30 THEN 1 ELSE 0 END) AS hiccups
            FROM (
              SELECT (unix_timestamp(minute_ts)
                - unix_timestamp(LAG(minute_ts) OVER (PARTITION BY line ORDER BY minute_ts)))/60.0 AS gap_min
              FROM {FR}
            ) t WHERE gap_min IS NOT NULL
            """
        )
        h = int(hiccup.iloc[0]["hiccups"] or 0)
        st.metric("윈도우 내 끊김", f"{h} 건", "0이면 건강", delta_color="off")
    with f1:
        fr = q(f"SELECT line, CAST(minute_ts AS CHAR) AS minute_ts, records FROM {FR} ORDER BY minute_ts")
        if not fr.empty:
            fr["minute_ts"] = pd.to_datetime(fr["minute_ts"])
            chart = (
                alt.Chart(fr)
                .mark_line()
                .encode(
                    x=alt.X("minute_ts:T", title=None),
                    y=alt.Y("records:Q", title="분당 수신 레코드"),
                    color=alt.Color("line:N", scale=line_scale(), title="노선"),
                    tooltip=["line", "minute_ts:T", "records"],
                )
                .properties(height=380)
            )
            st.altair_chart(chart, use_container_width=True)
    st.caption("윈도우 사이 빈 구간은 정상(러시아워만 수집). 윈도우 *내부* 끊김이 0이면 파이프라인 건강.")
except Exception as e:  # noqa: BLE001
    st.error(f"조회 실패: {e}")
