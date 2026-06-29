"""배차 불안정 역 Top N (페이지)."""
import sys

sys.path.append("/app")

import altair as alt
import streamlit as st

from lib import HW, banner, hw_filters, inject_css, line_scale, q

st.set_page_config(page_title="불안정 역 Top N", page_icon="🚇", layout="wide")
inject_css()
banner("② 배차 불안정 역 Top N",
       "CV(변동계수)=표준편차/평균. 높을수록 그 역·방향·시간대 배차가 들쭉날쭉.")

where = hw_filters()
topn = st.sidebar.slider("Top N", 5, 50, 20)

try:
    top = q(
        f"""
        SELECT line, statn_nm, direction, time_band, headway_samples AS n,
               p50_sec, p90_sec, cv, over_1p5x_ratio
        FROM {HW} WHERE {where}
        ORDER BY cv DESC LIMIT {topn}
        """
    )
    if top.empty:
        st.warning("조건에 맞는 데이터가 없어요. 필터(최소 표본 수 등)를 낮춰보세요.")
    else:
        top["역(방향·시간대)"] = top["statn_nm"] + " (" + top["direction"] + "·" + top["time_band"] + ")"
        chart = (
            alt.Chart(top)
            .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
            .encode(
                y=alt.Y("역(방향·시간대):N", sort="-x", title=None),
                x=alt.X("cv:Q", title="CV"),
                color=alt.Color("line:N", scale=line_scale(), title="노선"),
                tooltip=["line", "statn_nm", "direction", "time_band", "n", "p50_sec", "p90_sec", "cv"],
            )
            .properties(height=max(320, len(top) * 26))
        )
        st.altair_chart(chart, use_container_width=True)
        st.dataframe(
            top.drop(columns=["역(방향·시간대)"]),
            use_container_width=True, hide_index=True,
        )
except Exception as e:  # noqa: BLE001
    st.error(f"조회 실패: {e}")
