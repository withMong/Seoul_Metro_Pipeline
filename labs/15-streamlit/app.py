"""서울 지하철 배차 안정성 BI — 개요(홈). KPI + 노선 구조 스토리."""
import sys

sys.path.append("/app")

import altair as alt
import streamlit as st

from lib import HW, TB_ORDER, banner, hw_filters, inject_css, line_scale, q

st.set_page_config(page_title="배차 안정성 — 개요", page_icon="🚇", layout="wide")
inject_css()
banner(
    "🚇 서울 지하철 배차 안정성 — 1·2·9호선",
    "중점 질문: 출퇴근 시간대에 어느 노선·역·방향에서 배차 간격(headway)이 불안정한가?  ·  CV↑ = 불안정",
    "✅ WAP(Write-Audit-Publish) + Great Expectations 검증 통과 gold",
)

where = hw_filters()

try:
    # ── KPI 카드 ──
    n_groups = int(q(f"SELECT COUNT(*) AS n FROM {HW} WHERE {where}").iloc[0]["n"])
    worst_line = q(f"SELECT line, ROUND(AVG(cv),3) AS c FROM {HW} WHERE {where} GROUP BY line ORDER BY c DESC LIMIT 1")
    worst_st = q(f"SELECT statn_nm, line, direction, cv FROM {HW} WHERE {where} ORDER BY cv DESC LIMIT 1")

    k1, k2, k3 = st.columns(3)
    k1.metric("분석 그룹 (역×방향×시간대)", f"{n_groups:,}")
    if not worst_line.empty:
        k2.metric("가장 불안정 노선", worst_line.iloc[0]["line"], f"평균 CV {worst_line.iloc[0]['c']}")
    if not worst_st.empty:
        w = worst_st.iloc[0]
        k3.metric("최불안정 역", f"{w['statn_nm']} · {w['line']}", f"CV {w['cv']} · {w['direction']}")

    st.write("")
    # ── 노선 구조 스토리 ──
    st.subheader("노선·시간대별 배차 변동성 (평균 CV)")
    cv = q(
        f"""
        SELECT line, time_band, ROUND(AVG(cv),3) AS avg_cv,
               ROUND(AVG(p50_sec),0) AS avg_headway_sec, COUNT(*) AS n_groups
        FROM {HW} WHERE {where}
        GROUP BY line, time_band ORDER BY line, time_band
        """
    )
    if not cv.empty:
        chart = (
            alt.Chart(cv)
            .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
            .encode(
                x=alt.X("time_band:N", sort=TB_ORDER, title=None, axis=alt.Axis(labelAngle=0)),
                xOffset=alt.XOffset("line:N"),
                y=alt.Y("avg_cv:Q", title="평균 CV"),
                color=alt.Color("line:N", scale=line_scale(), title="노선"),
                tooltip=["line", "time_band", "avg_cv", "avg_headway_sec", "n_groups"],
            )
            .properties(height=380)
        )
        st.altair_chart(chart, use_container_width=True)
    st.info("**순환선(2호선) 안정 · 급행혼용(9호선) 중간 · 분기노선(1호선) 불안정** "
            "— 노선 구조가 배차 안정성을 좌우한다.")
except Exception as e:  # noqa: BLE001
    st.error(f"조회 실패: {e}")
    st.caption("StarRocks(CN ALIVE)·iceberg_catalog·gold 마트가 준비됐는지 확인하세요.")
