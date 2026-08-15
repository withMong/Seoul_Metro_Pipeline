"""정시성 — 계획 배차 대비 (9호선). [개선] 시간표 조인.

관측 배차(gold headway)와 서울 열린데이터 9호선 시간표(계획 배차)를 조인해,
'계획 대비 실제 배차가 얼마나 벌어졌나(vs_plan_ratio)'를 본다.
  1.0 ≈ 계획대로 · >1 계획보다 벌어짐(지연·번칭) · <1 계획보다 촘촘
"""
import sys

sys.path.append("/app")

import altair as alt
import pandas as pd
import streamlit as st

from lib import banner, inject_css, q

PUNCT = "iceberg_catalog.gold.subway_punctuality_9line"

st.set_page_config(page_title="정시성 (계획 대비)", page_icon="🕒", layout="wide")
inject_css()
banner(
    "③ 정시성 — 계획 배차 대비 (9호선)",
    "vs_plan_ratio = 관측 배차(P50) ÷ 계획 배차(P50). 1.0=계획대로, 값이 클수록 계획보다 배차가 벌어짐(지연).",
    badge="시간표 조인 · 9호선 (1·2호선 확장 예정)",
)

st.sidebar.header("필터")
svc = st.sidebar.selectbox("서비스", ["전체", "급행", "완행"])
band = st.sidebar.selectbox("시간대", ["출퇴근", "출근", "퇴근", "전체"])
min_n = st.sidebar.slider("최소 표본 수", 3, 30, 5)
topn = st.sidebar.slider("Top N", 5, 40, 15)

BANDF = {
    "출퇴근": "time_band IN ('출근','퇴근')",
    "출근": "time_band='출근'",
    "퇴근": "time_band='퇴근'",
    "전체": "1=1",
}[band]
where = f"svc_type='{svc}' AND {BANDF} AND headway_samples>={min_n}"

# ── KPI: 급행 vs 완행 계획대비 평균 (출퇴근) ──
try:
    cmp = q(
        f"""
        SELECT svc_type, ROUND(AVG(vs_plan_ratio),2) AS avg_vs_plan,
               ROUND(AVG(obs_p50_sec),0) AS avg_obs, ROUND(AVG(plan_p50_sec),0) AS avg_plan
        FROM {PUNCT}
        WHERE svc_type IN ('급행','완행') AND time_band IN ('출근','퇴근') AND headway_samples>=3
        GROUP BY svc_type
        """
    )

    def getv(s, col):
        r = cmp[cmp.svc_type == s]
        return r[col].iloc[0] if not r.empty else None

    c1, c2, c3 = st.columns(3)
    c1.metric("급행 · 계획대비", f"{getv('급행','avg_vs_plan')}×",
              help="출퇴근 평균 · 관측 배차 ÷ 계획 배차")
    c2.metric("완행 · 계획대비", f"{getv('완행','avg_vs_plan')}×")
    c3.metric("판정 기준", "≤1.15 정시 · ≤1.5 지연경향 · >1.5 큰지연")
except Exception as e:  # noqa: BLE001
    st.info(f"급행/완행 요약 생략: {e}")

st.markdown("### 계획 배차 대비 지연 큰 역 Top")

# ── 메인: 계획 대비 지연(vs_plan_ratio) 큰 역 ──
try:
    df = q(
        f"""
        SELECT statn_nm, direction, time_band, headway_samples AS n,
               obs_p50_sec, plan_p50_sec, vs_plan_ratio, excess_sec, punctuality
        FROM {PUNCT} WHERE {where}
        ORDER BY vs_plan_ratio DESC LIMIT {topn}
        """
    )
    if df.empty:
        st.warning("조건에 맞는 데이터가 없어요. 필터(최소 표본 수 등)를 낮춰보세요.")
    else:
        df["역(방향·시간대)"] = df.statn_nm + " (" + df.direction + "·" + df.time_band + ")"
        color = alt.Color(
            "punctuality:N",
            scale=alt.Scale(domain=["정시", "지연경향", "큰지연"],
                            range=["#1E8A5A", "#C7811A", "#C0392B"]),
            title="판정",
        )
        bars = (
            alt.Chart(df)
            .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
            .encode(
                y=alt.Y("역(방향·시간대):N", sort="-x", title=None),
                x=alt.X("vs_plan_ratio:Q", title="계획 대비 배차 (×)"),
                color=color,
                tooltip=["statn_nm", "direction", "time_band", "n",
                         "obs_p50_sec", "plan_p50_sec", "vs_plan_ratio", "punctuality"],
            )
            .properties(height=max(320, len(df) * 28))
        )
        rule = (
            alt.Chart(pd.DataFrame({"x": [1.0]}))
            .mark_rule(color="#8b949e", strokeDash=[4, 4])
            .encode(x="x:Q")
        )
        st.altair_chart(bars + rule, use_container_width=True)
        st.dataframe(df.drop(columns=["역(방향·시간대)"]),
                     use_container_width=True, hide_index=True)
        st.caption(
            "계획 배차 = 서울 열린데이터 9호선 시간표(평일)에서 산출한 P50 배차. "
            "점선(1.0)=계획대로. vs_plan_ratio>1 은 관측 배차가 계획보다 벌어진(지연·번칭) 상태."
        )
except Exception as e:  # noqa: BLE001
    st.error(f"조회 실패: {e}")
