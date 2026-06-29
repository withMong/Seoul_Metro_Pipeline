"""headway 마트 데이터 품질 — Great Expectations(1.x) + Spark 직접 체크.

WAP 의 audit 단계(=staging 검증)와 Jupyter 탐색(=gold 검증)에서 공통으로 쓴다.
`ns`(staging/gold)만 바꿔 같은 Expectation 을 적용.
"""
from __future__ import annotations

import great_expectations as gx
from great_expectations import expectations as gxe

TABLE_KEYS = ["headway", "freshness"]

# 네임스페이스별 테이블 FQN (staging 과 gold 테이블명이 달라 명시적으로 둠)
STAGING = {
    "headway": "iceberg.staging.headway_by_station_tod",
    "freshness": "iceberg.staging.service_freshness",
}
GOLD = {
    "headway": "iceberg.gold.subway_headway_by_station_tod",
    "freshness": "iceberg.gold.subway_service_freshness",
}


def fqn(table_key: str, ns: str = "staging") -> str:
    return (STAGING if ns == "staging" else GOLD)[table_key]


def expectations_for(table_key: str) -> list:
    """테이블별 Expectation 목록."""
    if table_key == "headway":
        return [
            gxe.ExpectTableRowCountToBeBetween(min_value=1),
            gxe.ExpectColumnValuesToNotBeNull(column="statn_id"),
            gxe.ExpectColumnValuesToNotBeNull(column="direction"),
            gxe.ExpectColumnValuesToBeInSet(column="direction", value_set=["상행", "하행"]),
            gxe.ExpectColumnValuesToBeInSet(
                column="time_band", value_set=["새벽", "출근", "점심", "퇴근", "밤"]
            ),
            gxe.ExpectColumnValuesToBeBetween(column="cv", min_value=0, max_value=3),
            gxe.ExpectColumnValuesToBeBetween(column="p50_sec", min_value=1, max_value=900),
            gxe.ExpectColumnValuesToBeBetween(column="headway_samples", min_value=1),
            gxe.ExpectColumnValuesToBeBetween(column="over_1p5x_ratio", min_value=0, max_value=1),
        ]
    if table_key == "freshness":
        return [
            gxe.ExpectTableRowCountToBeBetween(min_value=1),
            gxe.ExpectColumnValuesToBeBetween(column="records", min_value=1),
            gxe.ExpectColumnValuesToBeBetween(column="distinct_trains", min_value=1),
        ]
    return []


def run_ge_suite(context, spark, table_key: str, ns: str = "staging"):
    """해당 네임스페이스 테이블에 Expectation Suite 실행 → 검증 결과."""
    df = spark.table(fqn(table_key, ns))
    ds = context.data_sources.add_spark(name=f"sp_{ns}_{table_key}")
    asset = ds.add_dataframe_asset(name=f"{ns}_{table_key}")
    batch_def = asset.add_batch_definition_whole_dataframe(name="whole")
    suite = context.suites.add(gx.ExpectationSuite(name=f"{ns}_{table_key}_suite"))
    for e in expectations_for(table_key):
        suite.add_expectation(e)
    return batch_def.get_batch(batch_parameters={"dataframe": df}).validate(suite)


def summarize_result(res) -> list[dict]:
    """검증 결과를 표 형태(dict 리스트)로."""
    return [
        {
            "expectation": r.expectation_config.type,
            "column": r.expectation_config.kwargs.get("column"),
            "success": r.success,
            "unexpected_count": (r.result or {}).get("unexpected_count"),
        }
        for r in res.results
    ]


def run_custom_checks(spark, ns: str = "staging") -> list[dict]:
    """GE 로 표현하기 번거로운 '행간 일관성' 규칙을 Spark SQL 로 직접 검사."""
    HW = fqn("headway", ns)
    checks = []

    r = spark.sql(
        f"SELECT COUNT(*) AS n, SUM(CASE WHEN p90_sec < p50_sec THEN 1 ELSE 0 END) AS v FROM {HW}"
    ).first()
    checks.append({"check": "p90_sec >= p50_sec", "checked": r["n"], "violations": r["v"]})

    r = spark.sql(
        f"SELECT COUNT(*) AS n, SUM(CASE WHEN mean_sec <= 0 THEN 1 ELSE 0 END) AS v FROM {HW}"
    ).first()
    checks.append({"check": "mean_sec > 0", "checked": r["n"], "violations": r["v"]})

    # (역,방향,시간대,요일) 조합 유일성 = 마트 grain 보장
    r = spark.sql(
        f"""
        SELECT COUNT(*) AS n, SUM(c - 1) AS v FROM (
          SELECT COUNT(*) AS c FROM {HW}
          GROUP BY line, statn_id, direction, time_band, day_type
        )
        """
    ).first()
    checks.append({"check": "grain 유일성(중복 그룹 없음)", "checked": r["n"], "violations": r["v"]})

    return checks
