#!/usr/bin/env python3
"""WAP (Write-Audit-Publish) — headway/freshness 마트를 검증 통과 후에만 서빙으로.

단계(--stage)로 분리, Airflow 가 별 task 로 호출:
  write    : silver/bronze → iceberg.staging.* 에 마트 적재 (서빙 아님)
  audit    : staging 데이터 품질 검사. 실패하면 exit 1 → 이후 단계 중단(=gold 안 바뀜)
  publish  : 감사 통과 시 staging → iceberg.gold.* 원자적 교체(CREATE OR REPLACE)
  validate : gold 가 staging 과 일치하는지 사후 확인

핵심: 나쁜 데이터는 절대 gold(서빙)에 도달하지 않는다.
"""
import os
import sys

from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from line_rules import BRANCH_2, EDGE_1, sql_in  # noqa: E402

BRANCH_IN = sql_in(BRANCH_2)
EDGE_IN = sql_in(EDGE_1)
MAX_HEADWAY_SEC = 900

HW_STAGE = "iceberg.staging.headway_by_station_tod"
HW_GOLD = "iceberg.gold.subway_headway_by_station_tod"
FR_STAGE = "iceberg.staging.service_freshness"
FR_GOLD = "iceberg.gold.subway_service_freshness"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("headway-wap")
        .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
        .config("spark.sql.catalog.paimon.warehouse", "s3://paimon/warehouse")
        .config("spark.sql.catalog.paimon.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.paimon.s3.access-key", "minioadmin")
        .config("spark.sql.catalog.paimon.s3.secret-key", "minioadmin")
        .config("spark.sql.catalog.paimon.s3.path.style.access", "true")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions",
        )
        .getOrCreate()
    )


def write(spark: SparkSession) -> None:
    """staging 에 headway/freshness 마트 적재 (서빙 아님)."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.staging")

    # ── headway ──
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW arrivals AS
        SELECT line, statn_id, statn_nm, updn_line, arrival_ts,
          CASE updn_line WHEN '0' THEN '상행' WHEN '1' THEN '하행' ELSE '미상' END AS direction,
          CASE WHEN line='2호선' AND statn_nm IN ({BRANCH_IN}) THEN '지선' ELSE '본선' END AS branch,
          CASE WHEN hour(arrival_ts) BETWEEN 5 AND 6  THEN '새벽'
               WHEN hour(arrival_ts) BETWEEN 7 AND 9  THEN '출근'
               WHEN hour(arrival_ts) BETWEEN 10 AND 16 THEN '점심'
               WHEN hour(arrival_ts) BETWEEN 17 AND 19 THEN '퇴근'
               ELSE '밤' END AS time_band,
          CASE dayofweek(arrival_ts) WHEN 1 THEN '휴일' WHEN 7 THEN '토요일' ELSE '평일' END AS day_type
        FROM paimon.silver.subway_arrival_events WHERE updn_line IN ('0','1')
        """
    )
    # 연속 도착 간격을 status 로 분류(조용히 버리지 않고 명시):
    #   nonpos      = 음수/0 (추정역전·중복) → 이상치, 통계에서 제외
    #   gap_missing = MAX 초과 (열차 스킵/윈도우 경계) → '결측'(0 아님), 통계에서 제외
    #   valid       = 0 < h <= MAX → P50/P90/CV 계산에 사용
    spark.sql(
        f"""
        CREATE OR REPLACE TEMP VIEW headways_cls AS
        SELECT *,
          CASE
            WHEN headway_sec IS NULL        THEN 'first'
            WHEN headway_sec <= 0           THEN 'nonpos'
            WHEN headway_sec > {MAX_HEADWAY_SEC} AND line='1호선' AND statn_nm IN ({EDGE_IN})
                                            THEN 'normal_term'
            WHEN headway_sec > {MAX_HEADWAY_SEC} THEN 'gap_missing'
            ELSE 'valid'
          END AS status
        FROM (
          SELECT line, statn_id, statn_nm, direction, branch, time_band, day_type,
            unix_timestamp(arrival_ts) - unix_timestamp(
              LAG(arrival_ts) OVER (PARTITION BY line, statn_id, direction ORDER BY arrival_ts)
            ) AS headway_sec
          FROM arrivals
        )
        """
    )
    spark.sql("CREATE OR REPLACE TEMP VIEW headways AS SELECT * FROM headways_cls WHERE status='valid'")
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW grp_med AS
        SELECT line, statn_id, direction, time_band, day_type,
               percentile_approx(headway_sec, 0.5) AS grp_p50
        FROM headways GROUP BY line, statn_id, direction, time_band, day_type
        """
    )
    # 결측·이상 건수(같은 grain) — 0 으로 뭉개지 않고 컬럼으로 노출
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW grp_excl AS
        SELECT line, statn_id, direction, time_band, day_type,
               SUM(CASE WHEN status='gap_missing' THEN 1 ELSE 0 END) AS n_missing,
               SUM(CASE WHEN status='nonpos' THEN 1 ELSE 0 END)      AS n_anomaly
        FROM headways_cls GROUP BY line, statn_id, direction, time_band, day_type
        """
    )
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {HW_STAGE} AS
        SELECT h.line, h.statn_id, MAX(h.statn_nm) AS statn_nm, h.direction, h.time_band, h.day_type,
          MAX(h.branch) AS branch,
          CASE WHEN h.time_band IN ('출근','퇴근') THEN true ELSE false END AS is_rush,
          COUNT(*) AS headway_samples,
          ROUND(percentile_approx(h.headway_sec,0.5),0) AS p50_sec,
          ROUND(percentile_approx(h.headway_sec,0.9),0) AS p90_sec,
          ROUND(AVG(h.headway_sec),0) AS mean_sec,
          ROUND(STDDEV(h.headway_sec),1) AS std_sec,
          ROUND(STDDEV(h.headway_sec)/NULLIF(AVG(h.headway_sec),0),3) AS cv,
          ROUND(AVG(CASE WHEN h.headway_sec > 1.5*g.grp_p50 THEN 1 ELSE 0 END),3) AS over_1p5x_ratio,
          COALESCE(MAX(e.n_missing),0) AS n_missing,
          COALESCE(MAX(e.n_anomaly),0) AS n_anomaly
        FROM headways h JOIN grp_med g
          ON h.line=g.line AND h.statn_id=g.statn_id AND h.direction=g.direction
         AND h.time_band=g.time_band AND h.day_type=g.day_type
        LEFT JOIN grp_excl e
          ON h.line=e.line AND h.statn_id=e.statn_id AND h.direction=e.direction
         AND h.time_band=e.time_band AND h.day_type=e.day_type
        GROUP BY h.line, h.statn_id, h.direction, h.time_band, h.day_type
        """
    )

    # ── freshness ──
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {FR_STAGE} AS
        SELECT line, date_trunc('minute', CAST(recptn_dt AS TIMESTAMP)) AS minute_ts,
          COUNT(*) AS records, COUNT(DISTINCT train_no) AS distinct_trains,
          ROUND(AVG(unix_timestamp(ingested_at) - unix_timestamp(CAST(recptn_dt AS TIMESTAMP))),1) AS ingest_lag_avg_sec
        FROM paimon.bronze.subway_position_log
        WHERE recptn_dt IS NOT NULL AND recptn_dt <> ''
        GROUP BY line, date_trunc('minute', CAST(recptn_dt AS TIMESTAMP))
        """
    )
    print(f"[write] staging 적재: headway={spark.table(HW_STAGE).count()} / freshness={spark.table(FR_STAGE).count()}")


def audit(spark: SparkSession) -> None:
    """Great Expectations(1.x, Spark 데이터소스) + Spark 직접 일관성 체크로 staging 검증.

    하나라도 실패하면 exit 1 → publish 중단(=gold 안 바뀜).
    Expectation 정의는 labs/16-data-quality/data_quality_checks.py 로 분리(코스 패턴).
    """
    sys.path.insert(0, "/workspace/labs/16-data-quality")
    import great_expectations as gx
    from data_quality_checks import TABLE_KEYS, run_custom_checks, run_ge_suite, summarize_result

    context = gx.get_context(mode="ephemeral")
    ok = True

    # ── GE Expectation Suite (staging 테이블별) ──
    for key in TABLE_KEYS:
        res = run_ge_suite(context, spark, key)
        for r in summarize_result(res):
            mark = "PASS" if r["success"] else "FAIL"
            print(f"  {mark} [{key}] {r['expectation']} {r.get('column') or ''}".rstrip())
        ok = ok and bool(res.success)

    # ── Spark 직접 일관성 체크 (GE로 표현 어려운 행간 규칙) ──
    for c in run_custom_checks(spark):
        cok = c["violations"] == 0
        print(f"  {'PASS' if cok else 'FAIL'} [spark] {c['check']} (위반 {c['violations']}/{c['checked']})")
        ok = ok and cok

    if not ok:
        print("\n[audit/GE] ❌ 검증 실패 → publish 중단 (gold 유지)")
        sys.exit(1)
    print("\n[audit/GE] ✅ 모든 검증 통과 → publish 진행")


def publish(spark: SparkSession) -> None:
    """staging → gold 원자적 교체."""
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")
    spark.sql(f"CREATE OR REPLACE TABLE {HW_GOLD} AS SELECT * FROM {HW_STAGE}")
    spark.sql(f"CREATE OR REPLACE TABLE {FR_GOLD} AS SELECT * FROM {FR_STAGE}")
    print("[publish] staging → gold 교체 완료")


def validate(spark: SparkSession) -> None:
    """gold 가 staging 과 일치하는지 사후 확인."""
    pairs = [("headway", HW_GOLD, HW_STAGE), ("freshness", FR_GOLD, FR_STAGE)]
    bad = []
    for name, gold, stage in pairs:
        g, s = spark.table(gold).count(), spark.table(stage).count()
        ok = g == s and g > 0
        print(f"  {'PASS' if ok else 'FAIL'} {name}: gold={g} staging={s}")
        if not ok:
            bad.append(name)
    if bad:
        print(f"[validate] ❌ 불일치 {bad}")
        sys.exit(1)
    print("[validate] ✅ 서빙 검증 통과")


def main() -> None:
    stage = sys.argv[sys.argv.index("--stage") + 1] if "--stage" in sys.argv else "all"
    spark = build_spark()
    try:
        if stage in ("write", "all"):
            write(spark)
        if stage in ("audit", "all"):
            audit(spark)
        if stage in ("publish", "all"):
            publish(spark)
        if stage in ("validate", "all"):
            validate(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
