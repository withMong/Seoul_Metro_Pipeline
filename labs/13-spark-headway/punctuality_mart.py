#!/usr/bin/env python3
"""[개선] 9호선 정시성 마트 — '계획 배차 대비' 관측 배차.

기존 over_1p5x_ratio 는 '관측 중앙값' 대비였다(스킵으로 부푼 간격과 진짜 긴 배차를
완벽히 구분 못 하는 한계). 여기서는 **시간표(계획 배차)** 를 기준으로 바꾼다:

  vs_plan_ratio = 관측 배차(P50) / 계획 배차(P50)
    1.0 ≈ 계획대로 · >1 배차가 계획보다 벌어짐(지연·번칭) · <1 계획보다 촘촘

입력:
  - iceberg.gold.subway_headway_by_station_tod  (관측, line='9호선', day_type='평일')
  - data/timetable/plan_headway_9line.csv       (계획 배차: 시간표에서 산출한 역×방향×시간대×서비스 P50)
      → svc_type 전체/급행(D)/완행(G) 모두 있어 관측 svc_type 과 그대로 조인.
산출:
  iceberg.gold.subway_punctuality_9line

주의: 9호선만(시간표 확보분). 1·2호선은 시간표 수집 후 확장 예정.
"""
import os
import sys

from pyspark.sql import SparkSession

PLAN_CSV = os.getenv("PLAN_CSV", "file:///workspace/data/timetable/plan_headway_9line.csv")
OUT = "iceberg.gold.subway_punctuality_9line"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("subway-punctuality-9line")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def run(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.gold")

    plan = spark.read.option("header", True).csv(PLAN_CSV)
    plan.createOrReplaceTempView("plan_raw")
    spark.sql(
        """
        CREATE OR REPLACE TEMP VIEW plan AS
        SELECT statn_nm, direction, time_band, svc_type,
               CAST(plan_p50_sec AS DOUBLE) AS plan_p50_sec, CAST(plan_n AS INT) AS plan_n
        FROM plan_raw
        """
    )

    # 관측(9호선·평일) × 계획 조인 → 계획 대비 지표
    spark.sql(
        f"""
        CREATE OR REPLACE TABLE {OUT} AS
        SELECT
          o.line, o.statn_nm, o.direction, o.svc_type, o.time_band,
          o.headway_samples, o.p50_sec AS obs_p50_sec, o.cv,
          p.plan_p50_sec,
          ROUND(o.p50_sec / NULLIF(p.plan_p50_sec, 0), 2)         AS vs_plan_ratio,
          ROUND(o.p50_sec - p.plan_p50_sec, 0)                    AS excess_sec,
          CASE
            WHEN o.p50_sec / NULLIF(p.plan_p50_sec,0) <= 1.15 THEN '정시'
            WHEN o.p50_sec / NULLIF(p.plan_p50_sec,0) <= 1.5  THEN '지연경향'
            ELSE '큰지연'
          END AS punctuality
        FROM (SELECT * FROM iceberg.gold.subway_headway_by_station_tod
              WHERE line='9호선' AND day_type='평일') o
        JOIN plan p
          ON o.statn_nm=p.statn_nm AND o.direction=p.direction
         AND o.time_band=p.time_band AND o.svc_type=p.svc_type
        """
    )
    n = spark.table(OUT).count()
    print(f"[punctuality] {OUT} 적재 완료 · {n} 행 (9호선·평일)")

    print("\n=== 계획 배차 대비 정시성 나쁜 역 Top 15 (전체·출퇴근, 표본>=5) ===")
    spark.sql(
        f"""
        SELECT statn_nm, direction, time_band, headway_samples AS n,
               obs_p50_sec, plan_p50_sec, vs_plan_ratio, punctuality
        FROM {OUT}
        WHERE svc_type='전체' AND time_band IN ('출근','퇴근') AND headway_samples>=5
        ORDER BY vs_plan_ratio DESC LIMIT 15
        """
    ).show(50, truncate=False)

    print("\n=== 급행 vs 완행 — 계획 대비 평균 (출퇴근, 표본>=5) ===")
    spark.sql(
        f"""
        SELECT svc_type, COUNT(*) AS groups,
               ROUND(AVG(vs_plan_ratio),2) AS avg_vs_plan,
               ROUND(AVG(obs_p50_sec),0) AS avg_obs, ROUND(AVG(plan_p50_sec),0) AS avg_plan
        FROM {OUT}
        WHERE svc_type IN ('급행','완행') AND time_band IN ('출근','퇴근') AND headway_samples>=5
        GROUP BY svc_type ORDER BY svc_type
        """
    ).show(truncate=False)


def main() -> None:
    spark = build_spark()
    try:
        run(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
