#!/usr/bin/env python3
"""품질 검증 러너 (gold 서빙 마트) — 헤드리스.

WAP audit 와 동일한 규칙(data_quality_checks)을 gold 에 직접 적용하고,
마지막에 '오염 주입 데모'로 검증이 나쁜 데이터를 실제로 막아내는지 보여준다.

  ① GE Expectation Suite (gold)   — not-null·in-set·range
  ② Spark 일관성 체크 (gold)       — p90≥p50, mean>0, grain 유일성
  ③ 오염 주입 데모                 — cv=99·direction='X' → success=False(차단) 확인

사용: spark-submit run_dq.py   (scripts/ops/dq-verify.sh 가 래핑)
"""
from __future__ import annotations

import sys

sys.path.insert(0, "/workspace/labs/16-data-quality")

import great_expectations as gx
from pyspark.sql import SparkSession, functions as F

from data_quality_checks import (
    GOLD,
    TABLE_KEYS,
    expectations_for,
    run_custom_checks,
    run_ge_suite,
    summarize_result,
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("subway-dq-verify")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "rest")
        .config("spark.sql.catalog.iceberg.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
        .config("spark.sql.catalog.iceberg.warehouse", "s3://warehouse/")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .getOrCreate()
    )


def validate_df(context, df, table_key: str, name: str):
    """임의 DataFrame 에 해당 테이블 Expectation 적용(오염 데모용)."""
    ds = context.data_sources.add_spark(name=f"sp_{name}")
    asset = ds.add_dataframe_asset(name=name)
    batch_def = asset.add_batch_definition_whole_dataframe(name="whole")
    suite = context.suites.add(gx.ExpectationSuite(name=f"{name}_suite"))
    for e in expectations_for(table_key):
        suite.add_expectation(e)
    return batch_def.get_batch(batch_parameters={"dataframe": df}).validate(suite)


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("ERROR")
    context = gx.get_context(mode="ephemeral")
    overall = True

    print("=" * 60)
    print("① GE Expectation Suite (gold)")
    print("=" * 60)
    for tk in TABLE_KEYS:
        res = run_ge_suite(context, spark, tk, ns="gold")
        ok = bool(res.success)
        overall = overall and ok
        print(f"\n[{tk}] success={ok}")
        for row in summarize_result(res):
            mark = "PASS" if row["success"] else "FAIL"
            print(
                f"  {mark}  {row['expectation']}"
                f"({row.get('column')})  unexpected={row.get('unexpected_count')}"
            )

    print("\n" + "=" * 60)
    print("② Spark 일관성 체크 (gold)")
    print("=" * 60)
    for c in run_custom_checks(spark, ns="gold"):
        ok = c["violations"] == 0
        overall = overall and ok
        mark = "PASS" if ok else "FAIL"
        print(f"  {mark}  {c['check']}: 위반 {c['violations']}/{c['checked']}")

    print("\n" + "=" * 60)
    print("③ 오염 주입 데모 — 검증이 막아내는가")
    print("=" * 60)
    bad = (
        spark.table(GOLD["headway"])
        .withColumn("cv", F.lit(99.0))          # cv ∈ [0,3] 위반
        .withColumn("direction", F.lit("X"))     # direction ∈ {상행,하행} 위반
    )
    bad_res = validate_df(context, bad, "headway", "contaminated")
    blocked = not bool(bad_res.success)
    print(f"  오염 배치 success = {bad_res.success}  →  "
          f"{'정상: 검증이 막아냄(차단)' if blocked else '⚠ 못 막음'}")

    print("\n" + "=" * 60)
    passed = overall and blocked
    print(f"종합: {'✅ gold 전체 PASS + 오염 차단' if passed else '⚠ 점검 필요'}")
    print("=" * 60)

    spark.stop()
    sys.exit(0 if passed else 1)


if __name__ == "__main__":
    main()
