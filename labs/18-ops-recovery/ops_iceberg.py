#!/usr/bin/env python3
"""운영 복구 도구 (Iceberg gold 마트) — R5 / R5b 드릴.

headway_mart.py 와 동일한 Iceberg REST 카탈로그(`iceberg`)를 재사용한다.

actions
  snapshots : 최근 스냅샷 목록(복구 지점 찾기)             [R5]
  empty     : 마트 행만 비움(DELETE) — 'DAG 성공인데 BI 공백' 재현  [R5 주입]
  rollback  : 특정 스냅샷으로 메타데이터 포인터 롤백        [R5b]

사용:
  spark-submit ops_iceberg.py --action snapshots
  spark-submit ops_iceberg.py --action empty
  spark-submit ops_iceberg.py --action rollback --snapshot-id <ID>
"""
from __future__ import annotations

import argparse
from pyspark.sql import SparkSession


def build_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("subway-ops-iceberg")
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--action", required=True, choices=["snapshots", "empty", "rollback"])
    ap.add_argument("--table", default="gold.subway_headway_by_station_tod",
                    help="iceberg 카탈로그 기준 namespace.table")
    ap.add_argument("--snapshot-id", type=int)
    args = ap.parse_args()

    spark = build_spark()
    fq = f"iceberg.{args.table}"

    if args.action == "snapshots":
        print(f"== 최근 스냅샷: {fq} ==")
        spark.sql(
            f"""
            SELECT committed_at, snapshot_id, operation,
                   summary['total-records'] AS total_records
            FROM {fq}.snapshots
            ORDER BY committed_at DESC
            LIMIT 15
            """
        ).show(truncate=False)
        print("복구 지점(snapshot_id)을 골라 rollback 에 사용:")
        print(f"  spark-submit ops_iceberg.py --action rollback --snapshot-id <ID>")

    elif args.action == "empty":
        before = spark.table(fq).count()
        spark.sql(f"DELETE FROM {fq} WHERE true")
        after = spark.table(fq).count()
        print(f"[R5 주입] {fq} 비움(테이블 유지, 행만 0): {before} → {after} rows")
        print("이제 BI/StarRocks 는 빈값으로 보임 = 'DAG 성공인데 BI 공백' 재현.")

    elif args.action == "rollback":
        if args.snapshot_id is None:
            raise SystemExit("--snapshot-id 필요 (먼저 --action snapshots 로 확인)")
        before = spark.table(fq).count()
        spark.sql(
            f"CALL iceberg.system.rollback_to_snapshot('{args.table}', {args.snapshot_id})"
        )
        after = spark.table(fq).count()
        print(f"[R5b 롤백] {fq} → 스냅샷 {args.snapshot_id}: {before} → {after} rows")
        print("주의: 해당 스냅샷 이후 커밋은 버려짐. 권위 복구가 필요하면 재집계(R5).")

    spark.stop()


if __name__ == "__main__":
    main()
