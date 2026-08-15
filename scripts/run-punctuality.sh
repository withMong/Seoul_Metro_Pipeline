#!/usr/bin/env bash
# =====================================================================
# [개선] 9호선 정시성 마트: 시간표(계획 배차) 대비 관측 배차
#   사전: gold.subway_headway_by_station_tod 가 적재돼 있어야 함 (wap.sh all)
#         data/timetable/plan_headway_9line.csv 가 있어야 함 (repo 포함)
# 사용법: bash scripts/run-punctuality.sh
# =====================================================================
set -euo pipefail
cd "$(dirname "$0")/.."

PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,\
org.apache.iceberg:iceberg-aws-bundle:1.6.1,\
org.apache.paimon:paimon-spark-3.5_2.12:1.4.1,\
org.apache.paimon:paimon-s3:1.4.1"
NETTY="-Dorg.apache.iceberg.shaded.io.netty.noUnsafe=true -Dio.netty.noUnsafe=true"

echo "== 의존 서비스 기동 =="
docker compose up -d minio iceberg-postgres iceberg-rest spark-client

echo "== spark-submit: 정시성 마트 (9호선) =="
docker compose exec -T spark-client /opt/spark/bin/spark-submit \
  --packages "$PKGS" \
  --conf spark.driver.memory=2g \
  --conf spark.sql.iceberg.vectorization.enabled=false \
  --conf "spark.driver.extraJavaOptions=$NETTY" \
  --conf "spark.executor.extraJavaOptions=$NETTY" \
  /workspace/labs/13-spark-headway/punctuality_mart.py

echo ""
echo "✅ 완료 → iceberg.gold.subway_punctuality_9line"
