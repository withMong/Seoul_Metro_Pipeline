#!/usr/bin/env bash
# 공통: ops_iceberg.py 를 spark-submit (run-spark-headway.sh 와 동일 PKGS/conf 재사용)
set -euo pipefail
cd "$(dirname "$0")/../.."

PKGS="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,\
org.apache.iceberg:iceberg-aws-bundle:1.6.1,\
org.apache.paimon:paimon-spark-3.5_2.12:1.4.1,\
org.apache.paimon:paimon-s3:1.4.1"
NETTY="-Dorg.apache.iceberg.shaded.io.netty.noUnsafe=true -Dio.netty.noUnsafe=true"

docker compose up -d minio iceberg-postgres iceberg-rest spark-client >/dev/null

docker compose exec -T spark-client /opt/spark/bin/spark-submit \
  --packages "$PKGS" \
  --conf spark.driver.memory=2g \
  --conf spark.sql.iceberg.vectorization.enabled=false \
  --conf "spark.driver.extraJavaOptions=$NETTY" \
  /workspace/labs/18-ops-recovery/ops_iceberg.py "$@"
