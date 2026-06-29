#!/usr/bin/env bash
# WAP 단계 실행 (write|audit|publish|validate|all). 슬라이드 ⑥ GE/WAP 증거용 한 줄 래퍼.
#   예) bash scripts/ops/wap.sh audit
set -euo pipefail
cd "$(dirname "$0")/../.."
STAGE="${1:-audit}"

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
  /workspace/labs/13-spark-headway/headway_wap.py --stage "$STAGE"
