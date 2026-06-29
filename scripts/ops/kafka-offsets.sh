#!/usr/bin/env bash
# Kafka 적재량(end offset) + Bronze 소비자그룹 lag.
set -euo pipefail
BOOT=kafka:19092

for t in subway-events subway-dq-alerts; do
  echo "== end offsets: $t =="
  docker exec subway-kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server "$BOOT" --topic "$t" 2>/dev/null || echo "  (토픽 없음/미생성)"
  echo
done

echo "== consumer lag: flink-paimon-subway-bronze =="
docker exec subway-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server "$BOOT" --describe --group flink-paimon-subway-bronze 2>/dev/null \
  || echo "  (커밋된 offset 없음 — bronze 잡 미실행이거나 아직 커밋 전)"
