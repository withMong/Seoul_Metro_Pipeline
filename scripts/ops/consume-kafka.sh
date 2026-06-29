#!/usr/bin/env bash
# subway-events 토픽에서 메시지 N건 미리보기(기본 5건).
set -euo pipefail
N="${1:-5}"
docker exec subway-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:19092 --topic subway-events \
  --max-messages "$N" --timeout-ms 10000 \
  --property print.key=true --property key.separator=" | "
