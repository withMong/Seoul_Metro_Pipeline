#!/usr/bin/env bash
# =====================================================================
# [로컬 PC 에서 실행] VM 에서 받은 JSONL 을 로컬 Kafka subway-events 로 재생.
# 이후 평소대로 bronze 적재(labs/04) → L1 → L2 배치를 돌리면 된다.
# bronze 는 event_id 멱등이라 같은 파일을 여러 번 재생해도 결과가 부풀지 않는다.
#   bash replay-to-local.sh events-20260701-0840.jsonl
# 사전: 로컬 스택의 subway-kafka 컨테이너가 떠 있어야 함.
# =====================================================================
set -euo pipefail
FILE="${1:?usage: replay-to-local.sh <events.jsonl>}"
[ -f "$FILE" ] || { echo "파일 없음: $FILE"; exit 1; }

docker exec -i subway-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic subway-events < "$FILE"

echo "replayed $(wc -l < "$FILE") lines → 로컬 subway-events"
echo "다음: labs/04 bronze 적재 → wap.sh all 순으로 배치 실행"
