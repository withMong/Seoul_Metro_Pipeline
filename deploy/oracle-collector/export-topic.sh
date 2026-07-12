#!/usr/bin/env bash
# =====================================================================
# [VM 에서 실행] subway-events 토픽에 보관된(최대 7일) 전체를 JSONL 로 덤프.
# 로컬로 내려받아 재적재하기 위함. bronze 는 event_id 로 멱등이라 재적재해도 안전.
#   bash export-topic.sh          → events-YYYYMMDD-HHMM.jsonl 생성
# =====================================================================
set -euo pipefail
OUT="events-$(date +%Y%m%d-%H%M).jsonl"

# --timeout-ms: 새 메시지가 없으면 15초 뒤 종료(=보관분 전부 읽고 멈춤)
docker exec subway-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic subway-events \
  --from-beginning \
  --timeout-ms 15000 > "$OUT" 2>/dev/null || true

echo "saved $OUT  ($(wc -l < "$OUT") lines)"
echo "로컬에서:  scp opc@<VM_PUBLIC_IP>:$(pwd)/$OUT ."
