#!/usr/bin/env bash
# R4 — 불량(null) 이벤트 주입 → 실시간 null 알람(labs/17)이 잡는지 검증.
#   statn_id / statn_nm 가 null 인 가짜 이벤트를 subway-events 에 1건 넣는다.
#   → Flink DQ 잡이 1분 윈도우에서 null_statn_id>0 로 집계 → dq-alerter 가 🔴 CRITICAL.
set -euo pipefail

BAD='{"event_id":"DQ-BAD-001","line":"2호선","subway_id":"1002","train_no":"9999","statn_id":null,"statn_nm":null,"statn_tnm":null,"updn_line":"0","train_sttus":"1","direct_at":"0","lstcar_at":"0","recptn_dt":"2026-06-25 09:00:00","ingested_at":"2026-06-25T00:00:00Z"}'

printf '%s\n' "$BAD" | docker exec -i subway-kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:19092 --topic subway-events

echo "불량 이벤트 1건 주입 완료(statn_id/statn_nm = null)."
echo "확인(최대 1분 윈도우 뒤):"
echo "  docker logs -f subway-dq-alerter    # 🔴 CRITICAL · 2호선 · statn_id 가 떠야 정상"
echo "참고: 이 레코드는 event_id 가 있어 Bronze 에 1행 남을 수 있음(불량 데이터 유입 재현). 알람 검증용."
