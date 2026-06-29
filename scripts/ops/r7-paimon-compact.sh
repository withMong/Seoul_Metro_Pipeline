#!/usr/bin/env bash
# R7(Paimon판) — log(append) small files 수동 compaction + 전/후 파일 수 비교.
#   ⚠️ 스트리밍 bronze 잡이 떠 있으면 그 잡이 compaction 을 맡으므로,
#      수동 실행은 가급적 bronze 잡을 멈춘 뒤 권장(동시 쓰기 충돌 방지).
set -euo pipefail
echo "TIP: 스트리밍 bronze 가 RUNNING 이면 자동 compaction 중일 수 있어요."
echo "     수동 compaction 충돌이 나면 bronze 잡을 잠시 cancel 후 재실행하세요."
docker exec subway-flink-jobmanager \
  /opt/flink/bin/sql-client.sh -f /workspace/labs/04-flink-paimon/04-compact-paimon-log.sql
