#!/usr/bin/env bash
# Paimon 운영 검증: 스냅샷 이력 · 파일 수(read amplification) · compaction 작동.
#   - commit_kind 에 COMPACT 가 보이면 = compaction 이 실제로 일어남(자동)
#   - 파일 수 vs 레코드 수로 small files 여부 판단
set -euo pipefail
docker exec subway-flink-jobmanager \
  /opt/flink/bin/sql-client.sh -f /workspace/labs/04-flink-paimon/03-query-paimon-ops.sql
