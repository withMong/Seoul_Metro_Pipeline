#!/usr/bin/env bash
# 운영 점검 한 방 — "지금 건강한가"를 증거로 모아 출력.
#   서비스 상태 → Flink 잡 → Kafka offset/lag → 서빙(gold) 행수.
set -euo pipefail
cd "$(dirname "$0")/../.."
HERE="scripts/ops"

echo "########################################"
echo "# 운영 점검 (baseline evidence)  $(date '+%F %T')"
echo "########################################"

echo; echo "== [1] Docker 서비스 =="
docker compose ps 2>/dev/null || docker ps --format 'table {{.Names}}\t{{.Status}}' | grep subway- || true

echo; echo "== [2] Flink 실행 중 잡 =="
bash "$HERE/flink-jobs.sh" || echo "  Flink 조회 실패"

echo; echo "== [3] Kafka offset / lag =="
bash "$HERE/kafka-offsets.sh" || echo "  Kafka 조회 실패"

echo; echo "== [4] 서빙(gold) 행수 =="
bash "$HERE/query-gold.sh" || echo "  서빙 조회 실패 → R6 refresh / CN 확인"

echo; echo "== 끝 =="
echo "더 보기: bash $HERE/query-bronze.sh (Paimon)  ·  bash $HERE/airflow-dags.sh (DAG)"
