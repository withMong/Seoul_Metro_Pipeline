#!/usr/bin/env bash
# R1 — Flink 컴퓨트(TaskManager) 장애/복구 드릴.
#   stop  : TaskManager 정지 → 잡이 슬롯을 잃고 FAILING/RESTARTING
#   start : TaskManager 기동 → 체크포인트에서 자동 복구되는지 확인
set -euo pipefail
cd "$(dirname "$0")/../.."

case "${1:-}" in
  stop)
    docker compose stop flink-taskmanager
    echo "TaskManager 정지. Flink UI(http://localhost:8081)에서 잡 상태 확인."
    echo "복구: bash scripts/ops/r1-flink-taskmanager.sh start"
    ;;
  start)
    docker compose up -d flink-taskmanager
    echo "TaskManager 기동. 잡이 마지막 체크포인트(30s 간격)에서 자동 복구되는지 8081 확인."
    ;;
  *)
    echo "사용: $0 stop|start" >&2
    exit 2
    ;;
esac
