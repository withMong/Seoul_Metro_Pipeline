#!/usr/bin/env bash
# R5 — 'DAG 는 성공(초록)인데 BI 엔 데이터가 없음' 재현.
#   gold headway 마트의 행만 비운다(테이블/스키마는 유지). 파이프라인은 멀쩡한데
#   서빙 마트만 비는, 가장 헷갈리는 운영 장애 클래스.
set -euo pipefail
cd "$(dirname "$0")/../.."

bash scripts/ops/_spark-ops.sh --action empty --table gold.subway_headway_by_station_tod

echo
echo "→ 이제 Streamlit/StarRocks 는 빈값. (StarRocks 캐시 때문에 잠시 옛 값이 보일 수 있음)"
echo "복구 두 갈래:"
echo "  (a) 재집계(권위·느림):  bash scripts/run-spark-headway.sh"
echo "  (b) 스냅샷 롤백(즉시):  bash scripts/ops/r5-find-recovery-point.sh"
echo "                          bash scripts/ops/r5b-rollback-gold-mart.sh <snapshot_id>"
echo "  복구 후 서빙 반영:       bash scripts/ops/r6-refresh-starrocks.sh"
