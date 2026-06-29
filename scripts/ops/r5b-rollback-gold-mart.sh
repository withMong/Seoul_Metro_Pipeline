#!/usr/bin/env bash
# R5b — gold headway 마트를 특정 스냅샷으로 롤백(메타데이터 포인터 이동, 재집계 없이 즉시).
#   snapshot_id 는 r5-find-recovery-point.sh 로 확인.
set -euo pipefail
cd "$(dirname "$0")/../.."

SID="${1:-}"
if [[ -z "$SID" ]]; then
  echo "사용: $0 <snapshot_id>" >&2
  echo "  먼저: bash scripts/ops/r5-find-recovery-point.sh" >&2
  exit 2
fi

bash scripts/ops/_spark-ops.sh --action rollback \
  --table gold.subway_headway_by_station_tod --snapshot-id "$SID"

echo
echo "→ 서빙 반영: bash scripts/ops/r6-refresh-starrocks.sh"
