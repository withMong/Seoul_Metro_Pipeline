#!/usr/bin/env bash
# R5 — 복구 지점 찾기: gold headway 마트의 최근 Iceberg 스냅샷 목록.
set -euo pipefail
cd "$(dirname "$0")/../.."
bash scripts/ops/_spark-ops.sh --action snapshots --table gold.subway_headway_by_station_tod
