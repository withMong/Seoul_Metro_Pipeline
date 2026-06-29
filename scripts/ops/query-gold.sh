#!/usr/bin/env bash
# 서빙 점검: StarRocks(외부 카탈로그)로 gold 마트 행수 조회 + CN 살아있나.
#   'DAG 성공인데 BI 공백' 의 1차 진단 도구.
set -euo pipefail

echo "== StarRocks CN 상태 =="
docker exec subway-starrocks-fe mysql -h127.0.0.1 -P9030 -uroot \
  -e "SHOW COMPUTE NODES\G" 2>/dev/null | grep -E "Alive|BE" \
  || echo "  CN 확인 실패 → docker compose up -d starrocks-fe starrocks-cn"

echo
echo "== gold 마트 행수 (StarRocks → Iceberg) =="
docker exec subway-starrocks-fe mysql -h127.0.0.1 -P9030 -uroot -e "
SELECT 'headway' AS mart, COUNT(*) AS n_rows FROM iceberg_catalog.gold.subway_headway_by_station_tod
UNION ALL
SELECT 'freshness', COUNT(*) FROM iceberg_catalog.gold.subway_service_freshness;
" \
  || echo "  조회 실패 → CN/카탈로그/마트 확인, 또는 bash scripts/ops/r6-refresh-starrocks.sh"
