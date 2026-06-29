#!/usr/bin/env bash
# R6 — StarRocks 외부 카탈로그 메타데이터 새로고침.
#   gold 마트는 갱신됐는데 BI 에 안 보일 때(외부 카탈로그가 새 Iceberg 스냅샷을
#   아직 못 읽은 경우)의 처방. R5/R5b 복구 뒤 서빙 반영용.
set -euo pipefail

docker exec subway-starrocks-fe mysql -h127.0.0.1 -P9030 -uroot -e "
REFRESH EXTERNAL TABLE iceberg_catalog.gold.subway_headway_by_station_tod;
REFRESH EXTERNAL TABLE iceberg_catalog.gold.subway_service_freshness;
" 2>/dev/null \
  || docker exec subway-starrocks-fe mysql -h127.0.0.1 -P9030 -uroot -e "
REFRESH EXTERNAL CATALOG iceberg_catalog;
"

echo "StarRocks 외부 메타데이터 새로고침 요청 완료 → BI(Streamlit) 다시 확인."
