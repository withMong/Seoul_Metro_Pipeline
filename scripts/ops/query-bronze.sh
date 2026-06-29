#!/usr/bin/env bash
# Paimon Bronze 점검: log/current 행수 등 (labs/04 02-query-bronze.sql 재사용).
set -euo pipefail
docker exec subway-flink-jobmanager \
  /opt/flink/bin/sql-client.sh -f /workspace/labs/04-flink-paimon/02-query-subway-bronze.sql
