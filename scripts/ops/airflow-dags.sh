#!/usr/bin/env bash
# Airflow DAG 목록 + subway_headway_wap_pipeline 최근 실행.
set -euo pipefail
docker exec subway-airflow-webserver airflow dags list
echo
echo "== subway_headway_wap_pipeline 최근 실행 =="
docker exec subway-airflow-webserver \
  airflow dags list-runs -d subway_headway_wap_pipeline 2>/dev/null | head -10 \
  || echo "  (실행 이력 없음 / orchestration 프로필 미기동)"
