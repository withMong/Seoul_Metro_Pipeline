#!/usr/bin/env bash
# Flink 실행 중 잡 목록 (bronze fanout / dq-null-alarm 가 RUNNING 인지).
set -euo pipefail
docker exec subway-flink-jobmanager /opt/flink/bin/flink list -r
