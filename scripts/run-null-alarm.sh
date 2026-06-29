#!/usr/bin/env bash
# 실시간 null 알람 기동:
#   ① Flink DQ 스트리밍 잡 제출(Kafka → 1분 윈도우 null 집계 → subway-dq-alerts)
#   ② Slack 알림 서비스(dq-alerter) 기동
#
# 선행: kafka, flink-jobmanager/taskmanager, (수집 중인) subway-producer 가 떠 있어야 함.
set -euo pipefail

echo "▶ Flink DQ null 감지 잡 제출…"
docker exec -i subway-flink-jobmanager \
  /opt/flink/bin/sql-client.sh -f /workspace/labs/17-alerting/01-null-alarm-stream.sql

echo "▶ Slack 알림 서비스 기동(profile alerting)…"
docker compose --profile alerting up -d --build dq-alerter

echo "✅ 완료. 확인:"
echo "   - Flink UI(http://localhost:8081)에서 'subway-dq-null-alarm' RUNNING"
echo "   - docker logs -f subway-dq-alerter   (콘솔 경고)"
echo "   - SLACK_WEBHOOK_URL 설정 시 Slack 채널로 알림"
