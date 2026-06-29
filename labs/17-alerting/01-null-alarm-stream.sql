-- =====================================================================
-- 실시간 데이터 품질 알람 — null 감지 (Flink 스트리밍)
-- =====================================================================
-- 목적: 적재(Bronze)와 별개로, 같은 Kafka 스트림(subway-events)을 독립
--   소비자그룹으로 한 번 더 읽어 1분 텀블링 윈도우로 노선별 null 건수를
--   집계하고 subway-dq-alerts 토픽에 내보낸다. 파이썬 알림 서비스
--   (slack_alerter.py)가 이를 구독해 규칙 위반 시 Slack 으로 경고한다.
--
-- 왜 별도 잡인가:
--   - Bronze 적재 잡과 분리 → DQ 모니터를 독립적으로 켜고/끌 수 있고,
--     적재 잡의 offset/안정성에 영향을 주지 않는다.
--   - latest-offset + 전용 group.id → 과거가 아닌 "지금 흐르는" 데이터만 감시.
--   - taskmanager 슬롯 2개 중 Bronze(1) + 알람(1) 으로 동시 실행 가능.
--
-- 실행:
--   docker exec -it subway-flink-jobmanager \
--     /opt/flink/bin/sql-client.sh -f /workspace/labs/17-alerting/01-null-alarm-stream.sql
-- =====================================================================
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'tableau';
SET 'execution.checkpointing.interval' = '30s';
SET 'pipeline.name' = 'subway-dq-null-alarm';

-- ── 소스: Bronze 와 다른 group.id, 최신 오프셋부터(지금 흐르는 것만 감시) ──
CREATE TEMPORARY TABLE subway_events_raw (
  raw_json STRING,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'subway-events',
  'properties.bootstrap.servers' = 'kafka:19092',
  'properties.group.id' = 'flink-dq-null-alarm',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'raw'
);

-- ── 싱크: 1분 윈도우 노선별 집계 결과 → 알람 토픽(json) ──
CREATE TEMPORARY TABLE subway_dq_alerts (
  window_start   TIMESTAMP(3),
  window_end     TIMESTAMP(3),
  line           STRING,
  total          BIGINT,
  null_event_id  BIGINT,
  null_statn_id  BIGINT,
  null_statn_nm  BIGINT,
  null_recptn_dt BIGINT,
  null_train_no  BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'subway-dq-alerts',
  'properties.bootstrap.servers' = 'kafka:19092',
  'format' = 'json',
  'json.timestamp-format.standard' = 'ISO-8601'
);

-- ── 파싱 뷰: 핵심 필드만 추출(시간속성 proc_time 보존) ──
CREATE TEMPORARY VIEW parsed AS
SELECT
  COALESCE(JSON_VALUE(raw_json, '$.line'), '(null)') AS line,
  JSON_VALUE(raw_json, '$.event_id')  AS event_id,
  JSON_VALUE(raw_json, '$.statn_id')  AS statn_id,
  JSON_VALUE(raw_json, '$.statn_nm')  AS statn_nm,
  JSON_VALUE(raw_json, '$.recptn_dt') AS recptn_dt,
  JSON_VALUE(raw_json, '$.train_no')  AS train_no,
  proc_time
FROM subway_events_raw;

-- ── 1분 텀블링 윈도우 × 노선 null 집계 ──
--   '' (빈 문자열)도 null 로 간주. event_id 가 '--'로 시작하면
--   (train_no·statn_id 둘 다 빈 값으로) 구성 키가 깨진 것이라 위반으로 본다.
INSERT INTO subway_dq_alerts
SELECT
  window_start,
  window_end,
  line,
  COUNT(*) AS total,
  SUM(CASE WHEN event_id  IS NULL OR event_id  = '' OR event_id LIKE '--%' THEN 1 ELSE 0 END) AS null_event_id,
  SUM(CASE WHEN statn_id  IS NULL OR statn_id  = '' THEN 1 ELSE 0 END) AS null_statn_id,
  SUM(CASE WHEN statn_nm  IS NULL OR statn_nm  = '' THEN 1 ELSE 0 END) AS null_statn_nm,
  SUM(CASE WHEN recptn_dt IS NULL OR recptn_dt = '' THEN 1 ELSE 0 END) AS null_recptn_dt,
  SUM(CASE WHEN train_no  IS NULL OR train_no  = '' THEN 1 ELSE 0 END) AS null_train_no
FROM TABLE(TUMBLE(TABLE parsed, DESCRIPTOR(proc_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, line;
