-- =====================================================================
-- Paimon 운영 점검 — 스냅샷 이력 · 파일 수(read amplification) · compaction
-- =====================================================================
-- Paimon 시스템 테이블(`테이블$snapshots`, `테이블$files`)로 건강 상태를 본다.
--   - commit_kind 에 COMPACT 가 보이면 = compaction 이 실제로 일어남
--   - 파일 수가 레코드 대비 과도하게 많으면 = small files(read amplification)
-- 실행: docker exec subway-flink-jobmanager /opt/flink/bin/sql-client.sh -f 이 파일
-- =====================================================================
SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE CATALOG paimon_lake WITH (
  'type' = 'paimon',
  'warehouse' = 's3://paimon/warehouse',
  's3.endpoint' = 'http://minio:9000',
  's3.access-key' = 'minioadmin',
  's3.secret-key' = 'minioadmin',
  's3.path.style.access' = 'true'
);
USE CATALOG paimon_lake;

-- ① log(append, bucket=-1) 스냅샷 이력: commit_kind = APPEND / COMPACT
SELECT snapshot_id, commit_kind, commit_time, total_record_count
FROM bronze.`subway_position_log$snapshots`
ORDER BY snapshot_id DESC LIMIT 12;

-- ② log 데이터 파일 수 + 레코드 (파일 많을수록 read amplification)
SELECT COUNT(*) AS data_files, SUM(record_count) AS records
FROM bronze.`subway_position_log$files`;

-- ③ current(PK, bucket=4) 스냅샷: PK 테이블은 쓰기 중 자동 compaction
SELECT snapshot_id, commit_kind, commit_time, total_record_count
FROM bronze.`subway_position_current$snapshots`
ORDER BY snapshot_id DESC LIMIT 12;

-- ④ current 버킷별 파일 수 (4개 버킷에 고르게 분포해야 정상)
SELECT bucket, COUNT(*) AS data_files, SUM(record_count) AS records
FROM bronze.`subway_position_current$files`
GROUP BY bucket ORDER BY bucket;
