-- =====================================================================
-- Paimon 수동 compaction — log(append) small files 병합 (R7 / Paimon판)
-- =====================================================================
-- 주의: 스트리밍 bronze 잡이 떠 있으면 그 잡이 compaction 을 맡는다.
--   수동 compaction 은 가급적 bronze 잡을 멈춘 뒤 실행(동시 쓰기 충돌 방지).
-- 문법은 Paimon 버전에 따라 다를 수 있음(1.4 기준 named-arg `table`).
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

-- compaction 전 파일 수
SELECT COUNT(*) AS files_before FROM bronze.`subway_position_log$files`;

-- 수동 compaction 실행 (small files → 더 큰 파일로 병합)
CALL paimon_lake.sys.compact(`table` => 'bronze.subway_position_log');

-- compaction 후 파일 수 (줄어들면 성공, 레코드 수는 불변)
SELECT COUNT(*) AS files_after FROM bronze.`subway_position_log$files`;
