-- =====================================================================
-- StarRocks → Paimon 외부 카탈로그 (Bronze 조회용)
-- =====================================================================
-- watchdog(무수신 감지)가 시작 시 position_current 의 노선·방향별 '마지막 수신시각'을
-- 조회해 시드하기 위함. (current 가 dead-end 가 아니라 실제 입력으로 쓰임)
-- 실행:
--   docker exec -i subway-starrocks-fe mysql -uroot -h starrocks-fe -P9030 \
--     < labs/10-delay-olap/01-create-paimon-catalog.sql
-- =====================================================================

DROP CATALOG IF EXISTS paimon_catalog;

CREATE EXTERNAL CATALOG paimon_catalog
PROPERTIES (
  "type" = "paimon",
  "paimon.catalog.type" = "filesystem",
  "paimon.catalog.warehouse" = "s3://paimon/warehouse",
  "aws.s3.endpoint" = "http://minio:9000",
  "aws.s3.access_key" = "minioadmin",
  "aws.s3.secret_key" = "minioadmin",
  "aws.s3.enable_path_style_access" = "true",
  "aws.s3.enable_ssl" = "false"
);

-- 확인: 노선·방향별 마지막 수신시각 (watchdog 시드 쿼리와 동일)
SELECT line, updn_line, MAX(recptn_dt) AS last_recv
FROM paimon_catalog.bronze.subway_position_current
GROUP BY line, updn_line
ORDER BY line, updn_line;
