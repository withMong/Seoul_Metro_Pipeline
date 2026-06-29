# `scripts/ops` — 운영 점검 & 리스크 드릴

commerce(Olist) 프로젝트의 R1~R8 운영 툴킷을 **이 프로젝트(1·2·9호선 headway)에 맞게
선별·이식**했다. 컨테이너 `subway-*`, 토픽 `subway-events`/`subway-dq-alerts`, 서빙 카탈로그
StarRocks `iceberg_catalog`, Spark 카탈로그 `iceberg` 기준.

> 모든 스크립트는 프로젝트 루트에서 **`bash scripts/ops/<이름>.sh`** 로 실행.
> 필요한 컨테이너는 대부분 스크립트가 자동 기동한다.

---

## 1. 운영 점검 (모니터링)

| 스크립트 | 무엇을 보나 |
|---|---|
| **`baseline-evidence.sh`** | **한 방 점검** — 서비스 → Flink 잡 → Kafka offset/lag → 서빙 행수 |
| `check-env.sh` | Docker · 데몬 · Compose · 메모리 |
| `flink-jobs.sh` | Flink 실행 중 잡 (`subway-bronze-fanout`, `subway-dq-null-alarm`) |
| `kafka-offsets.sh` | `subway-events`/`subway-dq-alerts` end offset + bronze 그룹 lag |
| `consume-kafka.sh [N]` | `subway-events` 메시지 N건 미리보기 (기본 5) |
| `query-bronze.sh` | Paimon Bronze(log/current) 행수 |
| `query-paimon-files.sh` | Paimon 스냅샷·파일 수·compaction 작동(`commit_kind`) |
| `query-gold.sh` | 서빙(StarRocks→Iceberg) gold 행수 + CN 생존 |
| `airflow-dags.sh` | DAG 목록 + `subway_headway_wap_pipeline` 최근 실행 |

## 2. 리스크 드릴 (주입 → 복구)

| 드릴 | 리스크 | 주입 → 복구 |
|---|---|---|
| **R1** `r1-flink-taskmanager.sh stop\|start` | Flink 컴퓨트 장애 | TaskManager 정지 → 기동(체크포인트 자동 복구) |
| **R4** `r4-inject-null-event.sh` | 소스 결함·null 유입 | null 이벤트 주입 → 실시간 알람 🔴 CRITICAL |
| **R5** `r5-empty-gold-mart.sh` | **배치 성공인데 BI 공백 ★** | gold 마트 비움 → 재집계 또는 롤백 |
| **R5** `r5-find-recovery-point.sh` | (복구 지점 찾기) | 최근 Iceberg 스냅샷 목록 |
| **R5b** `r5b-rollback-gold-mart.sh <id>` | (즉시 복구) | 스냅샷 포인터 롤백 |
| **R6** `r6-refresh-starrocks.sh` | 서빙 메타데이터 stale | StarRocks `REFRESH EXTERNAL` |
| **R7** `r7-paimon-compact.sh` | small files(read amplification) | Paimon log compaction (파일 수↓) |

## 3. 헬퍼

| 스크립트 | 용도 |
|---|---|
| `wap.sh <stage>` | WAP 단계 실행 (`write`/`audit`/`publish`/`validate`/`all`) — GE 검증 포함 |
| `_spark-ops.sh` | (내부) `ops_iceberg.py` spark-submit 래퍼 — R5/R5b 가 사용 |

---

## 대표 시나리오 — "DAG는 초록인데 BI가 비었다" (R5)

```bash
bash scripts/ops/r5-empty-gold-mart.sh          # 주입: gold 마트 비움
bash scripts/ops/query-gold.sh                  # 증상 확인(행수 0)
bash scripts/ops/r5-find-recovery-point.sh      # 복구 지점(snapshot_id) 찾기
bash scripts/ops/r5b-rollback-gold-mart.sh <id> # 롤백 (또는 run-spark-headway.sh 재집계)
bash scripts/ops/r6-refresh-starrocks.sh        # 서빙 반영
bash scripts/ops/query-gold.sh                  # 복구 확인(행수 복귀)
```

> ⚠️ **슬롯 주의**: Flink 슬롯이 2개라 `bronze` + `null-alarm` 이 동시에 돌면 꽉 찬다.
> 배치 점검(`query-bronze`·`query-paimon-files`)이 멈추면 알람 잡을 `flink cancel` 해 슬롯을 비운다.

---

## 가져오지 **않은** 것 (commerce 전용 · 중복 · 해당없음)

| 원본 | 사유 |
|---|---|
| `ops-r3-*kafka-isr*` | **해당 없음** — 단일 브로커 RF=1, `min.insync=2` 토픽 없음 (멀티브로커면 추가 가능) |
| `ops-r2-*savepoint*` · `ops-r8-spark-oom*` | **로드맵** — stateful 복구 / 드라이버 OOM 데모는 차후 |
| `produce-olist-*` · `live-*` · `start/stop-live-*` | commerce 전용 → `subway_producer.py` 로 대체 |
| `run-flink-olist-*` · `run-olist-bi-*` · `*-inner.sh` | commerce 실행기 → `run-spark-*.sh`, `labs/04` 로 대체 |
| `reset-olist-*` · `query-olist-*` · `query-realtime-olap*` | commerce 네임스페이스 전용 |
| `smoke-test.sh` | 이미 `scripts/smoke-test.sh` 보유 |
| `check-kafka-lag.sh` · `kafka-offset-summary.sh` | `kafka-offsets.sh` 로 통합 |

> 요약: **모니터링 9개 + 리스크 드릴 7개(R1·R4·R5·R5b·R6·R7) + 헬퍼 2개**.
> ISR(R3)은 인프라상 해당 없음, savepoint(R2)·OOM(R8)은 로드맵으로 README에 사유 명시.
