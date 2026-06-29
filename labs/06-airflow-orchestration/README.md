# Airflow — headway 배치 오케스트레이션

수집(스케줄러)과 분리된 **배치 집계**를 매일 묶는다. 마트 구축 전략을 **두 DAG로 분리**해
비교할 수 있게 했다. 두 DAG 모두 앞단에서 **런타임·적재(streaming) 검증**을 먼저 하고,
`start → … → finish` 로 감싼다.

## DAG ①: `subway_headway_wap_pipeline` (WAP, 매일 02:00)

검증(audit) 통과 전엔 serving(gold)에 절대 반영하지 않는다 — 나쁜 데이터가 대시보드에 새지 않음.

```
start ─► validate_runtime ─► validate_bronze ─► l1_arrival_events
  ─► wap_write ─► wap_audit ─► wap_publish ─► wap_validate ─► finish
       (staging)   (GE검증)    (staging→gold)   (사후확인)
                      │ 실패 → 멈춤 (gold 유지)
```

| task | 내용 |
|---|---|
| `validate_runtime_services` | kafka·flink·minio·iceberg·starrocks 헬스 |
| `validate_bronze_running` | bronze fanout 잡이 RUNNING 인지(Flink REST) |
| `l1_arrival_events` | Paimon log → `silver.subway_arrival_events` (Flink 도착 추출) |
| `wap_write` | 마트를 `iceberg.staging.*` 에 적재 (서빙 아님) |
| `wap_audit` | staging DQ(GE) 검사. **실패하면 exit 1 → 이후 중단** |
| `wap_publish` | 통과 시 `staging → gold` 원자적 교체(CREATE OR REPLACE) |
| `wap_validate` | gold 가 staging 과 일치하는지 사후 확인 |

## DAG ②: `subway_headway_pipeline` (직접 빌드, 수동 트리거)

마트를 gold 에 **바로 빌드한 뒤 검증**(build-then-validate). WAP 와 비교용.

```
start ─► validate_runtime ─► validate_bronze ─► l1_arrival_events
  ─► build_headway_mart ─► build_freshness_mart ─► validate_gold ─► finish
```

- **직접 빌드**: 빠르고 단순, 검증은 빌드 *후*.
- **WAP**: staging 검증 통과 후에만 publish (나쁜 데이터 차단). → 운영에선 WAP 권장.

## 공통

- 실행: 스케줄러가 `docker exec` 로 기존 spark-client/flink 컨테이너에서 실행 (Airflow 이미지에 docker CLI 포함)
- StarRocks/Streamlit 은 `gold` 만 조회 → **검증된 데이터만 서빙**

## 실행

```bash
# lakehouse 가 떠 있는 상태에서 Airflow 기동
docker compose --profile orchestration up -d --build

# http://localhost:8080 (admin/admin)
#  → subway_headway_wap_pipeline 트리거 → Graph 로 단계별 확인
# CLI: docker exec subway-airflow-webserver airflow dags trigger subway_headway_wap_pipeline
```

## WAP 검증 시연 포인트 (포트폴리오)

`headway_wap.py` 의 audit 임계값을 일부러 빡세게 바꿔 **audit 실패 → publish 미실행 → gold 불변**을
보여주면, "검증이 서빙을 막는다"는 WAP의 가치가 한눈에 드러난다.
