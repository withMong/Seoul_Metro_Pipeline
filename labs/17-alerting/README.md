# 17 · 실시간 알람 (운영 안정성)

적재(Bronze)와 **별개로** 같은 Kafka 스트림을 한 번 더 읽어 **두 가지 결함을 실시간 감지**하고
**Slack** 으로 경고한다. 배치 품질검증(GE/WAP)이 *서빙 직전*을 지킨다면, 이 알람은 *수집 순간*을 지킨다.

**두 결함은 종류가 다르다 — 한 잡으로 못 잡는다:**

| 구분 | 무엇 | 어떻게 | 잡는 것 |
|---|---|---|---|
| **validity** (들어온 행이 잘못) | 핵심 필드 null | Flink 1분 윈도우 null 집계 | `01-null-alarm-stream.sql` + `slack_alerter.py` |
| **completeness** (행 자체가 안 옴) | 노선·방향 **무수신** | (line,dir)별 '마지막 수신 후 경과시간' | `heartbeat_watchdog.py` |

> **왜 둘로 나누나:** 2호선이 통째로 끊겨 행이 **0개**가 되는 무수신은 윈도우 집계로 못 잡는다 —
> *없는 행은 집계 대상이 아니므로*. 그래서 completeness 는 행을 세지 말고 **마지막 수신 시각**을 추적한다.
> 이때 dead-end 였던 **`position_current`(upsert)** 가 노선별 '마지막 상태/수신시각'을 durable 하게
> 들고 있어 watchdog 의 **정답 소스(재시작 시드)** 가 된다.

**freshness 분리:** 라이브 건강(지금 끊겼나?) = watchdog(③ 실시간 레인), 회고 freshness(어제 어디서
끊겼나?) = 배치 gold(`service_freshness`). freshness 는 '지금' 신호라 배치 gold 만으로는 늦다.

```
subway-events ─▶ Flink 1분 윈도우 null 집계 ─▶ subway-dq-alerts ─▶ slack_alerter.py ─▶ Slack
            └──▶ heartbeat_watchdog.py (노선·방향 무수신 감지) ───────────────────────▶ Slack
```

## 구성

| 파일 | 역할 |
|---|---|
| `01-null-alarm-stream.sql` | Flink 스트리밍 잡. Kafka→1분 윈도우 노선별 null 집계→알람 토픽 (**validity**) |
| `slack_alerter.py` | 알람 토픽 구독, 규칙 평가·쿨다운, Slack 웹훅 + 콘솔/로그 |
| `heartbeat_watchdog.py` | (line,dir)별 마지막 수신 후 경과시간 감시 → 무수신 경고 (**completeness**) |
| `alerts.log` | (자동 생성) 발송된 경고 로그 |

## heartbeat_watchdog 동작

- **시작 시 `position_current` 시드** — StarRocks Paimon 카탈로그로 노선·방향별 '마지막 수신시각'을
  읽어 초기 상태로 (→ **current 가 실제 입력**, dead-end 해소). 실패하면 스트림만으로 graceful.
  - 선행(최초 1회): `docker exec -i subway-starrocks-fe mysql -uroot -h starrocks-fe -P9030 < labs/10-delay-olap/01-create-paimon-catalog.sql`
- 이후 **subway-events 스트림**을 구독해 (line, updn_line)별 **마지막 수신 wall-clock** 라이브 추적.
- **전체 스트림이 살아있는데**(수집 윈도우 가동 중) 특정 노선·방향만 `NORECV_GAP_SEC`(기본 180초)
  이상 조용하면 → **무수신 경고**.
- **전체가 조용하면**(`STREAM_QUIET_SEC` 초과) 윈도우 밖/전체 정전으로 보고 노선별 경고 보류(오탐 방지).
- 기대 노선·방향은 `SUBWAY_LINES` × {상행,하행} 로 시드 → 처음부터 통째로 죽은 노선도 감지.

실행:
```bash
docker compose --profile alerting up -d --build heartbeat-watchdog
docker logs -f subway-heartbeat-watchdog
```

## 감지 규칙

1. **CRITICAL** — `statn_id`/`statn_nm`/`recptn_dt` 가 윈도우 안에서 1건이라도 null.
   이들은 **절대 null 이면 안 되는** 필드라, null = 스키마·파싱 이상 신호.
2. **WARN · train_no** — `train_no` null 비율이 임계(기본 50%) 초과.
   단 **1호선은 코레일 구간에서 trainNo 가 사라지는 게 정상**이라 제외(오탐 방지).
3. **WARN · low_volume** — 윈도우 수신 건수가 기준(기본 5) 미만 → 수집 지연/장애 의심.

`(노선, 규칙)`별 쿨다운(기본 5분)으로 같은 경고 폭주를 막는다.

## 왜 "Flink 스트리밍"에서 감지하나 (설계 근거)

- **백필 불가 데이터라 사후 검증이 무의미** — 실시간 위치 API는 과거를 다시 못 받는다.
  null·유실은 *그 순간* 잡아야 의미가 있다. 배치로 다음 날 발견하면 이미 데이터는 없다.
- **데이터가 흐르는 경로에서 바로 감지** — 이미 Kafka→Flink 파이프라인이 있으니,
  같은 스트림을 **독립 소비자그룹**으로 한 번 더 읽으면 추가 인프라 없이 실시간 감시가 된다.
- **윈도우 집계 = Flink 의 본령** — "1분당 노선별 null 건수/비율" 같은 시간 윈도우 집계는
  스트리밍 엔진이 가장 잘하는 일(텀블링 윈도우). 외부에서 폴링·계산할 필요가 없다.
- **적재와 분리(관심사 분리)** — 별도 잡 + `latest-offset` + 전용 group.id 라
  Bronze 적재 잡의 offset·안정성에 영향을 주지 않고, 모니터만 켜고/끌 수 있다.
- **배치 GE 와 역할 분담** — GE/WAP(배치)는 *집계 마트가 서빙되기 직전*의 정합성을 막고,
  이 알람(스트리밍)은 *원천이 들어오는 순간*의 결함을 즉시 알린다. 두 게이트가 보완 관계.
- **vs StarRocks 주기 모니터** — 1분마다 쿼리해 보는 방식은 결국 폴링이라 지연·부하가 있고,
  "윈도우 안에서 몇 건이 null 이었나"를 정확히 못 본다. 스트리밍이 더 정확하고 즉시성이 높다.

> 대표 사례: **1호선 trainNo 가 코레일 구간에서 null** 로 사라지는 것 —
> 이건 *정상* 이므로 규칙에서 제외하고, 같은 null 이라도 *2·9호선에서 급증*하면 경고한다.
> "기대되는 null vs 비정상 null"을 구분하는 게 이 알람의 핵심.

## 실행

```bash
# (선행) kafka·flink·subway-producer 가 떠 있고 수집 중이어야 함
bash scripts/run-null-alarm.sh
```

수동으로 나눠서:

```bash
# ① Flink 잡 제출
docker exec -it subway-flink-jobmanager \
  /opt/flink/bin/sql-client.sh -f /workspace/labs/17-alerting/01-null-alarm-stream.sql

# ② 알림 서비스
docker compose --profile alerting up -d --build dq-alerter
docker logs -f subway-dq-alerter
```

## Slack 연결

1. Slack → Apps → **Incoming Webhooks** → Add to Slack → 채널 선택 → Webhook URL 복사
2. `.env` 의 `SLACK_WEBHOOK_URL=` 에 붙여넣기 (이 값은 `.env` 라 git 에 안 올라감)
3. `dq-alerter` 재기동. 미설정 시에도 **콘솔·`alerts.log`** 로는 동작.

## 테스트 (의도적 null 주입)

`subway-dq-alerts` 토픽에 가짜 위반 레코드를 직접 넣어 알람을 확인할 수 있다:

```bash
docker exec -i subway-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:19092 --topic subway-dq-alerts <<'EOF'
{"window_start":"2026-06-25T09:00:00","window_end":"2026-06-25T09:01:00","line":"2호선","total":40,"null_event_id":0,"null_statn_id":7,"null_statn_nm":0,"null_recptn_dt":0,"null_train_no":1}
EOF
```

→ `🔴 CRITICAL 2호선 statn_id — … statn_id null 7건 …` 가 콘솔/Slack 에 떠야 정상.
