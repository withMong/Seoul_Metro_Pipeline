# Oracle Cloud 상시 수집기 (Always Free VM)

PC 를 24시간 켜둘 수 없으니, **수집기(producer)+Kafka 만** 무료 클라우드 VM 에 올려 상시 가동한다.
Kafka 가 **7일간** 데이터를 durable 하게 보관하고, 무거운 분석 스택(Flink/Spark/StarRocks)은
**로컬 PC 에서 원할 때** VM 의 데이터를 회수해 배치로 돌린다.

```
[Oracle VM · 24/7]                          [내 PC · 필요할 때만]
 서울 API ─(cron 4창, 30s)→ producer          Flink→Paimon→Spark(WAP)→Iceberg→StarRocks→BI
              │                                        ▲
              ▼                                        │  ② replay-to-local.sh → 로컬 Kafka
           Kafka (7일 보관) ── ① export-topic.sh ──▶ events.jsonl (scp)
```
> 수집=상시(클라우드) / 분석=배치(로컬). Kafka 가 둘을 잇는 **재생 가능한 버퍼**.

---

## 0. 준비물
- Oracle Cloud 무료 계정 (카드 등록 필요, **과금 없음** — Always Free 등급 사용)
- 서울 열린데이터광장 `realtimePosition` 인증키
- 로컬엔 이미 이 repo 의 분석 스택이 돌아가는 상태

## 1. VM 만들기 (Always Free)
1. Oracle Cloud 콘솔 → **Compute → Instances → Create Instance**
2. Image: **Ubuntu 22.04**, Shape: **VM.Standard.A1.Flex (Ampere/ARM, Always Free)** — OCPU 1~2, RAM 6~12GB면 충분
3. **SSH 키** 등록(공개키 붙여넣기) → 생성
4. 생성 후 **Public IP** 확인 → `ssh ubuntu@<PUBLIC_IP>` (Ubuntu 이미지는 기본 사용자 `ubuntu`)
> 별도 포트 개방 불필요 — Kafka 는 외부에 열지 않는다(보안). SSH(22)만 쓴다.

## 2. VM 부트스트랩 (최초 1회)
```bash
# VM 안에서
sudo apt-get update -y && sudo apt-get install -y git
git clone https://github.com/withMong/Seoul_Metro_Pipeline.git
cd Seoul_Metro_Pipeline/deploy/oracle-collector
bash setup-vm.sh          # docker 설치 + 타임존 KST
exit                      # 도커 그룹 반영 위해 재접속
```
다시 `ssh ubuntu@<PUBLIC_IP>` → `cd Seoul_Metro_Pipeline/deploy/oracle-collector`

## 3. 키 넣고 Kafka 띄우기
```bash
cp .env.example .env
nano .env                 # SEOUL_API_KEY= 에 인증키 입력, 노선/간격 확인

docker compose -f docker-compose.cloud.yml up -d --build kafka
docker compose -f docker-compose.cloud.yml create producer   # 생성만(아직 폴링 X)
```
한 번 수동 테스트(1회 폴링):
```bash
docker start -a subway-producer &   # 30초마다 폴링 시작 — 로그 확인
sleep 70 && docker stop subway-producer
docker exec subway-kafka /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic subway-events    # offset 늘었으면 성공
```

## 4. 창(window) 자동화 — cron
```bash
crontab -e
# 편집기에 crontab.txt 내용을 붙여넣고 저장
crontab -l                # 등록 확인
```
기본 4창(KST): **출근 08:00–08:40 / 점심 12:00–12:30 / 퇴근 18:00–18:40 / 밤 23:00–23:30**
→ 3노선×30초 ≈ **하루 840콜(< 1,000 안전)**. 이제 PC 를 꺼도 VM 이 창마다 알아서 수집한다.

## 5. 데이터 회수 → 로컬 분석
필요할 때(예: 며칠에 한 번) 아래를 돌린다.
```bash
# ① VM 에서: 보관분(최대 7일) 전체 덤프
bash export-topic.sh                       # events-YYYYMMDD-HHMM.jsonl 생성

# ② 로컬 PC 에서: 파일 받기
scp ubuntu@<PUBLIC_IP>:~/Seoul_Metro_Pipeline/deploy/oracle-collector/events-*.jsonl .

# ③ 로컬 스택 켠 상태에서 재생 → bronze 적재
bash replay-to-local.sh events-YYYYMMDD-HHMM.jsonl
#   이후 평소대로: labs/04 bronze 적재 → 12 도착추출 → wap.sh all → BI
```
> bronze 는 `event_id`(train·역·상태·날짜) 로 멱등이라, 같은 파일을 여러 번 재생·재적재해도 결과가 부풀지 않는다.

## 6. 쿼터·창 조정
- 콜 예산: `노선수 × (창시간/30초)`. 1키 = 1,000/일. 넘으면 **창을 줄이거나** 2번째 키로 분산.
- 2번째 키 분산 예: `.env` 를 러시용/비첨두용 둘로 나눠 producer 를 2개(다른 SUBWAY_LINES/키)로 띄우고 cron 창을 나눈다.
- 2호선은 열차가 많아 한 번에 100행을 넘으면 페이지네이션이 필요할 수 있으니 콜 여유를 둔다.

## 7. 보안 · 운영 팁
- **Kafka 포트 비공개**: compose 가 `127.0.0.1:9092` 로만 바인딩 → 외부에서 접근 불가. 회수는 SSH 경유.
- **`.env` 는 커밋 금지**(repo 루트 `.gitignore` 에 `.env` 포함). 키가 노출되면 즉시 재발급.
- **retention 7일**: `KAFKA_LOG_RETENTION_HOURS=168`. 디스크가 부족하면 값을 줄인다(`docker df`).
- 수집 잠깐 끄기: `crontab -r`(전체 해제) 또는 특정 줄 주석. 다시 켜면 이어서 쌓임.
- 로그: `docker logs -f subway-producer` / `docker logs subway-kafka`.
- API 점검 중이면 producer 가 `no events fetched`/`INFO-200` 을 찍고 넘어간다(정상).
