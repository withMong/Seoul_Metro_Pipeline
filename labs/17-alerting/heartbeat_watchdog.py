#!/usr/bin/env python3
"""무수신(completeness) 실시간 감시 — 노선·방향별 '마지막 수신 후 경과시간'.

왜 별도인가:
  null 알람(01-null-alarm-stream.sql + slack_alerter.py)은 *들어온 행*의 null,
  즉 **validity**를 잡는다. 그런데 2호선이 통째로 끊겨 행이 0개가 되는 **무수신
  (completeness)**은 윈도우 집계로 못 잡는다 — 없는 행은 집계 대상이 아니므로.
  → 행을 세지 말고, '마지막으로 수신한 시각'을 추적해 경과시간을 봐야 한다.

핵심 로직:
  - subway-events 를 구독해 (line, updn_line)별 '마지막 수신 wall-clock'을 추적.
  - 전체 스트림이 살아있는데(=수집 윈도우 가동 중) 특정 노선·방향만 GAP_SEC 이상
    조용하면 → '무수신' 경고.
  - 전체가 조용하면(QUIET_SEC 초과) 수집 윈도우 밖/전체 정전으로 보고 노선별 경고 보류
    (윈도우 사이 정적은 정상이므로 오탐 방지).
  - 기대 노선·방향은 SUBWAY_LINES × {상행,하행} 로 시드 → 처음부터 통째로 죽은 노선도 감지.

position_current 와의 관계:
  position_current(upsert PK)는 같은 정보(노선별 '마지막 상태/수신시각')를 **durable**
  하게 들고 있다. 즉 이 watchdog 가 실시간으로 계산하는 값을 테이블로 박제한 것이라,
  재시작 시 시드/복구의 '정답 소스'다. (그동안 dead-end였던 current 에 역할을 부여.)

freshness 분리:
  - 라이브 건강(지금 끊겼나?) = 이 watchdog (③ 실시간 레인)
  - 회고 freshness(어제 어디서 끊겼었나?) = 배치 gold(service_freshness)
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.request
from datetime import datetime

from confluent_kafka import Consumer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "subway-events")
SLACK = os.getenv("SLACK_WEBHOOK_URL", "").strip()
GAP_SEC = int(os.getenv("NORECV_GAP_SEC", "180"))       # 노선·방향 무수신 임계
QUIET_SEC = int(os.getenv("STREAM_QUIET_SEC", "90"))    # 전체 정적 = 윈도우 밖으로 간주
TICK = int(os.getenv("WATCHDOG_TICK_SEC", "20"))
COOLDOWN = int(os.getenv("ALERT_COOLDOWN_SEC", "300"))
LINES = [x.strip() for x in os.getenv("SUBWAY_LINES", "1호선,2호선,9호선").split(",") if x.strip()]
DIRS = {"0": "상행", "1": "하행"}
LOG_PATH = os.getenv("ALERT_LOG_PATH", "/workspace/labs/17-alerting/alerts.log")
# position_current 시드(StarRocks Paimon 카탈로그) — current 를 실제 입력으로 사용
STARROCKS_HOST = os.getenv("STARROCKS_HOST", "")
STARROCKS_PORT = int(os.getenv("STARROCKS_PORT", "9030"))
CURRENT_TABLE = os.getenv("CURRENT_TABLE", "paimon_catalog.bronze.subway_position_current")

last_seen: dict[tuple[str, str], float] = {}   # (line,dir) -> wall ts
last_sent: dict[tuple[str, str], float] = {}   # (line,dir) -> 마지막 경고 ts (쿨다운)
global_last = 0.0
start_ts = time.time()


def post_slack(text: str) -> None:
    if not SLACK:
        return
    try:
        req = urllib.request.Request(
            SLACK, data=json.dumps({"text": text}).encode("utf-8"),
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
    except Exception as exc:  # noqa: BLE001
        print(f"[slack-fail] {exc}", file=sys.stderr)


def emit(line: str, dr: str, elapsed: float) -> None:
    key = (line, dr)
    now = time.time()
    if now - last_sent.get(key, 0.0) < COOLDOWN:
        return
    last_sent[key] = now
    dl = DIRS.get(dr, dr)
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = (f"[{stamp}] WARN · {line} {dl} · no_data — "
           f"{int(elapsed)}s 무수신(>{GAP_SEC}s) — 노선·방향 끊김 의심")
    print(msg, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(msg + "\n")
    except Exception:  # noqa: BLE001
        pass
    post_slack(f"🟠 *무수신* `{line} {dl}`\n"
               f"{int(elapsed)}초간 수신 없음(>{GAP_SEC}s) — 노선·방향 끊김 의심")


def check() -> None:
    now = time.time()
    if now - global_last > QUIET_SEC:   # 전체 정적 = 윈도우 밖/전체 정전 → 노선별 경고 보류
        return
    grace = (now - start_ts) > GAP_SEC
    for line in LINES:
        for dr in DIRS:
            ls = last_seen.get((line, dr))
            if ls is None:
                if grace:                 # 스트림 활성인데 이 노선·방향만 처음부터 무수신
                    emit(line, dr, now - start_ts)
            elif now - ls > GAP_SEC:
                emit(line, dr, now - ls)


def seed_from_current() -> dict:
    """시작 시 position_current(StarRocks Paimon 카탈로그)에서 노선·방향별 '마지막 수신시각'을
    읽어 last_seen 시드 → current 가 watchdog 의 **실제 입력**으로 쓰임(dead-end 해소).
    실패(StarRocks/카탈로그/pymysql 없음)하면 빈 dict → 스트림만으로 동작."""
    if not STARROCKS_HOST:
        return {}
    try:
        import pymysql
    except ImportError:
        print("[seed] pymysql 미설치 — current 시드 건너뜀(스트림만)", file=sys.stderr)
        return {}
    try:
        conn = pymysql.connect(host=STARROCKS_HOST, port=STARROCKS_PORT,
                               user="root", password="", connect_timeout=5, read_timeout=10)
        with conn.cursor() as cur:
            cur.execute(f"SELECT line, updn_line, MAX(recptn_dt) "
                        f"FROM {CURRENT_TABLE} GROUP BY line, updn_line")
            rows = cur.fetchall()
        conn.close()
    except Exception as exc:  # noqa: BLE001
        print(f"[seed] position_current 조회 실패({exc}) — 스트림만으로 진행", file=sys.stderr)
        return {}

    def epoch(s):
        try:
            return datetime.strptime(str(s)[:19], "%Y-%m-%d %H:%M:%S").timestamp()
        except Exception:  # noqa: BLE001
            return None

    parsed = [(r[0], str(r[1]), epoch(r[2])) for r in rows if r and r[0] and epoch(r[2])]
    if not parsed:
        return {}
    data_latest = max(p[2] for p in parsed)   # 데이터 기준 '가장 최근 수신'(시간대 무관 상대비교)
    now = time.time()
    seed = {(line, dr): now - (data_latest - e) for line, dr, e in parsed}
    print(f"[seed] position_current 에서 {len(seed)}개 (노선·방향) 시드 — "
          f"current 가 실제 입력으로 사용됨", flush=True)
    return seed


def main() -> int:
    global global_last
    seeded = seed_from_current()
    if seeded:
        last_seen.update(seeded)
        global_last = max([global_last] + list(last_seen.values()))
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "dq-heartbeat-watchdog",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC])
    print(f"heartbeat watchdog 시작 · topic={TOPIC} · gap={GAP_SEC}s quiet={QUIET_SEC}s · "
          f"slack={'on' if SLACK else 'off(콘솔/로그만)'}", flush=True)
    next_tick = time.time() + TICK
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and not msg.error():
                try:
                    ev = json.loads(msg.value().decode("utf-8"))
                    line = ev.get("line")
                    dr = str(ev.get("updn_line"))
                    if line:
                        now = time.time()
                        last_seen[(line, dr)] = now
                        global_last = now
                except Exception:  # noqa: BLE001
                    pass
            if time.time() >= next_tick:
                check()
                next_tick = time.time() + TICK
    except KeyboardInterrupt:
        return 130
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
