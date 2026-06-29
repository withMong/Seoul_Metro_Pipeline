#!/usr/bin/env python3
"""실시간 null 알람 — Flink DQ 윈도우 집계를 구독해 Slack 으로 경고.

Flink(01-null-alarm-stream.sql)가 1분 텀블링 윈도우로 노선별 null 건수를
subway-dq-alerts 토픽에 적재한다. 이 서비스가 그걸 읽어 규칙 위반 시 Slack 으로
알린다. (Slack 미설정 시 콘솔/로그만 — 의존성 없이 동작)

규칙
  1) CRITICAL — 절대 null 이면 안 되는 필드(statn_id/statn_nm/recptn_dt)가
     윈도우 안에서 1건이라도 null → 스키마/파싱 이상 의심.
  2) WARN(train_no) — train_no null 비율 급증. 단 1호선은 코레일 구간에서
     trainNo 가 사라지는 게 정상이라 제외(오탐 방지).
  3) WARN(low_volume) — 윈도우 수신 건수가 기준 미만 → 수집 지연/장애 의심.

쿨다운: (노선, 규칙)별로 ALERT_COOLDOWN_SEC 동안 한 번만 알림(스팸 방지).

환경변수
  KAFKA_BOOTSTRAP_SERVER  기본 localhost:9092 (도커 내부 kafka:19092)
  DQ_ALERT_TOPIC          기본 subway-dq-alerts
  SLACK_WEBHOOK_URL       Slack Incoming Webhook URL (없으면 콘솔/로그만)
  ALERT_COOLDOWN_SEC      기본 300
  MIN_ROWS_PER_WINDOW     기본 5
  TRAIN_NO_NULL_RATE      기본 0.5
  ALERT_LOG_PATH          기본 /workspace/labs/17-alerting/alerts.log
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
TOPIC = os.getenv("DQ_ALERT_TOPIC", "subway-dq-alerts")
SLACK = os.getenv("SLACK_WEBHOOK_URL", "").strip()
COOLDOWN = int(os.getenv("ALERT_COOLDOWN_SEC", "300"))
MIN_ROWS = int(os.getenv("MIN_ROWS_PER_WINDOW", "5"))
TRAIN_NO_NULL_RATE = float(os.getenv("TRAIN_NO_NULL_RATE", "0.5"))
LOG_PATH = os.getenv("ALERT_LOG_PATH", "/workspace/labs/17-alerting/alerts.log")

# 절대 null 이면 안 되는 필드(=null 시 즉시 CRITICAL)
CRITICAL = ["null_statn_id", "null_statn_nm", "null_recptn_dt"]

_last_sent: dict[tuple[str, str], float] = {}  # (line, rule) -> 마지막 발송 시각


def _throttled(key: tuple[str, str]) -> bool:
    now = time.time()
    if now - _last_sent.get(key, 0.0) < COOLDOWN:
        return True
    _last_sent[key] = now
    return False


def post_slack(text: str) -> None:
    if not SLACK:
        return
    body = json.dumps({"text": text}).encode("utf-8")
    req = urllib.request.Request(
        SLACK, data=body, headers={"Content-Type": "application/json"}
    )
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as exc:  # noqa: BLE001 — 알림 실패가 서비스를 죽이지 않게
        print(f"[slack-fail] {exc}", file=sys.stderr)


def emit(level: str, line: str, rule: str, text: str) -> None:
    if _throttled((line, rule)):
        return
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    msg = f"[{stamp}] {level} · {line} · {rule} — {text}"
    print(msg, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as fh:
            fh.write(msg + "\n")
    except Exception:  # noqa: BLE001
        pass
    icon = "🔴" if level == "CRITICAL" else "🟡"
    post_slack(f"{icon} *{level}* `{line}` *{rule}*\n{text}")


def evaluate(rec: dict) -> None:
    line = rec.get("line") or "(null)"
    total = int(rec.get("total") or 0)
    wend = rec.get("window_end", "")

    # 1) 절대 null 금지 필드
    for col in CRITICAL:
        n = int(rec.get(col) or 0)
        if n > 0:
            field = col.replace("null_", "")
            emit(
                "CRITICAL", line, field,
                f"{wend} 창에서 {field} null {n}건 / 총 {total}건 — 스키마·파싱 이상 의심",
            )

    # 2) train_no null 급증 (1호선 코레일 구간은 정상 → 제외)
    ntn = int(rec.get("null_train_no") or 0)
    if total > 0 and line != "1호선" and (ntn / total) >= TRAIN_NO_NULL_RATE:
        emit(
            "WARN", line, "train_no",
            f"{wend} 창 train_no null {ntn}/{total} ({ntn / total:.0%}) — 비정상 급증",
        )

    # 3) 수신량 급감
    if 0 < total < MIN_ROWS:
        emit(
            "WARN", line, "low_volume",
            f"{wend} 창 수신 {total}건(<{MIN_ROWS}) — 수집 지연/장애 의심",
        )


def main() -> int:
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": "dq-slack-alerter",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC])
    print(
        f"DQ alerter 시작 · topic={TOPIC} · "
        f"slack={'on' if SLACK else 'off(콘솔/로그만)'} · cooldown={COOLDOWN}s",
        flush=True,
    )
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                print(f"[kafka-err] {msg.error()}", file=sys.stderr)
                continue
            try:
                rec = json.loads(msg.value().decode("utf-8"))
            except Exception as exc:  # noqa: BLE001
                print(f"[parse-err] {exc}", file=sys.stderr)
                continue
            evaluate(rec)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr)
        return 130
    finally:
        consumer.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
