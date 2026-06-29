#!/usr/bin/env bash
# 환경 점검: Docker CLI/데몬/Compose/메모리.
set -euo pipefail
fail() { echo "ERROR: $*" >&2; exit 1; }

echo "== Docker =="
command -v docker >/dev/null 2>&1 || fail "Docker CLI 미설치."
docker --version
docker compose version

echo; echo "== Docker daemon =="
docker info >/dev/null 2>&1 || fail "Docker 데몬 미동작. Docker Desktop 켜고 다시 실행."
docker info --format 'Server Version: {{.ServerVersion}}'
docker info --format 'CPUs: {{.NCPU}}'
docker info --format 'Total Memory: {{.MemTotal}} bytes'

echo; echo "환경 점검 완료."
