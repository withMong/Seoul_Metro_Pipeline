#!/usr/bin/env bash
# =====================================================================
# Oracle Cloud Always Free (Ubuntu 22.04) 부트스트랩: Docker + 타임존
# 새로 만든 VM 에 최초 1회 실행. (ssh 로 접속 후)
#   curl -fsSL <이 파일 raw url> | bash    또는   bash setup-vm.sh
# =====================================================================
set -euo pipefail

echo "== 1) 타임존 KST =="
sudo timedatectl set-timezone Asia/Seoul

echo "== 1b) 스왑 2GB (E2.1.Micro RAM 1GB 대응 — Kafka OOM 방지) =="
if ! sudo swapon --show | grep -q /swapfile; then
  sudo fallocate -l 2G /swapfile || sudo dd if=/dev/zero of=/swapfile bs=1M count=2048
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile
  sudo swapon /swapfile
  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
  echo "  스왑 활성화 완료:"; sudo swapon --show
else
  echo "  스왑 이미 있음"
fi

echo "== 2) 기본 패키지 =="
sudo apt-get update -y
sudo apt-get install -y ca-certificates curl git

echo "== 3) Docker 설치 (공식 스크립트) =="
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sudo sh
fi
sudo usermod -aG docker "$USER" || true

echo ""
echo "== 완료 =="
echo "  1. 로그아웃 후 재접속(도커 그룹 반영):  exit  →  다시 ssh"
echo "  2. git clone https://github.com/withMong/Seoul_Metro_Pipeline.git"
echo "  3. cd Seoul_Metro_Pipeline/deploy/oracle-collector"
echo "  4. cp .env.example .env  &&  nano .env   (SEOUL_API_KEY 입력)"
echo "  5. docker compose -f docker-compose.cloud.yml up -d --build kafka"
echo "  6. docker compose -f docker-compose.cloud.yml create producer"
echo "  7. crontab -e  →  crontab.txt 내용 붙여넣기"
