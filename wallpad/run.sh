#!/usr/bin/env bash
set -e  # 오류 발생 시 즉시 종료

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

source venv/bin/activate

nohup python3 -u iterate.py > output.log 2>&1 &
echo $! > run.pid

echo "Python started with PID $(cat run.pid)"
