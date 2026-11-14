#!/usr/bin/env bash
# 用两个内置样例调用排程 API（需先启动服务：docker compose up 或 uvicorn）。
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"

run() {
  local title="$1" file="$2"
  echo "================================================================"
  echo "样例：${title}"
  echo "POST ${BASE_URL}/api/v1/plan  <- ${file}"
  echo "----------------------------------------------------------------"
  curl -sS -X POST "${BASE_URL}/api/v1/plan" \
    -H "Content-Type: application/json" \
    --data-binary "@${file}" | python3 -m json.tool --no-ensure-ascii
  echo
}

run "先升级订阅端才能新增缺陷等级（有安全顺序）" samples/safe_subscriber_first.json
run "新增枚举值撞上订阅端新必填字段（无解）" samples/infeasible_circular.json
