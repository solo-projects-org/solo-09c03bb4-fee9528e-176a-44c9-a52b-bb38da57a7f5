#!/usr/bin/env bash
# 裂纹事件分发链路一键演示（需先启动：docker compose up --build）。
# 演示：正常投递 → 幂等重提/异内容拒绝 → 单端故障与恢复 → 回执丢失与恢复。
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
QC_URL="${QC_URL:-http://127.0.0.1:8100}"
REPORT_URL="${REPORT_URL:-http://127.0.0.1:8200}"
EVENT_FILE="${EVENT_FILE:-samples/crack_event.json}"

EVENT_ID=$(python3 -c "import json; print(json.load(open('${EVENT_FILE}'))['event_id'])")

pp() { python3 -m json.tool --no-ensure-ascii; }
hr() { echo "================================================================"; }
step() { hr; echo "▶ $1"; echo "----------------------------------------------------------------"; }

step "1. 正常投递：POST /api/v1/events（先持久化，再向 qc / daily_report 投递）"
curl -sS -X POST "${BASE_URL}/api/v1/events" \
  -H "Content-Type: application/json" \
  --data-binary "@${EVENT_FILE}" | pp

step "2. 幂等重提：同号同内容再提交 → 200 deduplicated，只保留一条"
curl -sS -X POST "${BASE_URL}/api/v1/events" \
  -H "Content-Type: application/json" \
  --data-binary "@${EVENT_FILE}" | pp

step "3. 同号异内容 → 409 拒绝"
python3 -c "
import json
e = json.load(open('${EVENT_FILE}'))
e['defect_level'] = 'L3'
print(json.dumps(e, ensure_ascii=False))
" | curl -sS -X POST "${BASE_URL}/api/v1/events" \
  -H "Content-Type: application/json" \
  --data-binary @- | pp

step "4. 单端故障：qc 接收端注入 down（不保存直接 503）"
curl -sS -X POST "${QC_URL}/admin/behavior" \
  -H "Content-Type: application/json" -d '{"mode": "down"}' | pp

step "5. 故障期间补传新事件 EVT-DEMO-DOWN-1：qc 待送、daily_report 已确认"
curl -sS -X POST "${BASE_URL}/api/v1/events" \
  -H "Content-Type: application/json" \
  -d '{"event_id": "EVT-DEMO-DOWN-1", "car_id": "GJC-018", "mileage_m": 129000, "defect_level": "L1"}' | pp

step "6. qc 恢复正常并手动补送 → qc 确认，已确认端不重复投递"
curl -sS -X POST "${QC_URL}/admin/behavior" \
  -H "Content-Type: application/json" -d '{"mode": "normal"}' | pp
curl -sS -X POST "${BASE_URL}/api/v1/events/EVT-DEMO-DOWN-1/redeliver" | pp

step "7. 回执丢失：daily_report 注入 ack_loss（保存成功但回执 500）"
curl -sS -X POST "${REPORT_URL}/admin/behavior" \
  -H "Content-Type: application/json" -d '{"mode": "ack_loss"}' | pp

step "8. 补传新事件 EVT-DEMO-ACK-1：分发端记 daily_report 待送，但接收端实际已保存"
curl -sS -X POST "${BASE_URL}/api/v1/events" \
  -H "Content-Type: application/json" \
  -d '{"event_id": "EVT-DEMO-ACK-1", "car_id": "GJC-018", "mileage_m": 130200, "defect_level": "L3"}' | pp
echo "daily_report 接收端实际已保存："
curl -sS "${REPORT_URL}/stats" | pp

step "9. 恢复正常并补送 → 确认；重投只入库一次（stored_events 不变），qc 确认不被撤销"
curl -sS -X POST "${REPORT_URL}/admin/behavior" \
  -H "Content-Type: application/json" -d '{"mode": "normal"}' | pp
curl -sS -X POST "${BASE_URL}/api/v1/events/EVT-DEMO-ACK-1/redeliver" | pp
echo "daily_report 统计（stored_events 不重复计数，duplicates_suppressed 记录去重）："
curl -sS "${REPORT_URL}/stats" | pp

step "10. 逐端状态查询：GET /api/v1/events/{event_id}"
curl -sS "${BASE_URL}/api/v1/events/${EVENT_ID}" | pp

hr
echo "演示完成。也可体验：docker compose restart contract-scheduler 后，"
echo "未确认的投递会由后台循环自动补送（RETRY_INTERVAL_SECONDS 周期）。"
