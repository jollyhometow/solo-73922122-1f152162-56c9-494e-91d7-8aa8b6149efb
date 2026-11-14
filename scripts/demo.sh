#!/usr/bin/env bash
# 端到端演示脚本：正常评估（能改善 / 不能改善）+ 暂停断连后连接复用
# + 快照导出（创建 / 翻页 / 复用 / 换参数冲突）。
# 用法： BASE_URL=http://localhost:8000 bash scripts/demo.sh
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8000}"
PY="${PYTHON:-python3}"

req() {
  # req <json-file-or-inline> [extra curl args...]
  local payload="$1"; shift
  curl -sS -X POST "$BASE_URL/evaluate" \
    -H 'Content-Type: application/json' "$@" -d "$payload"
}

summarize() {
  "$PY" -c "
import json, sys
d = json.load(sys.stdin)
print('  基线总成本   :', d['baseline_total_cost'])
print('  候选总成本   :', d['hypothetical_total_cost'])
print('  成本变化     :', d['cost_delta'], '(改善比例', d['improvement_ratio'], ')')
print('  计划是否变化 :', d['plan_changed'])
print('  采纳候选     :', [c['index_name'] for c in d['adopted_candidates']])
print('  拒绝候选     :', [c['index_name'] for c in d['rejected_candidates']])
print('  基线前候选数 :', d['session_hypothetical_indexes_before_baseline'],
      '/ reset 后:', d['session_hypothetical_indexes_after_reset'])
"
}

echo "== 0. 健康检查（连接池硬上限应为 2） =="
curl -sS "$BASE_URL/health" | "$PY" -m json.tool

echo
echo "== 1. 能改善计划的候选 (warehouse_id, scan_time) =="
req '{"sample_id":"nightly_warehouse_scan",
      "candidates":[{"table":"public.package_scan","columns":["warehouse_id","scan_time"]}]}' | summarize

echo
echo "== 2. 不能改善计划的候选 (destination / weight_kg) =="
req '{"sample_id":"nightly_warehouse_scan",
      "candidates":[{"table":"public.package_scan","columns":["destination"]},
                    {"table":"public.package_scan","columns":["weight_kg"]}]}' | summarize

echo
echo "== 3. 全表聚合：B-tree 无法改善 =="
req '{"sample_id":"nightly_aggregate",
      "candidates":[{"table":"public.package_scan","columns":["warehouse_id"]},
                    {"table":"public.package_scan","columns":["scan_time"]}]}' | summarize

echo
echo "== 4. 只读守卫：UPDATE 必须被 400 拒绝 =="
req '{"sql_text":"UPDATE public.package_scan SET weight_kg=1 WHERE scan_id=1",
      "candidates":[{"table":"public.package_scan","columns":["warehouse_id"]}]}' \
  | "$PY" -c "import json,sys; print(' ', json.load(sys.stdin))"

echo
echo "== 5. 暂停期间断连，验证取消安全清理与连接复用 =="
# 30 秒暂停；2 秒后用超时强制断开 TCP（不依赖交互式 Ctrl+C）
if curl -sS --max-time 2 -X POST "$BASE_URL/evaluate" \
    -H 'Content-Type: application/json' -H 'X-Demo-Pause-Seconds: 30' \
    -d '{"sample_id":"nightly_warehouse_scan",
         "candidates":[{"table":"public.package_scan","columns":["warehouse_id","scan_time"]}]}' \
    > /tmp/demo_disconnect.out 2>/tmp/demo_disconnect.err; then
  echo "  (未发生超时，环境可能忽略了 --max-time)"
else
  echo "  客户端已在暂停 2 秒后强制断连"
fi
echo "  等待服务端完成取消安全清理 ..."
sleep 4
echo "  断连后立即发起新请求（必须成功且无候选残留）："
req '{"sample_id":"nightly_warehouse_scan",
      "candidates":[{"table":"public.package_scan","columns":["warehouse_id","scan_time"]}]}' | summarize
echo "  连接池状态："
curl -sS "$BASE_URL/health" | "$PY" -c "import json,sys; print('  ', json.dumps(json.load(sys.stdin)['pool'], ensure_ascii=False))"

echo
echo "== 6. 快照导出：创建 / 翻页 / 复用 / 换参数冲突 =="
EXPORT_KEY="demo-$(date +%s)-$$"
SNAP_JSON=$(
  curl -sS -X POST "$BASE_URL/snapshots" \
    -H 'Content-Type: application/json' \
    -d "{\"export_key\":\"$EXPORT_KEY\",\"warehouse_id\":3,
         \"business_date\":\"2026-11-11\",\"page_size\":5}"
)
echo "$SNAP_JSON" | "$PY" -c "
import json, sys
d = json.load(sys.stdin)
print('  快照号       :', d['snapshot_id'])
print('  窗口         :', d['window_start'], '~', d['window_end'])
print('  冻结记录数   :', d['total_rows'], '(截断:', d['truncated'], ')')
print('  首批条数     :', d['returned_rows'], '/ 页大小', d['page_size'])
print('  是否有下一页 :', d['has_more'])
print('  首批首条     :', d['records'][0] if d['records'] else None)
"
SNAP_ID=$(echo "$SNAP_JSON" | "$PY" -c "import json,sys;print(json.load(sys.stdin)['snapshot_id'])")
CURSOR=$(echo "$SNAP_JSON" | "$PY" -c "import json,sys;print(json.load(sys.stdin)['next_cursor'])")

echo "  -- 用游标翻到第 2 页 --"
curl -sS "$BASE_URL/snapshots/$SNAP_ID/pages?cursor=$CURSOR" | "$PY" -c "
import json, sys
d = json.load(sys.stdin)
print('  偏移         :', d['offset'], '/', d['total_rows'])
print('  本页条数     :', d['returned_rows'])
print('  本页 scan_id :', [r['scan_id'] for r in d['records']])
print('  下一游标     :', (d['next_cursor'] or '')[:40] + '...')
"

echo "  -- 同键同参数重试：必须复用同一快照 --"
curl -sS -X POST "$BASE_URL/snapshots" \
  -H 'Content-Type: application/json' \
  -d "{\"export_key\":\"$EXPORT_KEY\",\"warehouse_id\":3,
       \"business_date\":\"2026-11-11\"}" | "$PY" -c "
import json, sys
d = json.load(sys.stdin)
print('  快照号       :', d['snapshot_id'])
print('  是否复用     :', d['reused'])
assert d['snapshot_id'] == '$SNAP_ID', '同键同参数未复用同一快照'
print('  ✓ 复用同一构建结果')
"

echo "  -- 同键换仓号：必须 409 --"
CODE=$(curl -sS -o /tmp/demo_conflict.out -w '%{http_code}' -X POST "$BASE_URL/snapshots" \
  -H 'Content-Type: application/json' \
  -d "{\"export_key\":\"$EXPORT_KEY\",\"warehouse_id\":4,
       \"business_date\":\"2026-11-11\"}")
echo "  HTTP $CODE"
cat /tmp/demo_conflict.out; echo
[ "$CODE" = "409" ] || { echo "  ✗ 期望 409"; exit 1; }
echo "  ✓ 换参数返回冲突"

echo "  快照观测："
curl -sS "$BASE_URL/health" | "$PY" -c "import json,sys; print('  ', json.dumps(json.load(sys.stdin)['snapshots'], ensure_ascii=False))"
