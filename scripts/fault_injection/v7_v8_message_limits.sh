#!/usr/bin/env bash
# V7 + V8 前半段（R17/R23/R24）：
# - V7：单条超 1 MiB 明确拒绝、不截断、不静默，Broker 保持运行；
# - V8：批内部分失败——超限条目单独失败，同批合法条目仍成功，
#       汇总给出 succeeded/failed 与 exit_reason。
# 说明：客户端在发送前即按 24B 头 + key + value 口径拒绝超限记录，因此不会占用 offset。
set -euo pipefail

cd "$(dirname "$0")/../.."
MK="${MK:-./build/minikafka}"
PORT="${PORT:-9092}"
WORK="$(mktemp -d "/tmp/mk-msg.XXXXXX")"
DATA="$WORK/data"
BROKER_PID=""
FAILED=0

cleanup() {
  if [[ -n "$BROKER_PID" ]] && kill -0 "$BROKER_PID" 2>/dev/null; then
    kill -TERM "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; FAILED=$((FAILED + 1)); }
check_eq() { if [[ "$2" == "$3" ]]; then pass "$1"; else fail "$1 (actual=$2 expected=$3)"; fi; }

mkdir -p "$DATA"
"$MK" broker --port "$PORT" --data-dir "$DATA" >"$WORK/broker.log" 2>&1 &
BROKER_PID=$!
for _ in $(seq 1 100); do
  grep -q '"ready"' "$WORK/broker.log" && break
  sleep 0.1
done
"$MK" topic create --topic big --partitions 1 >/dev/null 2>&1

# 构造三行：合法小记录、临界合法记录（24+value=1048576）、超限记录。
python3 - "$WORK/input.jsonl" <<'PY'
import json, sys
limit = 1024 * 1024
rows = [
    {"value": "small"},
    {"value": "x" * (limit - 24)},      # 恰好等于上限，应通过
    {"value": "x" * (limit - 24 + 1)},  # 超 1 字节，应被拒
]
with open(sys.argv[1], "w", encoding="utf-8") as handle:
    for row in rows:
        handle.write(json.dumps(row) + "\n")
PY

set +e
"$MK" produce --topic big --file "$WORK/input.jsonl" >"$WORK/produce.out" 2>&1
RC=$?
set -e
check_eq "含超限条目时退出码 3" "$RC" "3"
check_eq "逐条结果共 3 行" "$(grep -c '"index":' "$WORK/produce.out")" "3"
grep -q '"index":0,.*"status":"ok"' "$WORK/produce.out" \
  && pass "index 0 成功" || fail "index 0 未成功"
grep -q '"index":1,.*"status":"ok"' "$WORK/produce.out" \
  && pass "临界 1 MiB 记录成功" || fail "临界 1 MiB 记录被误拒"
grep -q '"index":2,.*"error":"message_too_large"' "$WORK/produce.out" \
  && pass "超限记录返回 message_too_large" || fail "超限记录错误码不符"

grep -q '"status":"partial_failure"' "$WORK/produce.out" \
  && pass "汇总标注 partial_failure" || fail "汇总状态不符"
grep -q '"succeeded":2,"failed":1' "$WORK/produce.out" \
  && pass "汇总计数正确（2 成功 / 1 失败）" || fail "汇总计数不符"
grep -q '"exit_reason":"validation_error"' "$WORK/produce.out" \
  && pass "汇总标注 validation_error" || fail "汇总缺少 validation_error"

# 全成功时不输出汇总（R23：只在有失败时给对账信息）。
printf '{"value":"a"}\n{"value":"b"}\n' | "$MK" produce --topic big >"$WORK/ok.out" 2>&1
if grep -q 'produce_summary' "$WORK/ok.out"; then
  fail "全成功时不应输出汇总"
else
  pass "全成功时无汇总输出"
fi

# 超限记录不占用 offset：失败后继续写入应从 4 接续（3 条成功：2 条 + 上面 2 条 = 4）。
printf '{"value":"c"}\n' | "$MK" produce --topic big >"$WORK/next.out" 2>&1
grep -q '"offset":4' "$WORK/next.out" \
  && pass "超限记录未占用 offset（下一条 offset=4）" \
  || fail "offset 出现空洞"

# V7：拒绝超限消息后 Broker 仍存活且继续服务（未被超限负载影响）。
if kill -0 "$BROKER_PID" 2>/dev/null; then
  pass "超限拒绝后 Broker 仍在运行"
else
  fail "Broker 因超限消息退出"
fi
"$MK" topic list >"$WORK/list.out" 2>&1
grep -q '"topic":"big"' "$WORK/list.out" \
  && pass "拒绝后仍可正常响应请求" || fail "拒绝后 Broker 无响应"
"$MK" replay --topic big --partition 0 >"$WORK/all.out" 2>&1
# 合法记录共 5 条：3 行输入里 2 条 + 2 条全成功 + 1 条续写；超限那 1 条不在日志中。
check_eq "日志未出现超限消息（记录数 = 5）" \
  "$(grep -c '"topic":"big"' "$WORK/all.out")" "5"

kill -TERM "$BROKER_PID"
wait "$BROKER_PID" 2>/dev/null || true
BROKER_PID=""

echo
if [[ "$FAILED" -eq 0 ]]; then
  echo "message_too_large: 全部通过"
  exit 0
fi
echo "message_too_large: $FAILED 项失败"
exit 1
