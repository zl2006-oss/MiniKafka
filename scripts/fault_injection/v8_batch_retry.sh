#!/usr/bin/env bash
# V8：批量失败 / 重试耗尽（R23/R24/R29/R3）。
# 用 `--min-free-space-mb` 抬高阈值模拟盘满，使同分区 3 条记录全部失败：
#   - 失败条目之后的条目被 Broker 标记为 partition_blocked（R24/AD-14）；
#   - Producer 按 R3 重试，耗尽后退出码 4，输出逐条结果 + R29 汇总；
#   - 失败批次不占用 offset，释放空间后从原 offset 接续。
set -euo pipefail

cd "$(dirname "$0")/../.."
MK="${MK:-./build/minikafka}"
PORT="${PORT:-9092}"
WORK="$(mktemp -d "/tmp/mk-v8.XXXXXX")"
DATA="$WORK/data"
LOG="$DATA/topics/batch/0/00000000000000000000.log"
BROKER_PID=""
FAILED=0

start_broker() { # <min-free-space-mb>
  : >"$WORK/broker.log"
  "$MK" broker --port "$PORT" --data-dir "$DATA" --min-free-space-mb "$1" \
    >>"$WORK/broker.log" 2>&1 &
  BROKER_PID=$!
  for _ in $(seq 1 100); do
    grep -q '"ready"' "$WORK/broker.log" && return 0
    sleep 0.1
  done
  echo "broker 未就绪" >&2
  return 1
}

stop_broker() {
  if [[ -n "$BROKER_PID" ]] && kill -0 "$BROKER_PID" 2>/dev/null; then
    kill -TERM "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
  fi
  BROKER_PID=""
}

cleanup() {
  stop_broker
  rm -rf "$WORK"
}
trap cleanup EXIT

pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; FAILED=$((FAILED + 1)); }
check_eq() { if [[ "$2" == "$3" ]]; then pass "$1"; else fail "$1 (actual=$2 expected=$3)"; fi; }

mkdir -p "$DATA"

# --- 阶段 1：正常空间，写入 1 条基线 ---
start_broker 256
"$MK" topic create --topic batch --partitions 1 >/dev/null 2>&1
printf '{"value":"baseline"}\n' | "$MK" produce --topic batch --partition 0 \
  >"$WORK/base.out" 2>&1
check_eq "基线写入 1 条成功" "$(grep -c '"status":"ok"' "$WORK/base.out")" "1"
SIZE_BASELINE="$(stat -c %s "$LOG")"
stop_broker

# --- 阶段 2：盘满，3 条同分区记录全部失败并重试耗尽 ---
start_broker 1048576
printf '%s\n' '{"value":"a"}' '{"value":"b"}' '{"value":"c"}' >"$WORK/input.jsonl"
set +e
"$MK" produce --topic batch --partition 0 --file "$WORK/input.jsonl" \
  --retries 1 --retry-backoff-ms 10 --retry-timeout-ms 100 \
  >"$WORK/fail.out" 2>"$WORK/fail.err"
RC=$?
set -e
check_eq "重试耗尽退出码 4" "$RC" "4"
check_eq "逐条结果共 3 行" "$(grep -c '"index":' "$WORK/fail.out")" "3"
grep -q '"index":0,.*"status":"error".*"error":"disk_full"' "$WORK/fail.out" \
  && pass "首条返回 disk_full" || fail "首条未返回 disk_full"
grep -q '"index":1,.*"status":"error".*"error":"partition_blocked"' "$WORK/fail.out" \
  && pass "后续条目被阻塞（R24）" || fail "后续条目未被阻塞"
grep -q '"index":2,.*"status":"error".*"error":"partition_blocked"' "$WORK/fail.out" \
  && pass "第 3 条被阻塞（R24）" || fail "第 3 条未被阻塞"
grep -q '"event":"produce_summary","status":"failed","succeeded":0,"failed":3' \
  "$WORK/fail.out" && pass "汇总 failed/0 成功" || fail "汇总计数不符"
grep -q '"failed_indices":\[0,1,2\]' "$WORK/fail.out" \
  && pass "汇总列出失败条目（R29）" || fail "汇总缺少失败条目"
grep -q '"exit_reason":"retry_exhausted"' "$WORK/fail.out" \
  && pass "汇总标注 retry_exhausted" || fail "汇总缺少 exit_reason"
grep -q '"event":"retry"' "$WORK/fail.err" \
  && pass "输出重试事件（R3）" || fail "缺少重试事件"
check_eq "失败批次未落盘（日志长度不变）" "$(stat -c %s "$LOG")" "$SIZE_BASELINE"

# 失败条目不得占用 offset：释放空间后新写入仍为 offset=1。
stop_broker
start_broker 256
printf '{"value":"after"}\n' | "$MK" produce --topic batch --partition 0 \
  >"$WORK/resume.out" 2>&1
grep -q '"offset":1' "$WORK/resume.out" \
  && pass "释放空间后从 offset=1 接续" || fail "offset 被失败批次占用"
"$MK" replay --topic batch --partition 0 >"$WORK/all.out" 2>&1
check_eq "最终日志 2 条（基线 + 恢复后）" \
  "$(grep -c '"topic":"batch"' "$WORK/all.out")" "2"
stop_broker

echo
if [[ "$FAILED" -eq 0 ]]; then
  echo "v8: 全部通过"
  exit 0
fi
echo "v8: $FAILED 项失败"
exit 1
