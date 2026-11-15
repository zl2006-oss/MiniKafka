#!/usr/bin/env bash
# V6：磁盘满 —— 空间不足时写入被拒（ack 前预检）、已有数据仍可读、Broker 不崩溃、
# 释放空间后恢复写入。用 --min-free-space-mb 把阈值抬高来模拟"盘满"，不需要真填满磁盘。
set -euo pipefail

cd "$(dirname "$0")/../.."
MK="${MK:-./build/minikafka}"
PORT="${PORT:-9092}"
WORK="$(mktemp -d "/tmp/mk-disk.XXXXXX")"
DATA="$WORK/data"
LOG="$DATA/topics/tight/0/00000000000000000000.log"
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

# --- 阶段 1：正常空间，写入 2 条基线数据 ---
start_broker 256
"$MK" topic create --topic tight --partitions 1 >/dev/null 2>&1
printf '{"key":"k","value":"a"}\n{"key":"k","value":"b"}\n' \
  | "$MK" produce --topic tight >"$WORK/base.out" 2>&1
check_eq "基线写入 2 条成功" "$(grep -c '"status":"ok"' "$WORK/base.out")" "2"
SIZE_BASELINE="$(stat -c %s "$LOG")"
stop_broker

# --- 阶段 2：把阈值抬到 1 TiB（模拟盘满），写入必须被拒 ---
start_broker 1048576
set +e
printf '{"key":"k","value":"c"}\n' \
  | "$MK" produce --topic tight --retries 0 --retry-timeout-ms 1 \
      >"$WORK/reject.out" 2>&1
RC=$?
set -e
check_eq "空间不足时退出码 4（投递失败）" "$RC" "4"
grep -q '"error":"disk_full"' "$WORK/reject.out" \
  && pass "逐条结果返回 disk_full" || fail "未返回 disk_full"
grep -q '"exit_reason":"retry_exhausted"' "$WORK/reject.out" \
  && pass "汇总标注 retry_exhausted" || fail "汇总缺少 exit_reason"
check_eq "被拒写入未落盘（日志长度不变）" "$(stat -c %s "$LOG")" "$SIZE_BASELINE"

# 已有数据仍可读（R16：读路径不受低空间影响）。
"$MK" replay --topic tight --partition 0 >"$WORK/replay.out" 2>&1
check_eq "盘满时已有数据可读（2 条）" "$(grep -c '"topic":"tight"' "$WORK/replay.out")" "2"
"$MK" consume --topic tight --group g --consumer-index 0 --consumer-count 1 \
  --auto-commit-interval 1s --exit-at-end >"$WORK/consume.out" 2>/dev/null
check_eq "盘满时仍可消费（2 条）" "$(grep -c '"topic":"tight"' "$WORK/consume.out")" "2"

# Broker 未崩溃：仍在监听且能正常响应。
if kill -0 "$BROKER_PID" 2>/dev/null; then
  pass "被拒写入后 Broker 仍在运行"
else
  fail "Broker 因 disk_full 退出"
fi
check_eq "Broker 未产生崩溃日志" "$(grep -c 'internal_error' "$WORK/broker.log")" "0"
stop_broker

# --- 阶段 3：释放空间（恢复阈值）后继续写入，offset 从 2 接续 ---
start_broker 256
printf '{"key":"k","value":"c"}\n' | "$MK" produce --topic tight >"$WORK/resume.out" 2>&1
grep -q '"status":"ok"' "$WORK/resume.out" \
  && pass "恢复空间后写入成功" || fail "恢复空间后仍失败"
grep -q '"offset":2' "$WORK/resume.out" \
  && pass "被拒写入未占用 offset（接续 offset=2）" \
  || fail "offset 不连续（被拒写入可能占用了 offset）"
# 三条记录：24B 头 + key(1) + value(1) 各 26B。
check_eq "日志长度等于 3 条记录" "$(stat -c %s "$LOG")" "78"
stop_broker

echo
if [[ "$FAILED" -eq 0 ]]; then
  echo "disk_full: 全部通过"
  exit 0
fi
echo "disk_full: $FAILED 项失败"
exit 1
