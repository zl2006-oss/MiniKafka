#!/usr/bin/env bash
# 磁盘空间不足（Q-P11 / R3）：--min-free-space-mb 设成大于可用空间，写入被拒返回 disk_full；
# 恢复阈值后写入正常。不破坏文件系统，只验证"预检 → 拒写 → 不落盘"这条链路。
set -euo pipefail

cd "$(dirname "$0")/../.."
MK="${MK:-./build/minikafka}"
PORT="${PORT:-9092}"
WORK="$(mktemp -d "/tmp/mk-disk.XXXXXX")"
DATA="$WORK/data"
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

# 1 TiB 阈值 ≫ 实际可用空间：所有写入都应被预检拦截。
start_broker 1048576
"$MK" topic create --topic tight --partitions 1 >/dev/null 2>&1

set +e
printf '{"key":"k","value":"v"}\n' \
  | "$MK" produce --topic tight --retries 0 --retry-timeout-ms 1 \
      >"$WORK/produce1.out" 2>&1
RC=$?
set -e
check_eq "空间不足时退出码 4（投递失败）" "$RC" "4"
grep -q '"error":"disk_full"' "$WORK/produce1.out" \
  && pass "逐条结果返回 disk_full" || fail "未返回 disk_full"
grep -q '"exit_reason":"retry_exhausted"' "$WORK/produce1.out" \
  && pass "汇总标注 retry_exhausted" || fail "汇总缺少 exit_reason"
grep -q '"status":"failed"' "$WORK/produce1.out" \
  && pass "汇总标注 failed" || fail "汇总状态不符"

# 拒绝写入后分区日志不应增长（预检在追加前完成）。
LOG="$DATA/topics/tight/0/00000000000000000000.log"
SIZE_AFTER_REJECT="$(stat -c %s "$LOG" 2>/dev/null || echo 0)"
check_eq "被拒写入未落盘（日志 0 字节）" "$SIZE_AFTER_REJECT" "0"
stop_broker

# 恢复默认阈值后同一数据目录可正常写入。
start_broker 256
printf '{"key":"k","value":"ok"}\n' \
  | "$MK" produce --topic tight >"$WORK/produce2.out" 2>&1
grep -q '"status":"ok"' "$WORK/produce2.out" \
  && pass "恢复阈值后写入成功" || fail "恢复阈值后仍失败"
grep -q '"offset":0' "$WORK/produce2.out" \
  && pass "被拒写入未占用 offset（首个成功 offset=0）" \
  || fail "offset 不连续（被拒写入可能占用了 offset）"
# 一条记录 = 24B 头 + key(1) + value(2)。
check_eq "日志长度等于一条记录" "$(stat -c %s "$LOG" 2>/dev/null || echo 0)" "27"
stop_broker

echo
if [[ "$FAILED" -eq 0 ]]; then
  echo "disk_full: 全部通过"
  exit 0
fi
echo "disk_full: $FAILED 项失败"
exit 1
