#!/usr/bin/env bash
# V1：进程崩溃恢复 —— 多批写入并记录 ack offset，对 Broker kill -9，直接重启，
# 校验已 ack 消息全部可读、offset 连续无空洞、未 ack 的残缺尾部被丢弃（R7/R27）。
set -euo pipefail

cd "$(dirname "$0")/../.."
MK="${MK:-./build/minikafka}"
PORT="${PORT:-9092}"
WORK="$(mktemp -d "/tmp/mk-v1.XXXXXX")"
DATA="$WORK/data"
BROKER_PID=""
FAILED=0

start_broker() {
  : >"$WORK/broker.log"
  "$MK" broker --port "$PORT" --data-dir "$DATA" >>"$WORK/broker.log" 2>&1 &
  BROKER_PID=$!
  for _ in $(seq 1 100); do
    grep -q '"ready"' "$WORK/broker.log" && return 0
    sleep 0.1
  done
  echo "broker 未就绪" >&2
  return 1
}

stop_broker_graceful() {
  if [[ -n "$BROKER_PID" ]] && kill -0 "$BROKER_PID" 2>/dev/null; then
    kill -TERM "$BROKER_PID" 2>/dev/null || true
    wait "$BROKER_PID" 2>/dev/null || true
  fi
  BROKER_PID=""
}

cleanup() {
  stop_broker_graceful
  rm -rf "$WORK"
}
trap cleanup EXIT

pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; FAILED=$((FAILED + 1)); }
check_eq() { if [[ "$2" == "$3" ]]; then pass "$1"; else fail "$1 (actual=$2 expected=$3)"; fi; }

mkdir -p "$DATA"
start_broker
"$MK" topic create --topic durable --partitions 2 >/dev/null 2>&1
"$MK" topic create --topic durable_multi --partitions 2 >/dev/null 2>&1

# 3 批 × 10 条，逐条记录 ack 的 offset（R1：页缓存即 ack）。
ACKED="$WORK/acked.txt"
: >"$ACKED"
for batch in $(seq 1 3); do
  for i in $(seq 1 10); do
    printf '{"key":"k","value":"b%s-%s"}\n' "$batch" "$i"
  done | "$MK" produce --topic durable --partition 0 >>"$ACKED" 2>&1
done
ACK_COUNT="$(grep -c '"status":"ok"' "$ACKED")"
check_eq "3 批共 30 条已 ack" "$ACK_COUNT" "30"

# 多 Producer 并发（V1）：3 个进程同时写，两个撞同一分区、一个写另一分区。
MULTI_PIDS=()
MULTI_OUTS=()
for n in 0 1 2; do
  part=$((n % 2))
  out="$WORK/multi-${n}.out"
  MULTI_OUTS+=("$out")
  (
    for i in $(seq 1 10); do
      printf '{"key":"m","value":"multi-%s-%s"}\n' "$n" "$i"
    done
  ) | "$MK" produce --topic durable_multi --partition "$part" >"$out" 2>&1 &
  MULTI_PIDS+=("$!")
done
MULTI_FAILED=0
for pid in "${MULTI_PIDS[@]}"; do
  if ! wait "$pid"; then
    MULTI_FAILED=$((MULTI_FAILED + 1))
  fi
done
check_eq "并发 Producer 全部成功退出" "$MULTI_FAILED" "0"
MULTI_ACKED=0
for out in "${MULTI_OUTS[@]}"; do
  MULTI_ACKED=$((MULTI_ACKED + $(grep -c '"status":"ok"' "$out" || true)))
done
check_eq "并发 Producer 共 30 条已 ack" "$MULTI_ACKED" "30"

# --- kill -9：不经过任何清理路径 ---
kill -9 "$BROKER_PID"
wait "$BROKER_PID" 2>/dev/null || true
BROKER_PID=""
pass "Broker 已被 kill -9"

# V1/R7：注入未 ack 的残缺尾部（模拟崩溃时半条记录），重启必须截断。
LOG="$DATA/topics/durable/0/00000000000000000000.log"
SIZE_VALID="$(stat -c %s "$LOG")"
printf 'PARTIAL-TAIL-DATA' >>"$LOG"
SIZE_CORRUPT="$(stat -c %s "$LOG")"
if [[ "$SIZE_CORRUPT" -gt "$SIZE_VALID" ]]; then
  pass "已注入未 ack 残缺尾部"
else
  fail "残缺尾部注入失败"
fi

# --- 直接重启，无需人工干预 ---
start_broker
check_eq "重启后 ready" "$(grep -c '"ready"' "$WORK/broker.log")" "1"
check_eq "残缺尾部被截断到最后一条有效记录（R7）" \
  "$(stat -c %s "$LOG")" "$SIZE_VALID"

# 已 ack 消息必须全部可读，且 offset 连续 0..29。
"$MK" replay --topic durable --partition 0 >"$WORK/replay.out" 2>&1
check_eq "重启后可读 30 条" "$(grep -c '"topic":"durable"' "$WORK/replay.out")" "30"
OFFSETS="$(sed 's/.*"offset":\([0-9]*\).*/\1/' "$WORK/replay.out" | tr '\n' ' ')"
EXPECTED="$(seq 0 29 | tr '\n' ' ')"
check_eq "offset 连续无空洞（R27）" "$OFFSETS" "$EXPECTED"

# ack offset 与重启后读到的 offset 集合一致（无丢失、无错位）。
sed 's/.*"offset":\([0-9]*\).*/\1/' "$ACKED" | sort -n >"$WORK/acked.offsets"
sed 's/.*"offset":\([0-9]*\).*/\1/' "$WORK/replay.out" | sort -n >"$WORK/replay.offsets"
if diff -q "$WORK/acked.offsets" "$WORK/replay.offsets" >/dev/null; then
  pass "已 ack 的 offset 与读回一致"
else
  fail "已 ack 的 offset 与读回不一致"
fi

# 多 Producer 并发写入的 ack 也必须全部可读（partition 0=20 条，partition 1=10 条）。
"$MK" replay --topic durable_multi --partition 0 >"$WORK/multi-p0.out" 2>&1
"$MK" replay --topic durable_multi --partition 1 >"$WORK/multi-p1.out" 2>&1
check_eq "并发写入 partition 0 可读 20 条" \
  "$(grep -c '"topic":"durable_multi"' "$WORK/multi-p0.out" || true)" "20"
check_eq "并发写入 partition 1 可读 10 条" \
  "$(grep -c '"topic":"durable_multi"' "$WORK/multi-p1.out" || true)" "10"
MULTI_EXPECTED="$(seq 0 19 | tr '\n' ' ')"
MULTI_ACTUAL="$(sed 's/.*"offset":\([0-9]*\).*/\1/' "$WORK/multi-p0.out" | tr '\n' ' ')"
check_eq "并发写入 partition 0 offset 连续" "$MULTI_ACTUAL" "$MULTI_EXPECTED"
MULTI_EXPECTED="$(seq 0 9 | tr '\n' ' ')"
MULTI_ACTUAL="$(sed 's/.*"offset":\([0-9]*\).*/\1/' "$WORK/multi-p1.out" | tr '\n' ' ')"
check_eq "并发写入 partition 1 offset 连续" "$MULTI_ACTUAL" "$MULTI_EXPECTED"

# 重启后可继续写入，offset 从 30 接续（R27：已写消息 offset 不因重启改变）。
printf '{"key":"k","value":"after-restart"}\n' \
  | "$MK" produce --topic durable --partition 0 >"$WORK/next.jsonl" 2>&1
grep -q '"offset":30' "$WORK/next.jsonl" \
  && pass "重启后新写入 offset=30" || fail "重启后 offset 未接续"

# 消费侧同样能读到全部 31 条（R6/R11）。
"$MK" consume --topic durable --group g --consumer-index 0 --consumer-count 1 \
  --auto-commit-interval 1s --exit-at-end >"$WORK/consume.out" 2>"$WORK/consume.err"
check_eq "消费者读到 31 条" "$(grep -c '"topic":"durable"' "$WORK/consume.out")" "31"

stop_broker_graceful

echo
if [[ "$FAILED" -eq 0 ]]; then
  echo "v1: 全部通过"
  exit 0
fi
echo "v1: $FAILED 项失败"
exit 1
