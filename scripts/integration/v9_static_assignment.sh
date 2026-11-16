#!/usr/bin/env bash
# V9：静态分配（Q-P2=A）—— partition % consumer_count == consumer_index，
# 多消费者不重叠、全覆盖；非法槽位拒绝。
set -euo pipefail

cd "$(dirname "$0")/../.."
MK="${MK:-./build/minikafka}"
PORT="${PORT:-9092}"
WORK="$(mktemp -d "/tmp/mk-v9.XXXXXX")"
DATA="$WORK/data"
BLOG="$WORK/broker.log"
BROKER_PID=""
CONSUMER0_PID=""
CONSUMER1_PID=""
FAILED=0

stop_consumer() { # <pid-var>
  local pid="${!1:-}"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    kill -TERM "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  fi
  printf -v "$1" '%s' ""
}

cleanup() {
  stop_consumer CONSUMER0_PID
  stop_consumer CONSUMER1_PID
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
"$MK" broker --port "$PORT" --data-dir "$DATA" >"$BLOG" 2>&1 &
BROKER_PID=$!
for _ in $(seq 1 100); do
  grep -q '"ready"' "$BLOG" && break
  sleep 0.1
done

"$MK" topic create --topic chat --partitions 4 >/dev/null 2>&1

# 每个分区 10 条，便于核对各消费者拿到的总量。
for p in 0 1 2 3; do
  for i in $(seq 1 10); do
    printf '{"key":"k%s-%s","value":"v%s"}\n' "$p" "$i" "$i"
  done | "$MK" produce --topic chat --partition "$p" >/dev/null 2>&1
done

# --- 两个消费者（count=2）：各自负责 2 个分区 ---
"$MK" consume --topic chat --group room --consumer-index 0 --consumer-count 2 \
  --auto-commit-interval 1s --exit-at-end >"$WORK/c0.out" 2>"$WORK/c0.err"
"$MK" consume --topic chat --group room --consumer-index 1 --consumer-count 2 \
  --auto-commit-interval 1s --exit-at-end >"$WORK/c1.out" 2>"$WORK/c1.err"

grep -o '"partition":[0-9]*' "$WORK/c0.err" | sort -u >"$WORK/c0.parts"
grep -o '"partition":[0-9]*' "$WORK/c1.err" | sort -u >"$WORK/c1.parts"

check_eq "consumer 0 分配 {0,2}" "$(tr '\n' ' ' <"$WORK/c0.parts")" '"partition":0 "partition":2 '
check_eq "consumer 1 分配 {1,3}" "$(tr '\n' ' ' <"$WORK/c1.parts")" '"partition":1 "partition":3 '

set +e
grep -c '"topic":"chat"' "$WORK/c0.out" >"$WORK/c0.count" 2>/dev/null
grep -c '"topic":"chat"' "$WORK/c1.out" >"$WORK/c1.count" 2>/dev/null
set -e
check_eq "consumer 0 读到 20 条" "$(cat "$WORK/c0.count")" "20"
check_eq "consumer 1 读到 20 条" "$(cat "$WORK/c1.count")" "20"

# 不重叠：两个消费者的分区集合交集为空。
if [[ -z "$(comm -12 "$WORK/c0.parts" "$WORK/c1.parts")" ]]; then
  pass "分配不重叠"
else
  fail "分配存在重叠"
fi

# 合法槽位边界：index=0/count=4 只拿分区 0（不报错）。
"$MK" consume --topic chat --group room4 --consumer-index 0 --consumer-count 4 \
  --auto-commit-interval 1s --exit-at-end >"$WORK/c4.out" 2>"$WORK/c4.err"
grep -o '"partition":[0-9]*' "$WORK/c4.err" | sort -u >"$WORK/c4.parts"
check_eq "count=4 时 index=0 只拿分区 0" "$(tr '\n' ' ' <"$WORK/c4.parts")" '"partition":0 '

# 非法槽位：index >= count。
set +e
"$MK" consume --topic chat --group room --consumer-index 2 --consumer-count 2 \
  --exit-at-end >"$WORK/bad.out" 2>&1
RC=$?
set -e
check_eq "index=count 退出码 3" "$RC" "3"
grep -q '"error":"invalid_consumer_slot"' "$WORK/bad.out" \
  && pass "index=count 返回 invalid_consumer_slot" \
  || fail "index=count 错误码不符"

# --- V9/R10（MVP）：失效消费者的分区暂停，存活消费者不接管 ---
"$MK" topic create --topic pause --partitions 4 >/dev/null 2>&1
"$MK" consume --topic pause --group pauseg --consumer-index 0 --consumer-count 2 \
  --auto-commit-interval 1s >"$WORK/pause0.out" 2>"$WORK/pause0.err" &
CONSUMER0_PID=$!
"$MK" consume --topic pause --group pauseg --consumer-index 1 --consumer-count 2 \
  --auto-commit-interval 1s >"$WORK/pause1.out" 2>"$WORK/pause1.err" &
CONSUMER1_PID=$!
for _ in $(seq 1 50); do
  grep -q '"event":"assigned"' "$WORK/pause0.err" 2>/dev/null && \
    grep -q '"event":"assigned"' "$WORK/pause1.err" 2>/dev/null && break
  sleep 0.1
done
grep -q '"event":"assigned"' "$WORK/pause0.err" \
  && grep -q '"event":"assigned"' "$WORK/pause1.err" \
  && pass "两个消费者均已加入" || fail "消费者未就绪"

# kill -9 consumer 1：其负责的分区 1/3 应暂停，直到同槽位重新加入。
kill -9 "$CONSUMER1_PID" 2>/dev/null || true
wait "$CONSUMER1_PID" 2>/dev/null || true
CONSUMER1_PID=""
pass "consumer 1 已被 kill -9"

printf '%s\n' '{"value":"p1-a"}' '{"value":"p1-b"}' '{"value":"p1-c"}' \
  | "$MK" produce --topic pause --partition 1 >/dev/null 2>&1
printf '%s\n' '{"value":"p0-a"}' \
  | "$MK" produce --topic pause --partition 0 >/dev/null 2>&1
for _ in $(seq 1 50); do
  [[ "$(grep -c '"partition":0' "$WORK/pause0.out" || true)" -ge 1 ]] && break
  sleep 0.1
done
check_eq "存活消费者继续消费自己的分区（partition 0）" \
  "$(grep -c '"partition":0' "$WORK/pause0.out" || true)" "1"
check_eq "失效消费者的分区未被接管（consumer 0 未读 partition 1）" \
  "$(grep -c '"partition":1' "$WORK/pause0.out" || true)" "0"

# 同槽位重新加入：从暂停处（无已提交位点 → earliest）继续读 partition 1。
"$MK" consume --topic pause --group pauseg --consumer-index 1 --consumer-count 2 \
  --auto-commit-interval 1s --exit-at-end >"$WORK/pause1r.out" \
  2>"$WORK/pause1r.err"
check_eq "consumer 1 重新加入后读到暂停分区的 3 条" \
  "$(grep -c '"partition":1' "$WORK/pause1r.out" || true)" "3"

set +e
kill -TERM "$CONSUMER0_PID" 2>/dev/null
wait "$CONSUMER0_PID" 2>/dev/null
RC=$?
set -e
CONSUMER0_PID=""
check_eq "consumer 0 SIGTERM 退出码 0" "$RC" "0"

kill -TERM "$BROKER_PID"
wait "$BROKER_PID" 2>/dev/null || true
BROKER_PID=""

echo
if [[ "$FAILED" -eq 0 ]]; then
  echo "v9: 全部通过"
  exit 0
fi
echo "v9: $FAILED 项失败"
exit 1
