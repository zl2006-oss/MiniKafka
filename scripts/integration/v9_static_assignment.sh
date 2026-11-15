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
