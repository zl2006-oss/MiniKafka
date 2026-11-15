#!/usr/bin/env bash
# V2：消费者 kill -9 后以同 group/topic 重启 —— 从最后已提交位点继续，
# 不丢消息，重复不超过一个自动提交间隔内已处理的消息（R6/R11/R28）。
set -euo pipefail

cd "$(dirname "$0")/../.."
MK="${MK:-./build/minikafka}"
PORT="${PORT:-9092}"
WORK="$(mktemp -d "/tmp/mk-v2.XXXXXX")"
DATA="$WORK/data"
BROKER_PID=""
CONSUMER_PID=""
FAILED=0

cleanup() {
  if [[ -n "$CONSUMER_PID" ]] && kill -0 "$CONSUMER_PID" 2>/dev/null; then
    kill -9 "$CONSUMER_PID" 2>/dev/null || true
    wait "$CONSUMER_PID" 2>/dev/null || true
  fi
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
"$MK" topic create --topic resume --partitions 1 >/dev/null 2>&1

TOTAL=6
for i in $(seq 1 "$TOTAL"); do
  printf '{"key":"k","value":"m%s"}\n' "$i"
done | "$MK" produce --topic resume >/dev/null 2>&1

# --- 消费并等待一次自动提交，然后 kill -9 ---
"$MK" consume --topic resume --group gresume --consumer-index 0 --consumer-count 1 \
  --auto-commit-interval 1s >"$WORK/first.out" 2>"$WORK/first.err" &
CONSUMER_PID=$!
for _ in $(seq 1 50); do
  [[ "$(grep -c '"topic":"resume"' "$WORK/first.out" || true)" -ge "$TOTAL" ]] && break
  sleep 0.1
done
check_eq "首次消费读到全部 6 条" "$(grep -c '"topic":"resume"' "$WORK/first.out")" "6"
sleep 1.5  # 跨过一个自动提交间隔，确保位点已提交
kill -9 "$CONSUMER_PID"
wait "$CONSUMER_PID" 2>/dev/null || true
CONSUMER_PID=""
pass "消费者已被 kill -9"

# --- 以同 group/topic 重启：从已提交位点继续 ---
"$MK" consume --topic resume --group gresume --consumer-index 0 --consumer-count 1 \
  --auto-commit-interval 1s --exit-at-end >"$WORK/second.out" 2>"$WORK/second.err"
START="$(sed -n 's/.*"start_offset":\([0-9]*\).*/\1/p' "$WORK/second.err" | head -1)"
REPLAYED="$(grep -c '"topic":"resume"' "$WORK/second.out" || true)"

grep -q '"resumed":true' "$WORK/second.err" \
  && pass "重启后从已提交位点恢复（resumed=true）" || fail "未从已提交位点恢复"
# 不丢消息：起点 + 重读条数 = 总条数。
check_eq "不丢消息（start_offset + 重读 = 6）" \
  "$(( ${START:-0} + ${REPLAYED:-0} ))" "$TOTAL"
# 重复不超过一个自动提交间隔：最多重读 1 条。
if [[ "${REPLAYED:-0}" -le 1 ]]; then
  pass "重复不超过一个提交间隔（重读 ${REPLAYED} 条）"
else
  fail "重复过多（重读 ${REPLAYED} 条）"
fi

# 再次重启：位点已推进到 6，不应再有重复。
"$MK" consume --topic resume --group gresume --consumer-index 0 --consumer-count 1 \
  --auto-commit-interval 1s --exit-at-end >"$WORK/third.out" 2>"$WORK/third.err"
check_eq "第三次启动无重复" "$(grep -c '"topic":"resume"' "$WORK/third.out" || true)" "0"
grep -q '"start_offset":6' "$WORK/third.err" \
  && pass "位点已推进到 6" || fail "位点未推进到 6"

kill -TERM "$BROKER_PID"
wait "$BROKER_PID" 2>/dev/null || true
BROKER_PID=""

echo
if [[ "$FAILED" -eq 0 ]]; then
  echo "v2: 全部通过"
  exit 0
fi
echo "v2: $FAILED 项失败"
exit 1
