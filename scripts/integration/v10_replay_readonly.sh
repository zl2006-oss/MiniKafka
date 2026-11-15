#!/usr/bin/env bash
# V10：replay 只读 —— 组位点已推进后执行 replay，组后续消费仍从原位点继续。
# 同时覆盖 V11：同分区 offset 严格递增、消费顺序与写入顺序一致。
set -euo pipefail

cd "$(dirname "$0")/../.."
MK="${MK:-./build/minikafka}"
PORT="${PORT:-9092}"
WORK="$(mktemp -d "/tmp/mk-v10.XXXXXX")"
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

"$MK" topic create --topic seq --partitions 2 >/dev/null 2>&1

# 同 key 连续 10 条（R2/R22）：同一分区、offset 递增、顺序一致。
for i in $(seq 0 9); do
  printf '{"key":"same","value":"v%s"}\n' "$i"
done | "$MK" produce --topic seq --partition 0 >"$WORK/p0.jsonl" 2>&1
for i in $(seq 0 9); do
  printf '{"key":"same","value":"w%s"}\n' "$i"
done | "$MK" produce --topic seq --partition 1 >/dev/null 2>&1

# --- V11：写入顺序 = 消费顺序，offset 严格递增 ---
"$MK" consume --topic seq --group gseq --consumer-index 0 --consumer-count 2 \
  --auto-commit-interval 1s --exit-at-end >"$WORK/c1.out" 2>"$WORK/c1.err"
grep '"topic":"seq"' "$WORK/c1.out" >"$WORK/c1.records"
check_eq "分区 0 顺序消费 10 条" "$(wc -l <"$WORK/c1.records" | tr -d ' ')" "10"

OFFSETS="$(sed 's/.*"offset":\([0-9]*\).*/\1/' "$WORK/c1.records" | tr '\n' ' ')"
check_eq "offset 从 0 连续递增" "$OFFSETS" "0 1 2 3 4 5 6 7 8 9 "

VALUES="$(sed 's/.*"value":"v\([0-9]*\)".*/\1/' "$WORK/c1.records" | tr '\n' ' ')"
check_eq "消费顺序与写入顺序一致" "$VALUES" "0 1 2 3 4 5 6 7 8 9 "

# --- V10：位点已推进到 10，replay 后仍为 10 ---
"$MK" replay --topic seq --partition 0 --offset 0 >"$WORK/r.out" 2>&1
check_eq "replay 分区 0 读到 10 条" "$(grep -c '"topic":"seq"' "$WORK/r.out")" "10"

"$MK" consume --topic seq --group gseq --consumer-index 0 --consumer-count 2 \
  --auto-commit-interval 1s --exit-at-end >"$WORK/c2.out" 2>"$WORK/c2.err"
set +e
grep -c '"topic":"seq"' "$WORK/c2.out" >"$WORK/c2.count" 2>/dev/null || echo 0 >"$WORK/c2.count"
set -e
check_eq "replay 后组位点未回退（0 条）" "$(cat "$WORK/c2.count")" "0"
grep -q '"partition":0,"start_offset":10' "$WORK/c2.err" \
  && pass "分区 0 起点仍为 10" || fail "分区 0 起点被 replay 改变"

kill -TERM "$BROKER_PID"
wait "$BROKER_PID" 2>/dev/null || true
BROKER_PID=""

echo
if [[ "$FAILED" -eq 0 ]]; then
  echo "v10: 全部通过"
  exit 0
fi
echo "v10: $FAILED 项失败"
exit 1
