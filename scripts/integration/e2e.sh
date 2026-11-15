#!/usr/bin/env bash
# V2 / V10 / V11 / V9 之外的端到端基础链路：
# broker 启动 → topic create/list → produce → consume（位点续读）→ replay → 错误路径 → 优雅退出。
# 注意：03 §3.4 规定 produce/consume/replay 固定连 127.0.0.1:9092，因此本脚本占用 9092。
set -euo pipefail

cd "$(dirname "$0")/../.."
MK="${MK:-./build/minikafka}"
PORT="${PORT:-9092}"
WORK="$(mktemp -d "/tmp/mk-e2e.XXXXXX")"
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

check_eq() { # <name> <actual> <expected>
  if [[ "$2" == "$3" ]]; then pass "$1"; else fail "$1 (actual=$2 expected=$3)"; fi
}
check_contains() { # <name> <file> <pattern>
  if grep -q -- "$3" "$2"; then pass "$1"; else fail "$1 (missing: $3)"; fi
}

if ! command -v "$MK" >/dev/null 2>&1 && [[ ! -x "$MK" ]]; then
  echo "missing binary: $MK (先运行 scripts/build.sh)" >&2
  exit 1
fi
if ss -ltn 2>/dev/null | grep -q ":$PORT "; then
  echo "port $PORT 已被占用：03 §3.4 固定默认 broker 地址，请先释放" >&2
  exit 1
fi

mkdir -p "$DATA"
"$MK" broker --port "$PORT" --data-dir "$DATA" >"$BLOG" 2>&1 &
BROKER_PID=$!
for _ in $(seq 1 100); do
  grep -q '"ready"' "$BLOG" && break
  sleep 0.1
done
check_contains "broker 就绪（Q-P10）" "$BLOG" '"ready"'

# --- R14：Topic 创建与列举 ---
"$MK" topic create --topic orders --partitions 4 >"$WORK/create.json" 2>&1
check_contains "topic create 返回 created" "$WORK/create.json" '"status":"created"'
"$MK" topic list >"$WORK/list.json" 2>&1
check_contains "topic list 含 orders" "$WORK/list.json" '"topic":"orders"'
check_contains "topic list 含分区列表" "$WORK/list.json" '"partition_ids":\[0,1,2,3\]'

# --- R1/R2/R22/R23：投递逐条结果 ---
# 全部显式落在分区 0，使后续 replay 断言可确定复现。
printf '%s\n' \
  '{"key":"k1","value":"m0"}' \
  '{"key":"k1","value":"m1"}' \
  '{"key":"k1","value":"m2"}' \
  '{"value":"m3"}' \
  '{"value":"m4"}' \
  | "$MK" produce --topic orders --partition 0 >"$WORK/produce.jsonl" 2>&1
set +e
grep -c '"status":"ok"' "$WORK/produce.jsonl" >"$WORK/ok.count" 2>/dev/null
set -e
check_eq "produce 全部成功（5 条）" "$(cat "$WORK/ok.count")" "5"
check_contains "produce 输出 offset" "$WORK/produce.jsonl" '"offset":0'

# --- R18/R19：消费到当前末尾 ---
"$MK" consume --topic orders --group g1 --consumer-index 0 --consumer-count 1 \
  --auto-commit-interval 1s --exit-at-end >"$WORK/consume1.out" 2>"$WORK/consume1.err"
set +e
grep -c '"topic":"orders"' "$WORK/consume1.out" >"$WORK/c1.count" 2>/dev/null
set -e
check_eq "consume 首次读到 5 条" "$(cat "$WORK/c1.count")" "5"
check_contains "consume 输出含 key/value（R146）" "$WORK/consume1.out" '"key":"k1","value":"m0"'

# --- R6/R11：位点续读，第二次消费不再重复 ---
"$MK" consume --topic orders --group g1 --consumer-index 0 --consumer-count 1 \
  --auto-commit-interval 1s --exit-at-end >"$WORK/consume2.out" 2>"$WORK/consume2.err"
set +e
grep -c '"topic":"orders"' "$WORK/consume2.out" >"$WORK/c2.count" 2>/dev/null || echo 0 >"$WORK/c2.count"
set -e
check_eq "consume 第二次从已提交位点续读（0 条）" "$(cat "$WORK/c2.count")" "0"
check_contains "第二次消费标记 resumed=true" "$WORK/consume2.err" '"resumed":true'

# --- R12/V10：replay 只读，不改变组位点 ---
"$MK" replay --topic orders --partition 0 --offset 0 >"$WORK/replay.out" 2>&1
set +e
grep -c '"topic":"orders"' "$WORK/replay.out" >"$WORK/r.count" 2>/dev/null
set -e
check_eq "replay 分区 0 读到 5 条" "$(cat "$WORK/r.count")" "5"
"$MK" consume --topic orders --group g1 --consumer-index 0 --consumer-count 1 \
  --auto-commit-interval 1s --exit-at-end >"$WORK/consume3.out" 2>/dev/null
set +e
grep -c '"topic":"orders"' "$WORK/consume3.out" >"$WORK/c3.count" 2>/dev/null || echo 0 >"$WORK/c3.count"
set -e
check_eq "replay 后组位点未回退（0 条）" "$(cat "$WORK/c3.count")" "0"

# --- R22：未指定分区时的选择策略（key 哈希 / 轮询）---
printf '{"value":"r1"}\n{"value":"r2"}\n' \
  | "$MK" produce --topic orders >"$WORK/rr.jsonl" 2>&1
RR_PARTS="$(sed 's/.*"partition":\([0-9]*\).*/\1/' "$WORK/rr.jsonl" | tr '\n' ' ')"
check_eq "无 key 时轮询到不同分区" "$RR_PARTS" "0 1 "
printf '{"key":"stable","value":"a"}\n{"key":"stable","value":"b"}\n' \
  | "$MK" produce --topic orders >"$WORK/hash.jsonl" 2>&1
HASH_PARTS="$(sed 's/.*"partition":\([0-9]*\).*/\1/' "$WORK/hash.jsonl" | sort -u | tr -d '\n')"
check_eq "同 key 落在同一分区" "$(wc -l <"$WORK/hash.jsonl" | tr -d ' ')" "2"
if [[ -z "$(printf '%s\n' "$HASH_PARTS" | tr -d '0-9')" ]]; then
  pass "同 key 分区一致（partition=$HASH_PARTS）"
else
  fail "同 key 落在多个分区"
fi

# --- 错误路径与退出码（03 §3.4）---
set +e
"$MK" produce --topic missing --message '{"value":"x"}' >"$WORK/e1.out" 2>&1
RC=$?
set -e
check_eq "未知 topic 退出码 3" "$RC" "3"
check_contains "未知 topic 错误码" "$WORK/e1.out" '"error":"topic_not_found"'

set +e
"$MK" produce --topic orders --partition 9 --message '{"value":"x"}' >"$WORK/e2.out" 2>&1
RC=$?
set -e
check_eq "越界分区退出码 3" "$RC" "3"
check_contains "越界分区错误码" "$WORK/e2.out" '"error":"invalid_partition"'

set +e
"$MK" consume --topic orders --group g1 --consumer-index 2 --consumer-count 2 \
  --exit-at-end >"$WORK/e3.out" 2>&1
RC=$?
set -e
check_eq "非法槽位退出码 3" "$RC" "3"
check_contains "非法槽位错误码" "$WORK/e3.out" '"error":"invalid_consumer_slot"'

set +e
"$MK" consume --topic orders --group g1 --consumer-index 0 --consumer-count 1 \
  --from nope --exit-at-end >"$WORK/e4.out" 2>&1
RC=$?
set -e
check_eq "非法 --from 退出码 2" "$RC" "2"
check_contains "非法 --from 错误码" "$WORK/e4.out" '"error":"invalid_from"'

set +e
"$MK" replay --topic orders --offset 999999 >"$WORK/e5.out" 2>&1
RC=$?
set -e
check_eq "replay 越界位点退出码 3" "$RC" "3"
check_contains "replay 越界位点错误码" "$WORK/e5.out" '"error":"offset_out_of_range"'

# --- 优雅退出 ---
kill -TERM "$BROKER_PID"
wait "$BROKER_PID" 2>/dev/null || true
BROKER_PID=""
if [[ -f "$DATA/topics/orders/0/00000000000000000000.log" ]]; then
  pass "分区日志已落盘"
else
  fail "分区日志缺失"
fi

echo
if [[ "$FAILED" -eq 0 ]]; then
  echo "e2e: 全部通过"
  exit 0
fi
echo "e2e: $FAILED 项失败"
exit 1
