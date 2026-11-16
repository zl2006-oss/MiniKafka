#!/usr/bin/env bash
# R28/R30 集成级损坏恢复：
#   - R30：单个 Topic meta 损坏 → 跳过该 Topic、stderr 告警、Broker 仍启动并服务其他 Topic；
#   - R28：位点双槽均损坏 → 回退为“无已提交位点”，Broker/消费者仍可用，不阻塞启动。
set -euo pipefail

cd "$(dirname "$0")/../.."
MK="${MK:-./build/minikafka}"
PORT="${PORT:-9092}"
WORK="$(mktemp -d "/tmp/mk-corrupt.XXXXXX")"
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
start_broker
"$MK" topic create --topic healthy --partitions 1 >/dev/null 2>&1
"$MK" topic create --topic broken --partitions 1 >/dev/null 2>&1
for i in 1 2 3; do
  printf '{"value":"h%s"}\n' "$i"
done | "$MK" produce --topic healthy --partition 0 >/dev/null 2>&1
printf '{"value":"b1"}\n' | "$MK" produce --topic broken --partition 0 >/dev/null 2>&1
"$MK" consume --topic healthy --group gcorrupt --consumer-index 0 \
  --consumer-count 1 --auto-commit-interval 1s --exit-at-end \
  >/dev/null 2>&1
stop_broker

# --- 注入损坏：broken 的 meta + gcorrupt/healthy 的双槽位点 ---
printf 'CORRUPT-META' >"$DATA/meta/broken.meta"
mkdir -p "$DATA/groups/gcorrupt/healthy"
printf 'BAD-SLOT-A' >"$DATA/groups/gcorrupt/healthy/offsets.a"
printf 'BAD-SLOT-B' >"$DATA/groups/gcorrupt/healthy/offsets.b"

# --- R30：Broker 必须仍能启动，healthy 可服务，broken 被跳过并告警 ---
start_broker
check_eq "meta 损坏后 Broker 仍 ready（R30）" \
  "$(grep -c '"ready"' "$WORK/broker.log")" "1"
grep -q '"event":"warning"' "$WORK/broker.log" \
  && pass "启动期输出 warning（D-03）" || fail "缺少启动告警"
grep -q "skip topic 'broken'" "$WORK/broker.log" \
  && pass "告警指明跳过的 Topic" || fail "告警未指明 broken"
"$MK" topic list >"$WORK/list.out" 2>&1
grep -q '"topic":"healthy"' "$WORK/list.out" \
  && pass "healthy 仍在 Topic 列表" || fail "healthy 丢失"
if grep -q '"topic":"broken"' "$WORK/list.out"; then
  fail "损坏 Topic 仍出现在列表"
else
  pass "broken 已跳过"
fi

# --- R28：双槽损坏 → 无已提交位点，从 earliest 重读且 Broker/消费者可用 ---
"$MK" consume --topic healthy --group gcorrupt --consumer-index 0 \
  --consumer-count 1 --auto-commit-interval 1s --exit-at-end \
  >"$WORK/reconsume.out" 2>"$WORK/reconsume.err"
check_eq "位点回退后从 earliest 重读 3 条（R28）" \
  "$(grep -c '"topic":"healthy"' "$WORK/reconsume.out" || true)" "3"
grep -q 'offset file degraded' "$WORK/broker.log" \
  && pass "位点降级告警已输出" || fail "缺少位点降级告警"

# healthy 仍可写；新消费者可读回全部 4 条。
printf '{"value":"h4"}\n' | "$MK" produce --topic healthy --partition 0 \
  >"$WORK/produce.out" 2>&1
grep -q '"status":"ok"' "$WORK/produce.out" \
  && pass "恢复后 healthy 仍可写" || fail "healthy 写入失败"
"$MK" consume --topic healthy --group gverify --consumer-index 0 \
  --consumer-count 1 --auto-commit-interval 1s --exit-at-end \
  >"$WORK/verify.out" 2>&1
check_eq "新 group 读到 4 条" \
  "$(grep -c '"topic":"healthy"' "$WORK/verify.out" || true)" "4"

stop_broker

echo
if [[ "$FAILED" -eq 0 ]]; then
  echo "corruption: 全部通过"
  exit 0
fi
echo "corruption: $FAILED 项失败"
exit 1
