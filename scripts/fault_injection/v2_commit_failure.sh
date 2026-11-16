#!/usr/bin/env bash
# V2/R8：位点提交失败 → 退避重试 + 告警，消费者继续消费、不主动退出；
# 障碍解除后提交成功；SIGTERM 正常退出并补提交，重启不重复。
# 注入方式：在 Broker 的 data-dir 下把 `groups` 建成普通文件，使位点目录无法创建。
set -euo pipefail

cd "$(dirname "$0")/../.."
MK="${MK:-./build/minikafka}"
PORT="${PORT:-9092}"
WORK="$(mktemp -d "/tmp/mk-commit.XXXXXX")"
DATA="$WORK/data"
BROKER_PID=""
CONSUMER_PID=""
FAILED=0

stop_consumer() {
  if [[ -n "$CONSUMER_PID" ]] && kill -0 "$CONSUMER_PID" 2>/dev/null; then
    kill -TERM "$CONSUMER_PID" 2>/dev/null || true
    wait "$CONSUMER_PID" 2>/dev/null || true
  fi
  CONSUMER_PID=""
}

cleanup() {
  stop_consumer
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
"$MK" topic create --topic cfo --partitions 1 >/dev/null 2>&1
printf '{"value":"m1"}\n{"value":"m2"}\n' | "$MK" produce --topic cfo \
  --partition 0 >/dev/null 2>&1

# 注入提交失败：groups 必须是普通文件（Broker 启动时不创建它）。
printf 'blocked\n' >"$DATA/groups"

"$MK" consume --topic cfo --group gc --consumer-index 0 --consumer-count 1 \
  --auto-commit-interval 1s --retry-backoff-ms 10 \
  >"$WORK/consume.out" 2>"$WORK/consume.err" &
CONSUMER_PID=$!

for _ in $(seq 1 50); do
  [[ "$(grep -c '"topic":"cfo"' "$WORK/consume.out" || true)" -ge 2 ]] && break
  sleep 0.1
done
check_eq "提交失败时仍读到 2 条（R8）" \
  "$(grep -c '"topic":"cfo"' "$WORK/consume.out" || true)" "2"
sleep 2.5  # 跨过多个自动提交周期与 CommitWithRetry 的退避
if kill -0 "$CONSUMER_PID" 2>/dev/null; then
  pass "提交持续失败时消费者未退出（R8）"
else
  fail "消费者因提交失败退出"
fi
grep -q '"event":"commit_failed"' "$WORK/consume.err" \
  && pass "输出 commit_failed 告警（R8）" || fail "缺少 commit_failed 告警"
grep -q '"error":"storage_io_error"' "$WORK/consume.err" \
  && pass "告警含明确错误码" || fail "告警错误码不符"

# 解除障碍：下次自动提交应成功。
rm -f "$DATA/groups"
sleep 2.0
set +e
kill -TERM "$CONSUMER_PID" 2>/dev/null
wait "$CONSUMER_PID" 2>/dev/null
RC=$?
set -e
CONSUMER_PID=""
check_eq "障碍解除后 SIGTERM 正常退出（退出码 0）" "$RC" "0"
grep -q '"event":"consumer_stopped"' "$WORK/consume.err" \
  && pass "输出 consumer_stopped" || fail "缺少退出事件"

# 重启同 group：已提交到位点 2，不应重复消费。
"$MK" consume --topic cfo --group gc --consumer-index 0 --consumer-count 1 \
  --auto-commit-interval 1s --exit-at-end >"$WORK/again.out" 2>"$WORK/again.err"
check_eq "重启后从已提交位点续读（0 条）" \
  "$(grep -c '"topic":"cfo"' "$WORK/again.out" || true)" "0"
grep -q '"start_offset":2' "$WORK/again.err" \
  && pass "位点为 2" || fail "位点未推进到 2"

kill -TERM "$BROKER_PID"
wait "$BROKER_PID" 2>/dev/null || true
BROKER_PID=""

echo
if [[ "$FAILED" -eq 0 ]]; then
  echo "v2-commit-failure: 全部通过"
  exit 0
fi
echo "v2-commit-failure: $FAILED 项失败"
exit 1
