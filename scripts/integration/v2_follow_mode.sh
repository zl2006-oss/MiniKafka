#!/usr/bin/env bash
# V2 + R18 + R8：跟随模式（不带 --exit-at-end）阻塞等待新消息；
# 收到 SIGTERM 后正常退出并补一次提交；重启从已提交位点续读。
set -euo pipefail

cd "$(dirname "$0")/../.."
MK="${MK:-./build/minikafka}"
PORT="${PORT:-9092}"
WORK="$(mktemp -d "/tmp/mk-follow.XXXXXX")"
DATA="$WORK/data"
BROKER_PID=""
CONSUMER_PID=""
FAILED=0

cleanup() {
  if [[ -n "$CONSUMER_PID" ]] && kill -0 "$CONSUMER_PID" 2>/dev/null; then
    kill -TERM "$CONSUMER_PID" 2>/dev/null || true
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
"$MK" topic create --topic live --partitions 1 >/dev/null 2>&1

# 已存在的历史消息：--from latest 时应被跳过。
printf '{"value":"old-1"}\n{"value":"old-2"}\n' | "$MK" produce --topic live >/dev/null 2>&1

# --- 跟随模式：不带 --exit-at-end，进程不应退出 ---
"$MK" consume --topic live --group glive --consumer-index 0 --consumer-count 1 \
  --from latest --auto-commit-interval 1s >"$WORK/follow.out" 2>"$WORK/follow.err" &
CONSUMER_PID=$!
for _ in $(seq 1 50); do
  grep -q '"event":"assigned"' "$WORK/follow.err" 2>/dev/null && break
  sleep 0.1
done
grep -q '"event":"assigned"' "$WORK/follow.err" \
  && pass "消费者已加入并等待新消息" || fail "消费者未就绪"
check_eq "--from latest 跳过历史消息" "$(grep -c '"topic":"live"' "$WORK/follow.out" || true)" "0"

if kill -0 "$CONSUMER_PID" 2>/dev/null; then
  pass "到达末尾后继续跟随（R18）"
else
  fail "到达末尾后进程意外退出"
fi

# 新消息应被立即消费。
printf '{"value":"new-1"}\n{"value":"new-2"}\n' | "$MK" produce --topic live >/dev/null 2>&1
for _ in $(seq 1 50); do
  COUNT="$(grep -c '"topic":"live"' "$WORK/follow.out" || true)"
  [[ "${COUNT:-0}" -ge 2 ]] && break
  sleep 0.1
done
check_eq "跟随即到新消息" "$(grep -c '"topic":"live"' "$WORK/follow.out" || true)" "2"

# --- SIGTERM：正常退出 + 补一次提交（03 §3.4）---
kill -TERM "$CONSUMER_PID"
set +e
wait "$CONSUMER_PID"
RC=$?
set -e
CONSUMER_PID=""
check_eq "SIGTERM 后退出码 0" "$RC" "0"
grep -q '"event":"consumer_stopped"' "$WORK/follow.err" \
  && pass "输出 consumer_stopped" || fail "缺少退出事件"

# 重启（--exit-at-end）：位点已提交，不应重复消费。
"$MK" consume --topic live --group glive --consumer-index 0 --consumer-count 1 \
  --auto-commit-interval 1s --exit-at-end >"$WORK/again.out" 2>"$WORK/again.err"
check_eq "退出前已提交位点，重启无重复（R6）" \
  "$(grep -c '"topic":"live"' "$WORK/again.out" || true)" "0"
grep -q '"start_offset":4' "$WORK/again.err" \
  && pass "起点为 4（new-1/new-2 已提交）" || fail "起点不符"

kill -TERM "$BROKER_PID"
wait "$BROKER_PID" 2>/dev/null || true
BROKER_PID=""

echo
if [[ "$FAILED" -eq 0 ]]; then
  echo "follow: 全部通过"
  exit 0
fi
echo "follow: $FAILED 项失败"
exit 1
