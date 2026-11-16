#!/usr/bin/env bash
# V4 / V5 压测：固定负载（payload 100B + key 8B，R26），测 Broker 总入口吞吐（R5）
# 与 Producer ack 延迟 P99（R4）。基准机为 4 vCPU / 8 GiB VMware Ubuntu（R9）。
# 未达标不隐藏：同时打印实测值与目标值。
set -euo pipefail

cd "$(dirname "$0")/../.."
MK="${MK:-./build/minikafka}"
PORT="${PORT:-9092}"
MESSAGES="${MESSAGES:-20000}"
PRODUCERS="${PRODUCERS:-4}"
BATCH="${BATCH:-100}"
LATENCY_MESSAGES="${LATENCY_MESSAGES:-1000}"
WORK="$(mktemp -d "/tmp/mk-bench.XXXXXX")"
DATA="$WORK/data"
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

mkdir -p "$DATA"
"$MK" broker --port "$PORT" --data-dir "$DATA" >"$WORK/broker.log" 2>&1 &
BROKER_PID=$!
for _ in $(seq 1 100); do
  grep -q '"ready"' "$WORK/broker.log" && break
  sleep 0.1
done
"$MK" topic create --topic bench1 --partitions 8 >/dev/null 2>&1

MEM_GB="$(free -g 2>/dev/null | awk '/^Mem:/{print $2}')"
echo "基准机：$(nproc) vCPU / ${MEM_GB:-?} GiB / $(uname -r)"
echo

echo "=== 吞吐（V4：目标 ≥ 100000 条/秒）==="
"$MK" bench --topic bench1 --messages "$MESSAGES" --producers "$PRODUCERS" \
  --batch "$BATCH" --payload-bytes 100 --key-bytes 8 >"$WORK/tp.json" 2>&1
cat "$WORK/tp.json"
TP="$(sed 's/.*"throughput_msg_per_sec":\([0-9.]*\).*/\1/' "$WORK/tp.json")"
TP_INT="${TP%%.*}"
if [[ "${TP_INT:-0}" -ge 100000 ]]; then
  pass "总入口吞吐 ${TP_INT} 条/秒 ≥ 100000"
else
  fail "总入口吞吐 ${TP_INT} 条/秒 < 100000（如实记录差距，见 README）"
fi

echo
echo "=== ack 延迟（V5：目标 P99 < 10ms）==="
"$MK" bench --topic bench1 --messages "$LATENCY_MESSAGES" --producers 1 \
  --mode latency >"$WORK/lat.json" 2>&1
cat "$WORK/lat.json"
P99="$(sed 's/.*"latency_p99_ms":\([0-9.]*\).*/\1/' "$WORK/lat.json")"
P99_INT="${P99%%.*}"
if [[ "${P99_INT:-999}" -lt 10 ]]; then
  pass "P99 = ${P99} ms < 10ms"
else
  fail "P99 = ${P99} ms ≥ 10ms"
fi

kill -TERM "$BROKER_PID"
wait "$BROKER_PID" 2>/dev/null || true
BROKER_PID=""

echo
if [[ "$FAILED" -eq 0 ]]; then
  echo "bench: 全部达标"
  exit 0
fi
echo "bench: $FAILED 项未达标"
exit 1
