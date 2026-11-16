#!/usr/bin/env bash
# R24 / D-04 背压重试：Broker 队列满时以"请求级 broker_unavailable"拒绝整个请求
# （results 为空、status=2）。Producer 必须按 R3 重试，而不是立刻放弃。
#
# 真实 Broker 的队列满难以稳定复现，因此这里用一个只实现必要报文的假 Broker：
#   - ListTopics(0x0002) → 1 个 topic、1 个分区
#   - Produce(0x0003)    → 恒返回 broker_unavailable（请求级），并记录收到几次
# 断言：重试次数 = --retries、逐条结果为 broker_unavailable（不是 partition_blocked）、
#       退出码 4、汇总 exit_reason=retry_exhausted。
set -euo pipefail

cd "$(dirname "$0")/../.."
MK="${MK:-./build/minikafka}"
PORT="${PORT:-9092}"
WORK="$(mktemp -d "/tmp/mk-bp.XXXXXX")"
FAKE_LOG="$WORK/fake.log"
FAKE_PID=""
FAILED=0

cleanup() {
  if [[ -n "$FAKE_PID" ]] && kill -0 "$FAKE_PID" 2>/dev/null; then
    kill -TERM "$FAKE_PID" 2>/dev/null || true
    sleep 0.2
    kill -9 "$FAKE_PID" 2>/dev/null || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

pass() { printf 'PASS  %s\n' "$1"; }
fail() { printf 'FAIL  %s\n' "$1"; FAILED=$((FAILED + 1)); }
check_eq() { if [[ "$2" == "$3" ]]; then pass "$1"; else fail "$1 (actual=$2 expected=$3)"; fi; }

cat >"$WORK/fake_broker.py" <<'PY'
import socket
import struct
import sys
import zlib

MAGIC = 0x4D4B5031
VERSION = 1
MASK = 0x8000
BROKER_UNAVAILABLE = 0x0601
PRODUCE = 0x0003
LIST_TOPICS = 0x0002


def u16(v):
    return struct.pack(">H", v)


def u32(v):
    return struct.pack(">I", v)


def u64(v):
    return struct.pack(">Q", v)


def blob(text):
    raw = text.encode("utf-8")
    return u32(len(raw)) + raw


def frame(msg_type, request_id, payload):
    body = u32(MAGIC) + u16(VERSION) + u16(msg_type) + u64(request_id) + u32(len(payload)) + payload
    return body + u32(zlib.crc32(body) & 0xFFFFFFFF)


def recv_exact(conn, size):
    chunks = []
    got = 0
    while got < size:
        piece = conn.recv(size - got)
        if not piece:
            return None
        chunks.append(piece)
        got += len(piece)
    return b"".join(chunks)


def main():
    port = int(sys.argv[1])
    produce_count = 0
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", port))
    server.listen(8)
    print("LISTENING", flush=True)
    while True:
        conn, _ = server.accept()
        conn.settimeout(10)
        try:
            while True:
                header = recv_exact(conn, 20)
                if header is None:
                    break
                magic, version, msg_type, request_id, payload_len = struct.unpack(
                    ">IHHQI", header)
                if magic != MAGIC or version != VERSION:
                    break
                payload = recv_exact(conn, payload_len) if payload_len else b""
                crc_bytes = recv_exact(conn, 4)
                if payload is None or crc_bytes is None:
                    break
                expected = struct.unpack(">I", crc_bytes)[0]
                if zlib.crc32(header + payload) & 0xFFFFFFFF != expected:
                    break

                if msg_type == LIST_TOPICS:
                    out = u16(0) + u32(1) + blob("rt") + u32(1)
                elif msg_type == PRODUCE:
                    produce_count += 1
                    print("PRODUCE %d" % produce_count, flush=True)
                    # 请求级错误：status=2（无条目追加）、results 为空。
                    out = (u16(BROKER_UNAVAILABLE) +
                           blob("storage worker queue is full") +
                           struct.pack(">B", 2) + u32(0))
                else:
                    break
                conn.sendall(frame(msg_type | MASK, request_id, out))
        except (socket.timeout, ConnectionResetError):
            pass
        finally:
            conn.close()


main()
PY

if ss -ltn 2>/dev/null | grep -q ":$PORT "; then
  echo "port $PORT 已被占用（03 §3.4 固定默认 broker 地址）" >&2
  exit 1
fi

python3 "$WORK/fake_broker.py" "$PORT" >"$FAKE_LOG" 2>&1 &
FAKE_PID=$!
for _ in $(seq 1 100); do
  grep -q "LISTENING" "$FAKE_LOG" 2>/dev/null && break
  sleep 0.1
done
grep -q "LISTENING" "$FAKE_LOG" || { echo "假 Broker 未启动" >&2; exit 1; }

printf '%s\n' '{"value":"a"}' '{"value":"b"}' >"$WORK/input.jsonl"
set +e
"$MK" produce --topic rt --file "$WORK/input.jsonl" \
  --retries 3 --retry-backoff-ms 10 --retry-timeout-ms 2000 \
  >"$WORK/out.jsonl" 2>"$WORK/err.jsonl"
RC=$?
set -e

check_eq "请求级 broker_unavailable 重试耗尽后退出码 4" "$RC" "4"

# 首次请求 + 3 次重试 = 4 次 Produce。
check_eq "按 --retries 重试（共 4 次请求）" "$(grep -c '^PRODUCE' "$FAKE_LOG")" "4"
check_eq "重试事件 3 条（R3）" "$(grep -c '"event":"retry"' "$WORK/err.jsonl")" "3"
grep -q '"error":"broker_unavailable"' "$WORK/err.jsonl" \
  && pass "重试事件带 broker_unavailable" || fail "重试事件错误码不符"

# 两条消息都是 broker_unavailable（整体未被尝试），不是 partition_blocked。
check_eq "逐条结果 2 行" "$(grep -c '"index":' "$WORK/out.jsonl")" "2"
check_eq "两条均为 broker_unavailable（未被部分尝试）" \
  "$(grep -c '"error":"broker_unavailable"' "$WORK/out.jsonl")" "2"
if grep -q 'partition_blocked' "$WORK/out.jsonl"; then
  fail "请求级失败不应产生 partition_blocked"
else
  pass "无 partition_blocked（请求整体未执行）"
fi
grep -q '"event":"produce_summary","status":"failed","succeeded":0,"failed":2' \
  "$WORK/out.jsonl" && pass "汇总 failed/0 成功" || fail "汇总计数不符"
grep -q '"exit_reason":"retry_exhausted"' "$WORK/out.jsonl" \
  && pass "汇总标注 retry_exhausted" || fail "汇总缺少 exit_reason"

# 对照：--retries 0 时只发 1 次请求，立即失败（不重试）。
kill -TERM "$FAKE_PID" 2>/dev/null || true
sleep 0.2
kill -9 "$FAKE_PID" 2>/dev/null || true
FAKE_PID=""
: >"$FAKE_LOG"
python3 "$WORK/fake_broker.py" "$PORT" >>"$FAKE_LOG" 2>&1 &
FAKE_PID=$!
for _ in $(seq 1 100); do
  grep -q "LISTENING" "$FAKE_LOG" 2>/dev/null && break
  sleep 0.1
done
set +e
"$MK" produce --topic rt --file "$WORK/input.jsonl" --retries 0 \
  --retry-timeout-ms 1 >"$WORK/out0.jsonl" 2>"$WORK/err0.jsonl"
RC0=$?
set -e
check_eq "--retries 0 时退出码 4" "$RC0" "4"
check_eq "--retries 0 只发 1 次请求" "$(grep -c '^PRODUCE' "$FAKE_LOG")" "1"
check_eq "--retries 0 无重试事件" "$(grep -c '"event":"retry"' "$WORK/err0.jsonl" || true)" "0"

echo
if [[ "$FAILED" -eq 0 ]]; then
  echo "backpressure_retry: 全部通过"
  exit 0
fi
echo "backpressure_retry: $FAILED 项失败"
exit 1
