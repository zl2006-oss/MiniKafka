#!/usr/bin/env bash
# 一键验收：构建 + 单元测试 + 集成 + 故障注入 + 压测。
# 用法：
#   bash scripts/acceptance.sh                # 全量（含 bench）
#   SKIP_BENCH=1 bash scripts/acceptance.sh   # 跳过耗时压测
#   SKIP_BUILD=1 bash scripts/acceptance.sh   # 复用已构建的 build/
# 所有脚本按 03 §3.4 固定占用 127.0.0.1:9092，必须串行执行。
set -uo pipefail

cd "$(dirname "$0")/.."
BUILD_DIR="${BUILD_DIR:-build}"
SKIP_BUILD="${SKIP_BUILD:-0}"
SKIP_BENCH="${SKIP_BENCH:-0}"
REPORT="${REPORT:-$BUILD_DIR/acceptance-report.txt}"
FAILED=0
TOTAL=0

mkdir -p "$(dirname "$REPORT")"
if command -v ss >/dev/null 2>&1 && ss -ltn 2>/dev/null | grep -q ':9092 '; then
  echo "port 9092 已被占用：03 §3.4 固定默认 broker 地址，请先释放" >&2
  exit 1
fi
{
  echo "miniKafka acceptance report"
  echo "date: $(date -Is)"
  echo "host: $(uname -a)"
  echo "commit: $(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
  echo "build: $BUILD_DIR"
} | tee "$REPORT"

run_step() { # <label> <script>
  TOTAL=$((TOTAL + 1))
  echo "" | tee -a "$REPORT"
  echo "=== $1 ===" | tee -a "$REPORT"
  if bash "$2" 2>&1 | tee -a "$REPORT"; then
    echo "PASS  $1" | tee -a "$REPORT"
  else
    echo "FAIL  $1" | tee -a "$REPORT"
    FAILED=$((FAILED + 1))
  fi
}

if [[ "$SKIP_BUILD" != "1" ]]; then
  TOTAL=$((TOTAL + 1))
  echo "" | tee -a "$REPORT"
  echo "=== build + unit tests (ctest) ===" | tee -a "$REPORT"
  if bash scripts/build.sh "$BUILD_DIR" 2>&1 | tee -a "$REPORT"; then
    echo "PASS  build + unit tests" | tee -a "$REPORT"
  else
    echo "FAIL  build + unit tests" | tee -a "$REPORT"
    echo "构建或单元测试失败，停止验收。" | tee -a "$REPORT"
    exit 1
  fi
else
  echo "skip build (SKIP_BUILD=1)" | tee -a "$REPORT"
fi

run_step "e2e: 基础链路与错误路径" "scripts/integration/e2e.sh"
run_step "V2/R18/R8: 跟随模式与 SIGTERM 补提交" "scripts/integration/v2_follow_mode.sh"
run_step "V2: 消费者 kill -9 续消费" "scripts/integration/v2_kill9_resume.sh"
run_step "V9: 静态分配与失效分区暂停" "scripts/integration/v9_static_assignment.sh"
run_step "V10/V11: 重放只读与分区内顺序" "scripts/integration/v10_replay_readonly.sh"
run_step "V1: Broker kill -9 恢复与残缺尾部截断" "scripts/fault_injection/v1_kill9_recovery.sh"
run_step "V6: 磁盘满三阶段" "scripts/fault_injection/disk_full.sh"
run_step "V7/V8: 超限拒绝与批内部分失败" "scripts/fault_injection/v7_v8_message_limits.sh"
run_step "V8: 批量重试耗尽与分区阻塞" "scripts/fault_injection/v8_batch_retry.sh"
run_step "R24/D-04: 请求级背压（broker_unavailable）重试" "scripts/fault_injection/r24_backpressure_retry.sh"
run_step "V2/R8: 位点提交失败重试与告警" "scripts/fault_injection/v2_commit_failure.sh"
run_step "R28/R30: 位点与元数据损坏恢复" "scripts/fault_injection/v2_v30_corruption.sh"

if [[ "$SKIP_BENCH" != "1" ]]; then
  run_step "V4/V5: 吞吐与 ack 延迟" "scripts/bench/run_bench.sh"
else
  echo "skip bench (SKIP_BENCH=1)" | tee -a "$REPORT"
fi

echo "" | tee -a "$REPORT"
echo "summary: $((TOTAL - FAILED))/$TOTAL passed, $FAILED failed" | tee -a "$REPORT"
echo "report: $REPORT" | tee -a "$REPORT"
if [[ "$FAILED" -ne 0 ]]; then
  exit 1
fi
exit 0
