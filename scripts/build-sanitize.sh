#!/usr/bin/env bash
# Sanitizer 构建 + 单元测试。默认 ASan+UBSan；SANITIZE=thread 切换为 TSan。
# 用法：bash scripts/build-sanitize.sh [build-dir]
set -euo pipefail

cd "$(dirname "$0")/.."
BUILD_DIR="${1:-build-sanitize}"
SANITIZE="${SANITIZE:-address,undefined}"

case "$SANITIZE" in
  address,undefined|thread)
    ;;
  *)
    echo "SANITIZE must be 'address,undefined' or 'thread'" >&2
    exit 2
    ;;
esac

CXX_FLAGS="-fsanitize=${SANITIZE} -fno-omit-frame-pointer -g"
LINK_FLAGS="-fsanitize=${SANITIZE}"

cmake -S . -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Debug \
  -DCMAKE_CXX_FLAGS="$CXX_FLAGS" \
  -DCMAKE_EXE_LINKER_FLAGS="$LINK_FLAGS"
cmake --build "$BUILD_DIR" -j"$(nproc)"
(cd "$BUILD_DIR" && ctest --output-on-failure)
