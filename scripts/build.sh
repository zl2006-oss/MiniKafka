#!/usr/bin/env bash
# 在 Linux 下配置 + 构建 + 跑单元测试。
# 用法：bash scripts/build.sh [build-dir]
# 压测请用：CMAKE_BUILD_TYPE=Release bash scripts/build.sh build-release
set -euo pipefail

cd "$(dirname "$0")/.."
BUILD_DIR="${1:-build}"
CMAKE_BUILD_TYPE="${CMAKE_BUILD_TYPE:-RelWithDebInfo}"

cmake -S . -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE"
cmake --build "$BUILD_DIR" -j"$(nproc)"
(cd "$BUILD_DIR" && ctest --output-on-failure)
