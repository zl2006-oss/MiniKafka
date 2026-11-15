# miniKafka

一个从零实现的类 Kafka 消息队列（C++14，Linux/epoll），用于验证"页缓存即 ack + 分段日志 + 稀疏索引 + 双槽位点"这套存储语义在单机上的可行性。全部代码、测试与验收脚本按 `docs/` 下的需求与设计文档实现，实现偏差记录在 `docs/design/07.实现偏差记录.md`。

## 1. 能力范围

| 维度 | 已实现 |
| --- | --- |
| 投递语义 | 至少一次（允许重复）；写入 OS page cache 即回 ack，不 `fsync`（R1） |
| 崩溃恢复 | `kill -9` 后重启无需人工干预，已 ack 消息可读、offset 连续（R7/R27/V1） |
| Topic | 创建/列举，1–16 分区，元数据落盘（R14） |
| 生产 | 批量逐条结果、显式分区 / key 哈希 / 轮询选分区、可重试错误自动重试、重试耗尽输出对账汇总（R2/R22/R23/R24） |
| 消费 | 静态分配消费组（`partition % count == index`），位点持久化、自动提交、跟随模式（R6/R10/R11/R18） |
| 重放 | 按 offset 只读回放，不影响组位点（R12/V10） |
| 压测 | 多 Producer 并发纯写，输出吞吐与 ack 延迟分位（R4/R5/R26） |
| 协议 | 24B 固定开销帧（magic `MKP1` + CRC32），7 请求 / 7 响应（05） |

未实现（设计即如此，非缺陷）：动态 rebalance、消息压缩、副本与多 Broker、事务、时间戳索引、TLS。

## 2. 目录结构

```
include/minikafka/      公共头（common / protocol / net / storage / broker / group / client / bench）
src/                    实现，按模块分目录；src/main.cpp 为唯一产品可执行文件入口
test/unit/              单元测试（自研断言 + CTest）
scripts/build.sh        配置 + 构建 + ctest
scripts/integration/    端到端验收（V2/V9/V10/V11）
scripts/fault_injection/ 故障注入（V1、磁盘不足、1 MiB 边界与批内部分失败）
scripts/bench/          吞吐与延迟实测（V4/V5）
docs/                   需求、设计、规约、偏差记录
prototype/              早期原型与 CLI 交互规范
```

## 3. 构建

依赖：Linux、CMake ≥ 3.10、支持 C++14 的 GCC/Clang、pthread。

```bash
bash scripts/build.sh              # RelWithDebInfo，默认 build/
CMAKE_BUILD_TYPE=Release bash scripts/build.sh build-release
```

产物：`build/minikafka`（唯一可执行文件，含全部子命令）与 `build/mk_*_tests`。

## 4. 快速开始

Broker 前台常驻；`produce/consume/replay` 按 03 §3.4 固定连 `127.0.0.1:9092`（仅 `bench` 可用 `--broker` 覆盖），因此下面的命令要求 9092 空闲。

```bash
# 终端 1：启动（默认 --port 9092 --data-dir ./data）
./build/minikafka broker --port 9092 --data-dir /tmp/mkdata
# {"event":"broker_started","port":9092,"data_dir":"/tmp/mkdata","status":"running"}
# {"event":"ready"}                      ← 脚本以 ready 判定就绪（Q-P10）

# 终端 2：建 Topic → 投递 → 消费 → 回放
./build/minikafka topic create --topic orders --partitions 4
./build/minikafka topic list
printf '%s\n' '{"key":"k1","value":"v1"}' '{"value":"v2"}' \
  | ./build/minikafka produce --topic orders
./build/minikafka consume --topic orders --group g1 --consumer-index 0 \
  --consumer-count 1 --auto-commit-interval 1s --exit-at-end
./build/minikafka replay --topic orders --partition 0 --offset 0 --count 10
```

输出约定（R19）：**stdout** 只放结果（逐条结果、消息行、`produce_summary`、`bench_result`）；**stderr** 放生命周期事件、重试、错误对象 `{"error":"<code>","message":"..."}`。每行一个 JSON。

## 5. CLI 参数

| 命令 | 参数（默认值） |
| --- | --- |
| `broker` | `--port 9092`；`--data-dir ./data`；`--io-threads 0`（0 = `min(4, max(1, cores/2))`）；`--storage-threads 0`；`--min-free-space-mb 256`；`--segment-bytes 268435456`（下限 2097152）；`--log-level warn` |
| `topic create` | `--topic <name>` 必填；`--partitions 1..16` 必填；重复返回 `topic_exists` |
| `topic list` | 无参数；每个 Topic 一行 JSON |
| `produce` | `--topic` 必填；`--partition <n>` 可选；`--retries 3`；`--retry-backoff-ms 100`；`--retry-timeout-ms 5000`；`--message '<json>'` / `--file <path>` / stdin（优先级同左） |
| `consume` | `--topic`、`--group`、`--consumer-index`、`--consumer-count` 必填；`--from earliest`（`earliest`/`latest`/`<n>`）；`--auto-commit-interval 5s`；`--exit-at-end` 关闭 |
| `replay` | `--topic` 必填；`--partition 0`；`--offset 0`；`--count <n>` 缺省读到末尾 |
| `bench` | `--topic`、`--messages` 必填；`--broker 127.0.0.1:9092`；`--mode throughput\|latency`；`--producers 4`；`--batch 100`；`--payload-bytes 100`；`--key-bytes 8` |

输入 / 输出样例：

```json
{"key":"k1","value":"v1"}                                  // 输入：value 必填，key 缺失或 null 视为无 key
{"index":0,"partition":2,"offset":0,"status":"ok"}         // produce 逐条结果
{"topic":"orders","partition":2,"offset":0,"key":"k1","value":"v1"}  // consume / replay 消息行
{"event":"produce_summary","status":"partial_failure","succeeded":2,"failed":1,
 "success_offsets":[{"partition":0,"offsets":[0,1]}],"failed_indices":[2],
 "exit_reason":"validation_error"}                         // 仅在存在失败条目时输出
```

退出码：`0` 成功；`2` 用法；`3` 校验/业务；`4` 投递失败（含重试耗尽）。

## 6. 数据目录布局

```
<data-dir>/
  meta/<topic>.meta                     Topic 元数据（分区数、创建时间），原子写
  topics/<topic>/<partition>/
    <base_offset:020d>.log              分段日志，记录 = 24B 头 + key + value（上限 1 MiB）
    <base_offset:020d>.index            稀疏索引，4 KiB 一个检查点，可重建
  groups/<group>/<topic>/offsets.a      位点主槽（generation + CRC）
  groups/<group>/<topic>/offsets.b      位点备槽，读取时取双槽中更大的完整版本
```

记录头：`total_len(4) crc32(4) magic "MKAF"(4) version(2) flags(2) key_len(4) value_len(4)`；CRC32 覆盖 `[0,4)` 与 `[8, total_len)`。

## 7. 验收

```bash
bash scripts/build.sh                                  # 单元测试：8 个 CTest 目标全绿
bash scripts/integration/e2e.sh                        # 基础链路 + 错误路径
bash scripts/integration/v2_follow_mode.sh              # V2 跟随模式 / SIGTERM 补提交
bash scripts/integration/v9_static_assignment.sh        # V9 静态分配
bash scripts/integration/v10_replay_readonly.sh         # V10 重放只读 + V11 顺序
bash scripts/fault_injection/v1_kill9_recovery.sh       # V1 kill -9 恢复
bash scripts/fault_injection/disk_full.sh               # 磁盘不足拒写
bash scripts/fault_injection/v8_message_too_large.sh    # V8 部分失败与 1 MiB 边界
bash scripts/bench/run_bench.sh                         # V4 吞吐 + V5 延迟
```

脚本全部要求 9092 空闲（CLI 契约固定默认地址），会自建临时数据目录并在退出时清理。

### 实测结果（4 vCPU / 8 GiB VMware Ubuntu，broker 与客户端同机）

| 指标 | 目标 | 实测 | 参数 |
| --- | --- | --- | --- |
| Broker 总入口吞吐（R5） | ≥ 100 000 条/秒 | **142 857 条/秒**（另一次 200 000，16 producers / batch 500） | 4 producers，batch 100，8 分区，payload 100B + key 8B |
| Producer ack 延迟 P99（R4） | < 10 ms | **2 ms**（p50 1ms，max 4ms） | 1 producer，batch 1，1000 条 |

页缓存 ack 的代价是掉电可能丢尾部未落盘数据（允许，见 R1）；`kill -9` 不丢（页缓存由内核持有），由 V1 脚本验证。

## 8. 实现偏差

`docs/design/07.实现偏差记录.md` 记录 14 项实现选择，其中 5 项待项目所有者裁决：

- **D-02** `broker` 默认数据目录取 `./data`（原型写 `.demo-state`）。
- **D-03** 告警事件统一为 `{"event":"warning","detail":...}`（原型为结构化字段）。
- **D-04** Storage Worker 队列满时等 200 ms 后回 `broker_unavailable`，而非暂停连接读取。
- **D-10** `--from latest` 依赖一次越界 Fetch 探测 `log_end`（无独立元数据报文）。
- **D-11** 消费中 `offset_out_of_range` 收敛到 `log_end` 并告警，而非退出。

## 9. 文档索引

| 文件 | 内容 |
| --- | --- |
| `docs/01.需求文档.md` | R1–R31 需求、V1–V11 验收、性能口径 |
| `docs/design/02.功能模块梳理.md` | F-* 功能点与 M1–M8 模块划分 |
| `docs/design/03.整体架构设计.md` | 线程模型、CLI 契约、AD-* 架构决策 |
| `docs/design/04.存储设计.md` | 记录格式、分段、索引、位点、恢复规约 |
| `docs/design/05.协议设计.md` | 帧格式、7 种报文、错误码表 |
| `docs/design/06.设计方案评审结果.md` | 评审结论与裁决项 |
| `docs/design/07.实现偏差记录.md` | D-01~D-14 实现偏差与待裁决项 |
| `docs/rules/{storage,protocol}-rules.md` | 存储/协议硬规约（S-*、P-*） |
| `CLAUDE.md` | 开发约定（模块顺序、测试与文档要求） |

## 10. 已知限制

- 单 Broker、无副本：Broker 进程与机器即单点；`kill -9` 可恢复，机器宕机允许丢页缓存尾部。
- 无动态 rebalance：Consumer 槽位由部署者保证唯一；某槽位下线后其分区暂停，直至同 index 重新加入（Q-P2=A）。
- 无时间戳索引与按时间查找；`--from latest` 需一次额外探测（D-10）。
- 无消息压缩与批量压缩；1 MiB 单条上限由客户端与 Broker 双侧校验。
- `--log-level` 目前只做取值校验，不影响输出级别（D-14）。
