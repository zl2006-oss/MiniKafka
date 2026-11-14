# protocol-rules.md（已确认 v1.0）

> 项目：miniKafka（类 Kafka 消息队列，演示验证型学习项目）
> 性质：**协议规则**，用于补齐 CLAUDE.md「# 3. 协议设计规则」要求；不是 `05.协议设计.md`。
> 状态：**已确认 v1.0**。
> 依据：`docs/01.需求文档.md`（R1–R31、V1–V11）、`docs/design/03.整体架构设计.md`（已确认 v1.0）、`docs/rules/storage-rules.md`（已确认 v1.0）、`docs/design/04.存储设计.md`（批量模式）、CLAUDE.md 第 3 节。
> 边界：只定义网络帧、报文、错误码与协议层行为约束；**不定义存储文件格式**（属 04）；不写 C++ 类定义或任何源码。
> 前置：03 已确认；04 已生成；05 与本规则同步生成。

---

## 0. 结论速览

| 编号 | 决策点 | 定稿 | 状态 |
| --- | --- | --- | --- |
| P-01 | 帧结构 | 20 字节固定头 + 载荷 + 4 字节尾部 CRC（固定开销 24 字节），见第 2 节 | 已生成 |
| P-02 | 字节序 | 所有整数**大端** | 已生成 |
| P-03 | 粘包/半包 | `payload_len` 长度前缀；单帧载荷上限 8 MiB | 已生成 |
| P-04 | 帧校验 | CRC32 IEEE（`0xEDB88320`，`"123456789"` → `0xCBF43926`），覆盖头+载荷、排除 CRC 自身 | 已生成 |
| P-05 | 请求关联 | `request_id u64`；响应原样回显；同连接可多请求在途、响应可乱序 | 已生成 |
| P-06 | 报文集合 | CreateTopic、ListTopics、Produce、Fetch、OffsetFetch、OffsetCommit、Replay；**无心跳/会话/join-group** | 已生成 |
| P-07 | 批量生产 | 按分区一次请求；逐条结果（成功含 offset、失败含错误码与 index） | 已生成 |
| P-08 | 1 MiB 口径 | `record_size = 24 + key_len + value_len ≤ 1 MiB`；协议帧头与逐条编码开销不计入 | 已生成 |
| P-09 | 错误码 | 数值码 + 字符串码双轨；完整表见第 6 节；退出码 0/2/3/4/130 | 已生成 |
| P-10 | 跟随等待 | Broker 立即返回空结果；客户端短轮询；**不做 Broker 侧长轮询** | 已生成 |
| P-11 | 压缩/加密/鉴权 | MVP 均不做（R25 排除 ACL） | 已生成 |
| P-12 | 依赖 | 自研编解码；禁止 protobuf/boost/gRPC/现成网络或 MQ 库 | 已生成 |
| P-13 | 静态分配 | 分配纯函数并入 OffsetFetch 响应；Broker 无成员状态（Q-P2=A） | 已生成 |
| P-14 | 批量/响应上限 | 单请求条目 ≤ 1024；Fetch/Replay 单响应 ≤ 4 MiB 或 500 条 | 已生成 |

---

## 1. 不变量

1. 每个帧自描述、可独立校验；解码器不得依赖跨帧状态拼接业务字段。
2. 先校验帧头与 CRC，再做业务解码；CRC 失败不得进入业务处理。
3. 请求与响应按 `request_id` 关联；响应可乱序，客户端不得假设顺序。
4. Produce 请求只针对一个 `(topic, partition)`；同一分区同一时刻客户端只允许 1 个 Produce 在途（R2）。
5. 批次内每条请求条目必须恰有一个结果条目，顺序与请求一致；成功必有 offset，失败必有错误码，`index` 即定位标识（R23）。
6. 单条存储记录 ≤ 1 MiB；协议帧头、批量头、逐条字段不计入该上限（R17/S-09）。
7. 不定义心跳、会话、join-group、rebalance 报文；无组成员状态（R10/Q-P2=A/B6）。
8. 协议层不持久化、不 fsync；ack 语义仍由存储层“页缓存追加成功”决定（R1）。
9. 错误码只使用第 6 节已定义值；不得临时新增或复用。
10. 所有长度字段必须先做上界检查再分配内存，禁止按不可信长度直接分配。

---

## 2. 帧格式

```text
+--------+---------+----------+------------+-------------+---------+--------+
| magic  | version | msg_type | request_id | payload_len | payload | crc32  |
| 4      | 2       | 2        | 8          | 4           | N       | 4      |
+--------+---------+----------+------------+-------------+---------+--------+
  0        4         6          8            16            20        20+N
```

| 偏移 | 字节 | 字段 | 类型 | 规则 |
| --- | --- | --- | --- | --- |
| 0 | 4 | `magic` | u32 BE | 固定 `0x4D4B5031`（ASCII “MKP1”） |
| 4 | 2 | `version` | u16 BE | 固定 `1`；其他值返回 `unsupported_version` |
| 6 | 2 | `msg_type` | u16 BE | bit15=1 为响应；请求 `0x0001..0x7FFF`，响应 `0x8001..0xFFFF`；见第 4 节 |
| 8 | 8 | `request_id` | u64 BE | 客户端生成，连接内单调递增；响应原样回显 |
| 16 | 4 | `payload_len` | u32 BE | 载荷字节数；`0 ≤ payload_len ≤ 8,388,608`（8 MiB） |
| 20 | N | `payload` | bytes | 报文正文；N=`payload_len` |
| 20+N | 4 | `crc32` | u32 BE | CRC32 IEEE；覆盖 `[0, 20+N)`（头+载荷），排除 CRC 自身 |

- 帧总长 = `24 + payload_len`。
- 读取：先读 20 字节头 → 校验 magic/version/`payload_len` 上界 → 读 `N+4` 字节 → 校验 CRC → 业务解码。
- 长度前缀同时解决粘包（一次读到多帧）与半包（读不满时继续等待）。
- `payload_len` 超过 8 MiB：若头可解析，返回 `request_too_large` 响应后关闭连接；否则直接关闭。
- 编码原语：`u8/u16/u32/u64`；`str` = `u32 len` + UTF-8 字节；`blob` = `u32 len` + 原始字节；`array<T>` = `u32 count` + T 序列。所有长度均为大端。
- 严格解码：字段不足、长度越界、尾随多余字节均返回 `payload_decode_error`；不做压缩、不做 TLV、不做未知字段跳过。

---

## 3. CRC 与字节序

- **大端（network byte order）**：帧头、载荷内全部整数字段、存储记录均一致。
- CRC32 IEEE：反射多项式 `0xEDB88320`，初值 `0xFFFFFFFF`，最终异或 `0xFFFFFFFF`；测试向量 `"123456789"` → `0xCBF43926`。
- 帧 CRC 与存储记录 CRC 复用同一实现，但**语义独立**：帧 CRC 只保护本次传输的帧；存储 CRC 只保护落盘记录。任一失败都不得被另一层“补救”。
- `key`/`value` 是原始 UTF-8 字节；非法 UTF-8 返回 `invalid_utf8`，不截断。

---

## 4. 报文集合

| msg_type | 报文 | 方向 | 处理线程 | 说明 |
| --- | --- | --- | --- | --- |
| `0x0001` | CreateTopic | C→B | Main/控制面 | 创建 Topic；先写 meta 再替换不可变快照（R30/AD-15） |
| `0x0002` | ListTopics | C→B | Main/控制面 | 返回已加载 Topic 与分区数 |
| `0x0003` | Produce | C→B | 分区所属 Storage Worker | 单分区批次；逐条结果（R23） |
| `0x0004` | Fetch | C→B | 分区所属 Storage Worker | 立即返回；无消息返回空结果 + 末尾 offset |
| `0x0005` | OffsetFetch | C→B | Group-shard Storage Worker | 返回静态分配 + 已提交位点（Q-P2=A） |
| `0x0006` | OffsetCommit | C→B | Group-shard Storage Worker | 全量快照提交；全成或全不成（S-13） |
| `0x0007` | Replay | C→B | 分区所属 Storage Worker | 无组只读；不含 group 字段（D-06/R12） |
| `0x8001`–`0x8007` | 对应响应 | B→C | 发起请求的 IO Reactor | `payload` 以 `error_code u16` 开头 |

**明确不定义**：心跳、会话、join-group、leave-group、sync-group、成员注册、rebalance 相关报文（R10/Q-P2=A/B6）。P1 若引入，另行设计并升 version。

---

## 5. 批量逐条结果

- Produce 请求只含一个 `partition`；批次内条目顺序即追加顺序（AD-13/R2）。
- 响应 `results[]` 与请求条目一一对应，含 `index`、`status`、`error_code`、`offset`。
- `status`：`0=appended`（有 offset）、`1=failed`（有错误码）、`2=not_attempted`（同分区首失败后被阻塞，错误码 `partition_blocked`）。
- **AD-14**：同分区首个失败后，后续条目一律 `not_attempted`；其他分区使用独立请求，互不影响。
- 失败条目不分配 offset、不写记录；成功条目按顺序分配连续 offset（R23/R27）。
- 客户端只重试 `failed` 中可重试的错误；`not_attempted` 在失败条目成功后按原顺序重发（R24）。
- 重试耗尽：输出逐条错误与按分区成功 offset 汇总（R29），退出码 4；仅校验类失败退出码 3。

---

## 6. 错误码

### 6.1 分段

| 区间 | 用途 |
| --- | --- |
| `0x0000` | 成功 |
| `0x0100–0x01FF` | 帧/解码/协议版本 |
| `0x0200–0x02FF` | Topic/分区/名称校验 |
| `0x0300–0x03FF` | 消息内容/批次 |
| `0x0400–0x04FF` | offset/读取边界 |
| `0x0600–0x06FF` | 连接/服务状态 |
| `0x0700–0x07FF` | 磁盘/存储状态 |
| `0x0900–0x09FF` | 客户端本地投递失败 |

### 6.2 完整码表（触发条件与客户端行为见 05 §5）

| 码值 | 字符串 | 产生方 | 归类 |
| --- | --- | --- | --- |
| `0x0000` | `ok` | 双方 | 成功 |
| `0x0100` | `invalid_frame` | 双方 | 协议 |
| `0x0101` | `frame_crc_error` | 双方 | 协议 |
| `0x0102` | `unsupported_version` | 双方 | 协议 |
| `0x0103` | `unsupported_message_type` | Broker | 协议 |
| `0x0104` | `payload_decode_error` | Broker | 协议 |
| `0x0105` | `request_too_large` | Broker | 协议 |
| `0x0201` | `invalid_topic_name` | Broker/CLI | 校验 |
| `0x0202` | `invalid_group_name` | Broker/CLI | 校验（R31） |
| `0x0203` | `topic_not_found` | Broker | 业务 |
| `0x0204` | `topic_exists` | Broker | 业务 |
| `0x0205` | `invalid_partitions` | Broker/CLI | 校验 |
| `0x0206` | `invalid_partition` | Broker/CLI | 校验 |
| `0x0207` | `invalid_consumer_slot` | Broker/CLI | 校验 |
| `0x0301` | `message_too_large` | Broker/CLI | 校验（R17） |
| `0x0302` | `invalid_value_type` | CLI | 校验 |
| `0x0303` | `invalid_json` | CLI | 校验 |
| `0x0304` | `invalid_utf8` | Broker/CLI | 校验 |
| `0x0305` | `invalid_batch` | Broker | 校验 |
| `0x0306` | `partition_blocked` | Broker/CLI | 批次状态（R24） |
| `0x0401` | `offset_out_of_range` | Broker/CLI | 业务 |
| `0x0402` | `invalid_offset` | CLI | 用法 |
| `0x0403` | `invalid_count` | CLI | 用法 |
| `0x0404` | `invalid_from` | CLI | 用法 |
| `0x0405` | `invalid_interval` | CLI | 用法 |
| `0x0601` | `broker_unavailable` | 客户端本地 | 连接 |
| `0x0602` | `internal_error` | Broker | 服务 |
| `0x0701` | `disk_full` | Broker | 存储（R16） |
| `0x0702` | `storage_unavailable` | Broker | 存储（D-04-6） |
| `0x0703` | `storage_io_error` | Broker | 存储 |
| `0x0901` | `delivery_failed` | 客户端本地 | 投递失败（R3） |

- CLI 另有 `usage`（无线上码值），退出码 2。
- `retry_exhausted` 是 `produce_summary.exit_reason` 的取值，不是错误码。
- 错误对象统一 `{"error":"<字符串码>","message":"<说明>"}`；线上数值码由客户端映射为字符串码。

### 6.3 退出码映射（Q-P9）

| 退出码 | 含义 | 典型错误 |
| --- | --- | --- |
| 0 | 成功（含消费者正常退出并完成最终提交） | `ok` |
| 2 | 用法错误 | `usage`、`invalid_offset/count/from/interval` |
| 3 | 校验/业务错误 | `invalid_*`、`topic_*`、`message_too_large`、`offset_out_of_range`、`storage_unavailable` |
| 4 | 投递失败（重试耗尽） | `disk_full`、`delivery_failed`、连接失败耗尽 |
| 130 | 非消费者角色被 SIGINT 中断 | 保留 |

---

## 7. 跟随等待

- Fetch **立即返回**：offset 等于分区末尾时返回 0 条记录 + 末尾 offset，不阻塞、不注册等待者。
- Consumer 在客户端轮询：本周期全部分配分区均为空 → 等待 **100ms** → 再拉取；连续有数据则立即继续。
- `--exit-at-end`：首次“全部分区为空且 offset=末尾”即补一次最终提交（Q-P6，最多等 2s）并退出 0（R18）。
- 自动提交定时器（默认 5s，Q-P4）在 Client Reactor 独立触发，不受轮询等待影响（R6）。
- Broker 不引入长轮询定时器/等待队列；不新增 Broker 线程（03 AD-04/AD-06）。

---

## 8. 上限与背压

| 项 | 定稿 | 说明 |
| --- | --- | --- |
| 单帧载荷 | 8 MiB | 解码前检查；超限拒绝并关连接 |
| Produce 单请求条目 | ≤ 1024 | 客户端按分区组批 |
| 客户端组批建议 | ≤ 512 条且编码后 ≤ 4 MiB | 保证单帧 < 8 MiB |
| Fetch/Replay 单响应 | ≤ 4 MiB 且 ≤ 500 条 | 完整记录为界；至少返回 1 条可容纳记录 |
| 每连接在途请求 | ≤ 64 | 超过则暂停读取该连接（移除 EPOLLIN） |
| Storage Worker 队列 | 高水位 4096 / 低水位 1024（建议值） | 有界 MPSC；高水位暂停相关连接读取 |

- 所有队列有界；禁止无界缓冲（03 AD-12）。
- 同一分区 Produce 在途 = 1（客户端保证）；Broker 侧由分区所属 Storage Worker 串行追加。

---

## 9. 禁止项

1. 不引入 protobuf、boost、gRPC、现成网络库/MQ 库；不使用 JSON-RPC/HTTP。
2. 不定义心跳、会话、成员状态、join-group、rebalance 报文。
3. 不做压缩、TLS、鉴权、ACL（R25）；不做协议层持久化或 fsync。
4. 不定义存储文件格式、索引格式、位点文件格式（属 04）。
5. 不按不可信长度分配内存；不接受未知字段静默跳过。
6. 不写业务代码；本文只定义规则与格式约束。

---

## 10. 版本记录

| 版本 | 日期 | 说明 |
| --- | --- | --- |
| v0.1 | 2026-11-13 | 批量模式生成：帧格式、报文集合、批量逐条结果、错误码、跟随等待、上限与禁止项；待用户一次性终审 |
