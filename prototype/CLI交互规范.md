# miniKafka CLI 交互规范（原型 v0.1）

> 对应需求：`docs/01.需求文档.md`（R1–R29 为唯一口径）。
> 本阶段性质：**交互原型**。只模拟命令形态、输出与错误提示；数据全部是假的，不涉及真实网络、存储、位点逻辑，也不包含任何 C++ 业务代码。
> 原型的 `mk` 是 `prototype/demo.sh` 内的 bash 函数，不是真实可执行文件。
> 运行方式（Windows Git Bash）：`bash prototype/demo.sh`

---

## 1. 通用约定

### 1.1 调用形式

统一入口：`mk <子命令> [参数...]`（R15）。

| 子命令 | 作用 | 对应 R |
| --- | --- | --- |
| `mk broker` | 启动（模拟）Broker，并执行崩溃恢复扫描 | R1、R7、R27 |
| `mk topic create` | 创建 Topic | R14 |
| `mk topic list` | 列出 Topic 与分区 | R14、R15 |
| `mk produce` | 单条/批量生产消息 | R3、R17、R19、R22、R23、R24、R29 |
| `mk consume` | 以 Consumer Group 身份消费 | R6、R10、R11、R13、R18、R19 |
| `mk replay` | 按 offset 只读回放 | R12、R19 |

**不提供**：`topic delete`、消费者组/位点查询命令（R21）。

### 1.2 参数风格

- 长参数统一为 `--kebab-case value`，布尔开关不带值（R20）。
- 所有配置由命令行参数提供，**不读取配置文件**（R20）。
- 未提供的参数使用第 1.6 节的默认值。

### 1.3 输出流与格式

| 流 | 内容 | 格式 |
| --- | --- | --- |
| stdout | 命令结果、消息记录、produce 逐条结果、produce 汇总 | **JSON Lines**（每行一个 JSON 对象） |
| stderr | 生命周期事件（assigned/start/commit/follow/crash）、重试事件、错误对象 | JSON Lines |

- `demo.sh` 为便于肉眼核对，把 stderr 合并到 stdout 展示；真实 CLI 保持两流分离。
- 不输出颜色、时间戳、绝对路径或随机内容，保证重复运行输出一致。
- 消息记录字段（consume / replay）：`partition`、`offset`、`key`、`value`，均为必填；`key` 可为空字符串（R19）。
- 事件记录以 `event` 字段区分类型。
- 错误记录统一为 `{"error":"<code>","message":"<说明>"}`（R3、R16、R17）。

### 1.4 退出码

| 退出码 | 含义 | 对应 R |
| --- | --- | --- |
| 0 | 成功（含消费者正常读到末尾） | R18 |
| 2 | 用法错误（缺参数、未知参数、空输入） | — |
| 3 | 校验/业务错误（Topic 不存在、分区非法、超限、消息格式错误等） | R14、R17 |
| 4 | 投递失败：某分区重试耗尽 | R3、R24 |
| 137 | **仅原型模拟**的消费者 `kill -9`（不是最终 CLI 约定） | R6 |

### 1.5 默认值总表

> 标注"待确认"的默认值在需求文档中没有被固定，属于原型暂定值，见第 6 节。

| 参数 | 默认值 | 作用域 | 依据/状态 |
| --- | --- | --- | --- |
| `--port` | `9092` | broker | 原型暂定 |
| `--data-dir` | `.demo-state`（原型） | broker | R20；原型专用 |
| `--partitions` | 无默认，必填（1–16） | topic create | R14 |
| `--partition`（produce） | 不指定（按 R22 自动选择） | produce | R22 |
| `--partition`（replay） | `0` | replay | R12 |
| `--retries` | `3`（不含首次尝试，最多 1+3 次） | produce | R3 可配置；数值原型暂定 |
| 重试间隔 | 原型不等待（即时打印重试事件） | produce | R3；真值待设计 |
| `--from` | `earliest` | consume | **待确认**（Kafka 常用 `latest`） |
| `--auto-commit-interval` | `5s` | consume | R6 可配置；数值原型暂定 |
| `--exit-at-end` | 关闭（默认持续跟随） | consume | R18 |
| `--offset` | `0` | replay | R12 |
| `--count` | 不指定 = 从 offset 读到末尾 | replay | R12 |
| 单条消息上限 | `1MB`（原型用哨兵模拟） | produce | R17 |

### 1.6 校验规则

| 规则 | 原型行为 | 对应 R |
| --- | --- | --- |
| Topic 名 | 仅 `[A-Za-z0-9_-]+`，否则 `invalid_topic_name` | 原型补充 |
| 分区数 | 1–16，否则 `invalid_partitions` | R14 |
| Topic 是否存在 | 不存在返回 `topic_not_found` | — |
| 分区号范围 | 必须小于该 Topic 的分区数 | R14 |
| 单条消息大小 | > 1MB 拒绝，不截断 | R17 |
| 同名 Topic | 拒绝并返回 `topic_exists` | R14 |
| Topic 分区数 | 创建后不可修改；原型不提供修改入口 | R14 |

---

## 2. 子命令规范

### 2.1 `mk broker`

```
mk broker [--port <port>] [--data-dir <dir>]
```

**参数与默认值**

| 参数 | 默认 | 说明 | 对应 R |
| --- | --- | --- | --- |
| `--port` | `9092` | 监听端口（原型只回显，不真正监听） | R20 |
| `--data-dir` | `.demo-state` | 假数据目录（原型专用） | R20 |

**行为**：打印 `broker_started`、执行恢复扫描、打印 `ready`。原型启动后立即返回，不驻留进程。

**stdout**

```json
{"event":"broker_started","port":9092,"data_dir":".demo-state","status":"running"}
{"event":"ready"}
```

**stderr（恢复时）**

```json
{"event":"recovery","topic":"orders","partition":0,"kept":5,"dropped":1}
```

**错误**：未知参数 → `usage`，退出码 2。

**对应 R**：R1、R7、R27（恢复与 offset 重建）；R20（命令行配置）。

---

### 2.2 `mk topic create`

```
mk topic create --topic <name> --partitions <n>
```

**参数与默认值**

| 参数 | 默认 | 说明 | 对应 R |
| --- | --- | --- | --- |
| `--topic` | 必填 | Topic 名称 | R14 |
| `--partitions` | 必填 | 1–16；创建后不可修改 | R14 |

**stdout**

```json
{"topic":"orders","partitions":4,"status":"created"}
```

**错误**

| 错误码 | 触发条件 | 退出码 |
| --- | --- | --- |
| `invalid_topic_name` | 名称不匹配 `[A-Za-z0-9_-]+` | 3 |
| `invalid_partitions` | 非整数或不在 1–16 | 3 |
| `topic_exists` | 同名 Topic 已存在 | 3 |

**对应 R**：R14（分区上限、不可修改）、R15（子命令形态）、R21（不提供删除）。

---

### 2.3 `mk topic list`

```
mk topic list
```

**stdout**：每个 Topic 一行；无 Topic 时输出 `{"topics":[]}`。

```json
{"topic":"orders","partitions":4,"partition_ids":[0,1,2,3]}
```

**对应 R**：R14、R15、R21（不提供组/位点查询）。

---

### 2.4 `mk produce`

```
mk produce --topic <name> [--partition <n>] [--retries <n>]
           [--message '<json>' | --file <path> | <stdin>]
```

**参数与默认值**

| 参数 | 默认 | 说明 | 对应 R |
| --- | --- | --- | --- |
| `--topic` | 必填 | 目标 Topic | — |
| `--partition` | 未指定则按 R22 选择 | 显式分区；对批量中的所有条目生效 | R22 |
| `--message` | — | 单条 JSON 消息 | R19 |
| `--file` | — | JSON Lines 文件，每行一条 | R19 |
| stdin | — | 未给 `--message/--file` 时从管道读取 | R19 |
| `--retries` | `3` | 重试次数（不含首次尝试） | R3 |

**输入优先级**：`--message` > `--file` > stdin；三者都没有且 stdin 是终端 → `usage`，退出码 2。

**分区选择（R22）**：显式 `--partition` > key 哈希 > 无 key 轮询。原型哈希为 FNV-1a 32-bit（实现细节，见第 6 节待确认）。

**stdout（逐条结果，R23）**

成功：
```json
{"index":0,"partition":0,"offset":0,"status":"ok"}
```
失败：
```json
{"index":2,"partition":0,"status":"error","error":"disk_full"}
```
被分区阻塞：
```json
{"index":3,"partition":0,"status":"error","error":"partition_blocked"}
```

**stderr（重试，R3/R24）**

```json
{"event":"retry","index":2,"partition":0,"attempt":1,"error":"disk_full"}
```

**stdout（失败汇总，R29）**

```json
{"event":"produce_summary","status":"partial_failure","succeeded":2,"failed":2,
 "success_offsets":[{"partition":0,"offsets":[1,2]}],
 "failed_indices":[2,3],"exit_reason":"retry_exhausted"}
```

- `status`：有成功条目时为 `partial_failure`，全部失败时为 `failed`。
- `exit_reason`：`retry_exhausted` 或 `validation_error`。
- 只要本批次存在失败条目就输出汇总；汇总包含成功条目的分区与 offset（R29）。

**错误语义**

| 情况 | 逐条结果 / 事件 | 重试 | 退出码 | 对应 R |
| --- | --- | --- | --- | --- |
| 消息超过 1MB | `message_too_large` | 不重试 | 3（若同时有重试耗尽则 4） | R17 |
| JSON 缺 `value` | `invalid_json` | 不重试 | 3 | R19 |
| 磁盘满 | `disk_full` | 按 `--retries` 重试，耗尽后阻塞该分区 | 4 | R16、R3、R24 |
| 同一分区已有失败条目 | `partition_blocked` | 不再尝试，直到该分区恢复发送 | — | R24 |
| 某分区重试耗尽 | 逐条错误 + `produce_summary` | 耗尽后退出 | 4 | R3、R24、R29 |
| 只有校验类失败 | 逐条错误 + `produce_summary` | 无 | 3 | R17、R19 |

**对应 R**：R2（分区内保序，原型按顺序处理）、R3、R16、R17、R19、R22、R23、R24、R29、R20。

---

### 2.5 `mk consume`

```
mk consume --topic <name> --group <name>
           [--from earliest|latest|<n>]
           [--auto-commit-interval <N>s]
           [--exit-at-end]
```

**参数与默认值**

| 参数 | 默认 | 说明 | 对应 R |
| --- | --- | --- | --- |
| `--topic` | 必填 | 订阅的 Topic（MVP 仅单 Topic） | R13 |
| `--group` | 必填 | Consumer Group | R13 |
| `--from` | `earliest`（待确认） | 仅在无已提交位点时生效 | R11 |
| `--auto-commit-interval` | `5s` | 自动提交间隔（原型假时钟 1 条=1 秒） | R6 |
| `--exit-at-end` | 关闭 | 读到当前末尾即退出；否则进入跟随 | R18 |

**启动位置（R11）**

1. 若该 `(group, topic, partition)` 有已提交位点 → 从已提交位点继续，`source=committed`；
2. 否则按 `--from`：`earliest` → 0；`latest` → 分区末尾；`<n>` → 指定 offset；
3. 因此 `--from` 永远不会覆盖已提交位点。

**stdout（消息记录，R19）**

```json
{"partition":0,"offset":0,"key":"k0","value":"hello-0"}
```

**stderr（生命周期事件）**

```json
{"event":"assigned","group":"g1","topic":"orders","partitions":[0,1,2,3]}
{"event":"start","partition":0,"offset":0,"source":"earliest"}
{"event":"commit","partition":0,"offset":1}
{"event":"follow","status":"waiting_for_new_messages"}
```

- `source` 取值：`committed` / `earliest` / `latest` / `offset`。
- 提交语义：消息成功打印后才推进内存位点；定时器（原型用假时钟）只持久化已处理位点。崩溃最多重复未提交区间（R6）。
- 原型中 `--exit-at-end` 关闭时，读到末尾打印 `follow` 事件后**立即返回**，不真正阻塞（见第 5 节限制）。

**错误**

| 错误码 | 触发条件 | 退出码 |
| --- | --- | --- |
| `topic_not_found` | Topic 不存在 | 3 |
| `invalid_group_name` | 组名不合法 | 3 |
| `invalid_from` | 非 earliest/latest/非负整数 | 3 |
| `invalid_interval` | 非正数秒 | 2 |

**对应 R**：R6、R10（MVP 静态分配）、R11、R13、R18、R19、R28（位点文件）。

---

### 2.6 `mk replay`

```
mk replay --topic <name> [--partition <n>] [--offset <n>] [--count <n>]
```

**参数与默认值**

| 参数 | 默认 | 说明 | 对应 R |
| --- | --- | --- | --- |
| `--topic` | 必填 | 目标 Topic | R12 |
| `--partition` | `0` | 指定分区 | R12 |
| `--offset` | `0` | 起始 offset（含） | R12 |
| `--count` | 不指定 | 输出条数；不指定则读到末尾 | R12 |

**stdout**：与 consume 相同的消息记录格式。

```json
{"partition":0,"offset":0,"key":"k0","value":"hello-0"}
```

**副作用**：无。不提交位点、不创建/修改任何 group 状态（R12）。

**错误**：`topic_not_found`、`invalid_partition`、`invalid_offset`、`invalid_count`（退出码 3 或 2）。

**对应 R**：R12、R19。

---

## 3. 演示剧本与覆盖对照

`prototype/demo.sh` 按以下场景运行（每步打印"正在做什么 / 预期看到"）：

| 场景 | 内容 | 覆盖 R |
| --- | --- | --- |
| 0 | 启动假 Broker | R1、R7、R15、R20 |
| 1 | 创建/重复创建/非法分区/列出 Topic | R14、R15、R21 |
| 2.1 | 单条消息 + 指定分区 | R19、R22 |
| 2.2 | 批量 + 相同 key 哈希稳定 | R19、R22、R23 |
| 2.3 | 批量 + 无 key 轮询 | R19、R22、R23 |
| 2.4 | 准备位点演示数据（offset-demo 5 条） | R19、R27 |
| 3.1 | earliest 首次消费 + 自动提交 + `--exit-at-end` | R6、R11、R18 |
| 3.2 | 新 group 从指定 offset 开始 | R11 |
| 3.3 | 新 group 用 latest（无已提交位点） | R11 |
| 3.4 | 已提交位点优先于 `--from latest` | R11 |
| 3.5 | 同 group 两个消费者静态分区分工 | R10（MVP）、R13 |
| 3.6 | 持续跟随（原型立即返回） | R18 |
| 4 | 按 offset 只读回放；回放后 group 进度不变 | R12、R11 |
| 5 | 提交间隔 + 中断重启，展示重复区间 | R6、R11、R28 |
| 6.1 | 磁盘满拒写；读路径可用 | R16、R1 |
| 6.2 | 单条消息超限 | R17 |
| 6.3 | 批量部分失败、分区阻塞、重试耗尽、对账汇总 | R3、R23、R24、R29 |
| 6.4 | Broker 崩溃后自动截断残缺尾部 | R7、R27 |

---

## 4. 原型仿真钩子（不属于最终 CLI）

以下机制只用于让原型可复现，**不得进入最终 CLI 契约**：

| 钩子 | 作用 | 说明 |
| --- | --- | --- |
| `MK_DEMO_SLOT=i/n` | 模拟 MVP 静态分区分配（消费者取 `p % n == i` 的分区） | 最终成员发现机制未定，见待确认 |
| `MK_DEMO_CRASH_AFTER=N` | 处理 N 条后模拟 `kill -9`，返回 137 | 用于展示 R6 的重复窗口 |
| `.demo-state/disk_full` | 模拟磁盘满 | 由 demo.sh 直接设置 |
| `.demo-state/free_slots` | 模拟剩余可写条数 | 用于批量部分失败 |
| `value = "OVERSIZE_DEMO"` | 模拟 1MB+1 字节消息 | 避免生成超大文本 |
| 假时钟 | 1 条消息 = 1 秒 | 让 `--auto-commit-interval` 在演示中可预测 |
| 跟随模式 | 打印 `follow` 事件后立即返回 | 原型不真正阻塞，避免 demo 挂起 |

**假数据内部格式**（原型自用，不是最终存储格式）：

- `prototype/.demo-state/topics/<topic>/meta`：分区数；
- `prototype/.demo-state/topics/<topic>/p<N>.log`：每行 `offset<TAB>key<TAB>value`；
- `prototype/.demo-state/groups/<group>/<topic>/offsets/<partition>`：已提交的下一条 offset；
- `prototype/fixtures/*.jsonl`：演示输入。

---

## 5. 原型限制与已知偏差

1. `mk` 是 demo.sh 内的 bash 函数，不是独立可执行文件；没有真实进程、TCP、磁盘或 C++。
2. 跟随模式不会真正阻塞等待新消息，只打印 `follow` 事件后返回。
3. 提交失败（R8）未在原型中模拟；原型假设提交总是成功。
4. JSON 解析只支持演示中的简单一行一个对象；不支持转义引号、嵌套对象、二进制或大消息。
5. 假磁盘满/剩余空间是进程内标记，不检查真实磁盘。
6. 崩溃恢复用"三字段 + 连续 offset"判断记录完整性，模拟需求中的"长度字段 + CRC"；不含真实 CRC。
7. 消费者完成正常退出时**不会强制补一次提交**（需求未规定，见待确认）。
8. 两个消费者的静态分配依赖 `MK_DEMO_SLOT`；最终如何获得组内成员/分区数尚未定义。

---

## 6. 本阶段发现的待确认项

> 以下仅记录，不修改 `docs/01.需求文档.md`。确认后再回写需求文档。

| 编号 | 待确认项 | 为什么重要 | 原型暂定 |
| --- | --- | --- | --- |
| Q-P1 | Consumer `--from` 的默认值：`earliest` 还是 `latest`？ | 影响首次消费与演示语义；R11 只规定优先级，未规定默认值 | `earliest` |
| Q-P2 | MVP 静态分配下，消费者如何得知组内成员数与自己的分区？ | R10 说"启动时静态分配"，但未定义成员发现/配置机制；原型用 `MK_DEMO_SLOT` 绕过 | 环境变量模拟 |
| Q-P3 | `--retries` 默认次数、退避间隔与总超时如何定？ | R3 只说可配置；默认值影响失败恢复体验 | 3 次、无退避 |
| Q-P4 | 自动提交间隔默认值？ | R6 只说可配置；间隔决定崩溃后的最大重复量 | `5s` |
| Q-P5 | 按键哈希使用什么算法、是否要求跨进程/跨版本稳定？ | R22 只要求"按 key 哈希"；算法决定同 key 的分区归属 | FNV-1a 32-bit |
| Q-P6 | 消费者正常退出（Ctrl+C / 读到末尾）时是否强制提交一次？ | 影响重复窗口；R6 只规定"处理后推进、定时持久化" | 不强制提交 |
| Q-P7 | `--from <n>` 与 `--offset <n>` 的合法边界：等于末尾是否允许？超过末尾如何提示？ | 影响 CLI 报错与验收 | 允许等于末尾；超过末尾报错 |
| Q-P8 | Topic 名称规则、长度上限、保留字？ | 当前原型仅 `[A-Za-z0-9_-]+`；需求未规定 | 该正则 |
| Q-P9 | 错误码/退出码体系统一约定？ | 原型提出的 0/2/3/4 需确认后才能作为验收依据 | 0/2/3/4 |
| Q-P10 | `mk broker` 最终是前台常驻还是后台 daemon？默认端口/数据目录？ | 影响部署与验收脚本；R20 只要求参数化 | 前台模拟、9092、`.demo-state` |
| Q-P11 | R16 的"磁盘安全阈值"具体是多少？ | 决定何时拒写；原型用假标记 | 未模拟具体字节 |
| Q-P12 | R29 的对账汇总输出到 stdout 还是 stderr、用什么格式？ | 影响脚本对账；需求只要求"输出汇总" | stdout JSON 事件 |
| Q-P13 | `produce --partition` 作用于批量时，是否所有条目都写指定分区？ | 原型按"全部写指定分区"实现；需求未明确 | 全部写指定分区 |
| Q-P14 | R19 的消息 value 是否只支持 UTF-8 文本？是否需要 base64/二进制与转义规则？ | 影响 CLI 输入输出与验收数据 | 仅简单 JSON 字符串 |

---

## 7. 与需求文档的对应关系（汇总）

| 约定 | 对应 R |
| --- | --- |
| 单二进制 + 子命令 | R15 |
| JSON Lines 输入/输出 | R19 |
| 命令行参数 + 默认值，无配置文件 | R20 |
| 不提供 Topic 删除、组/位点查询 | R21 |
| 分区数 1–16、创建后不可修改 | R14 |
| 指定分区 > key 哈希 > 轮询 | R22 |
| 批量逐条返回结果 | R23 |
| 失败条目分区级阻塞重试 | R24 |
| 重试耗尽明确失败退出 | R3 |
| 单条消息 1MB 上限 | R17 |
| 磁盘满拒写、读路径可用 | R16 |
| Consumer Group、单 Topic | R13 |
| 已提交位点优先于起始位置 | R11 |
| 自动提交以"已处理"为准 | R6 |
| 默认跟随、`--exit-at-end` | R18 |
| MVP 启动时静态分区分配 | R10（MVP） |
| 只读回放、无副作用 | R12 |
| offset 逻辑序号与重启重建 | R27 |
| 位点文件与消息同可靠性口径 | R28 |
| 崩溃恢复截断残缺尾部 | R7 |
| ack 持久化边界（原型不直接体现） | R1 |
| 性能验收（原型不直接体现） | R4、R5、R9、R26 |
| MVP 排除项 | R25 |

---

## 8. 运行与复现

```bash
# 在仓库根目录（Windows Git Bash）
bash prototype/demo.sh
```

- 依赖：bash、coreutils（cat/cut/sort/uniq/tr/tail/wc/touch/rm/mv）、sed、grep、awk；不需要 jq、编译或 Linux 专有命令。
- 每次运行会重置 `prototype/.demo-state/`；脚本最后退出码为 0。
- 输出顺序固定：分区升序、offset 升序、场景顺序固定；无时间戳/随机值。
