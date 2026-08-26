# Design: deepen-javadoc-contract-detail

## Context

上次变更（`2026-08-26-normalize-javadoc-and-license`）已归档，完成标签覆盖度、协议头与 javadoc 构建防回归（maven-javadoc-plugin 3.12.0，`doclint=all`、`failOnWarnings`、绑定 `verify`，未设 `<show>` 故默认 `-protected` 扫描范围）。本次以 JDK `java.util.concurrent.locks.Lock` 的契约级注释为基准，对 30 个主源文件逐一审核，结论：

- **✗ 不达标（4）**：`CoreEngine`（门面=本项目的 Lock 接口，类级 2 行）、`LockEntry`（`acquire` 规则外包给 §4.3）、`LockType`（兼容性/重入语义缺失）、`ServerSessionHandler`（生命周期状态机只在内联注释）
- **◐ 部分达标（3）**：`Outcome`/`ReleaseStatus`（缺返回优先级契约）、`OpenLatchServer`（缺线程模型）
- **✓ 维持简洁（23）**：已达 JDK 对简单类/数据载体的简洁档位，见附录 A
- **private 成员**：17 个方法 + 1 个构造器完全无 Javadoc，见附录 B；且不在构建校验范围内

关键基准认知：JDK 注释深度与契约分量成正比——`Lock` 接口约 150 行（内存语义、调度、重入、中断/超时获取契约、示例），而 `ConcurrentHashMap.get` 仅两三行。本次采用同一比例原则，不统一制造深度。

约束：JDK 25、Maven 构建、统一 `-s /home/lam/repo/settings.xml`；注释语言保持中文；`openlatch-protocol` 主源码为生成代码不触碰。

## Goals / Non-Goals

**Goals:**

- 契约类 Javadoc 自足：只读注释即可理解契约；设计文档引用降级为深入指引
- 全部 17 个 private 方法与 1 个 private 构造器有详细 Javadoc
- private 成员注释纳入构建期校验（`<show>private</show>`）
- 48 个文件全部经过逐一判定，"维持简洁"亦有记录
- 零行为变化：不改动任何代码逻辑

**Non-Goals:**

- 不动 18 个测试文件（无外部契约，已满足上轮标准）
- 不深度重写 23 个已达标简单类（仅确认；发现标签小疵可顺手修）
- 不引入 checkstyle 等其他风格工具
- 不整改 protobuf 生成代码

## Decisions

### D1｜契约分量两级标准

| 档位 | 适用 | 标准 |
|---|---|---|
| 契约级（JDK Lock 接口级） | 承载行为契约、并发保证、协议可见语义的类与方法 | 类级写职责、线程模型、状态机、契约边界；方法级写分支语义（各返回值的含义与判定顺序）、幂等性、可见性、调用者义务 |
| 简洁级（JDK `get` 级） | 简单访问器、数据载体 | 一句话职责 + 必要标签，不制造深度 |

判定依据 = 调用者可依赖的行为保证数量。探索阶段已完成 30 个主源文件的全量判定（上文 Context），实施时对"维持简洁"文件仅做确认，不重写。

### D2｜契约自足原则

- 规则本身写进 Javadoc（§4.3 规则集明细、握手处理矩阵、惰性到期语义等），读懂注释即懂契约
- 设计说明书引用保留为"详见设计说明书 §x.y"的深入指引，不再是理解前提；文档改名/移动不使注释悬空
- 推翻上次非目标"不重写现有措辞正确的类级注释"，仅限本次深化的 7 个文件

### D3｜private 成员标准与强制机制

- private 方法必须有详细 Javadoc：行为意图、`@param`/`@return` 语义、副作用、线程/锁上下文（如"须在持有条目锁时调用"）
- private 构造器写一句用途说明（`ServerBootstrapFactory` 的工具类禁实例化）
- **（实施修正 2026-08-26）** private 字段与常量须有一句话注释：实施发现 `doclint=all` 的 `missing` 类别对 `show=private` 范围内的**所有**成员强制要求注释存在（含字段），否则 `no comment` 告警使构建失败。补齐字段注释与项目既有风格一致（`LockEntry` 字段本就有注释），record 的合成字段不受影响
- 包私有成员维持上次标准（不强制）；`RequestDispatcher` 的 6 个包私有映射方法已有完整 Javadoc，不动
- 强制机制：根 `pom.xml` maven-javadoc-plugin 增加 `<show>private</show>`，private 成员的注释存在性与合法性（标签、`{@link}`、HTML）均纳入 doclint；**"详细程度"本身仍靠约定与评审维持**（doclint 不判断内容深度）
- `@Override` 方法维持豁免

### D4｜各文件深化内容大纲（实施防失真清单）

写注释前必须读对应实现；分支语义以代码为准，必要时对照测试与规格。

**`CoreEngine`**
- 类级：职责；线程模型（多个 Netty IO 线程 + 租约扫描线程并发调用，安全性基于条目锁与并发容器）；事件回调在条目锁外触发
- `acquire`：校验顺序与各 `REJECT_*` 返回条件；`requestedLeaseMs=0` 取默认、夹取 `[min, max]`；重入语义（计数+1、同 token、租约整段刷新）；`QUEUED` 位次 1 起；同 `(sessionId, requestId)` 幂等去重
- `release`：token 校验为权威凭证；`fullyReleased` 含义；对新队首触发通知
- `renew`：`NOT_HELD`/`INVALID_TOKEN` 判定；成功后刷新到期堆
- `sessionClosed`：幂等；释放全部持锁并摘除全部等待项
- `expireDue`：**惰性到期契约**——锁到期不立即释放，由扫描调用回收；陈旧校验（堆记录凭证与到期时刻须与条目当前值一致）保证续租后旧堆记录不误杀
- `sweepNotifiedHeads`：移除超时未重发的已通知队首并补通知新队首
- private：`clampLease`（0→默认、夹取上下限）、`fireNotify`（条目锁外统一触发回调）

**`LockEntry`**
- 类级：保留现有并发模型段（最多持一个条目锁、通知经参数收集），补状态机总述（持有/租约/等待队列三要素）
- `acquire`：规则顺序自足写出——写/读重入刷新 → 快路径（读锁加入已有读者、复用凭证）→ 队首幂等重发 → `queueIfBusy=false` 则 `DENIED` → 同 `(会话, 请求)` 去重返回当前位次 → 队列满 `REJECT_QUEUE_FULL` → 入队；读锁共享单一租约凭证的原因（避免新 token 使旧读者释放失效）
- `release`：判定顺序——无持有者 `NOT_HELD` → 凭证不匹配 `INVALID_TOKEN` → 写侧/读侧计数递减 → 归零清租约并推进队首
- `forceExpire`/`sweepNotifiedHead`/`removeSession`：触发时机与副作用
- private：`grant`（授予落库）、`clearLease`（租约归零）、`notifyHeadIfPossible`（仅队首、不批量唤醒、标记截止时刻）、`compatibleWithHold`（队首重发兼容性判定）

**`LockType`**
- 类级：兼容性矩阵（READ 相互兼容；与 SIMPLE/REENTRANT/WRITE 互斥）、可重入归属（SIMPLE 不可重入，其余可重入）、"同一 key 应使用一致锁类型"约定（建条目时由首次请求定型）
- 各常量补语义与其在条目中的映射

**`ServerSessionHandler`**
- 类级：连接生命周期状态机（未握手 → 握手门闩 → 业务 → 断连清理）；处理矩阵：握手前业务请求拒绝不断连、畸形 HELLO 拒绝不断连、重复 HELLO 拒绝不断连、版本不匹配或携带认证令牌拒绝**且**断连、超限回 `OVERLOADED`、`PING` 不回复、读空闲断连；`markClosed` 保证只清理一次；清理顺序（先摘注册表使通知不再路由，后清会话）及原因
- private：`handleHandshake`（门闩全流程）、`helloResponse`（握手响应构造与回显字段）

**`Outcome` / `ReleaseStatus`**
- 各常量补"在哪一校验步骤、以什么优先级返回"；`ReleaseStatus.OK` 在释放/续租两种语境下的含义

**`OpenLatchServer`**
- 类级补线程模型：业务逻辑在 IO 线程同步执行、租约扫描独立单线程、通知回调可能来自扫描线程

### D5｜实施与验证顺序

1. 摸底：`mvn -s /home/lam/repo/settings.xml clean verify` 确认当前全绿
2. 加 `<show>private</show>`，单模块 `verify` 确认未引入存量告警
3. core 深化 → 4. server 深化 → 5. 逐一确认 23 个简洁文件与 18 个测试文件
6. 全量 `mvn -s /home/lam/repo/settings.xml clean verify` 收尾
7. 反向验证：临时破坏任一 private 方法注释标签，`verify` 应失败，随后恢复

## Risks / Trade-offs

- [深化 = 复述行为，注释失真] → 写前必读实现，`Outcome`/`ReleaseStatus` 分支逐条对照代码；以代码语义为准，必要时对照测试与规格
- [`show=private` 把字段与既有单行注释纳入 doclint 范围，可能报出未预料的存量告警] → 摸底先行，存量告警与深化工作一并清理
- [doclint 不校验"详细程度"，私有注释的详细度只能靠约定维持] → 接受；引入 checkstyle 的 Javadoc 规则表达力弱，与上次非目标一致不引入
- [7 个文件集中重写，评审量大] → tasks 按文件拆分，每模块独立 `verify`，可分次合入

## 附录 A｜维持简洁的 23 个主源文件（逐一判定记录）

判定理由统一为：无行为契约或契约已在类级/组件注释中写清，处于 JDK 对简单类/数据载体的简洁档位。

| # | 文件 | 判定摘要 |
|---|---|---|
| 1 | `core/SystemClock` | 单方法委托实现，类级已说明 |
| 2 | `core/Clock` | 单方法接口，契约一句话已足 |
| 3 | `core/CoreEventListener` | 单方法接口，触发时机已写明 |
| 4 | `core/CoreConfig` | record 组件与常量标签完整 |
| 5 | `core/session/SessionRegistry` | `touchIfPresent` 原子契约已写清（范本级） |
| 6 | `core/lease/LeaseManager` | "只入不删 + 陈旧校验"契约已写清 |
| 7 | `core/lock/LockTable` | 创建/移除竞态契约已写清 |
| 8 | `core/lock/Waiter` | 双状态语义（`notifyDeadlineMs`）与不可变替换已写清 |
| 9 | `core/lock/Owner` | 纯数据载体 |
| 10 | `core/command/AcquireCommand` | 组件标签完整，含 `queueIfBusy` 协议映射 |
| 11 | `core/command/ReleaseCommand` | 组件标签完整 |
| 12 | `core/command/RenewCommand` | 组件标签完整 |
| 13 | `core/result/AcquireResult` | 组件标签含有效性条件 |
| 14 | `core/result/ReleaseResult` | 组件标签完整 |
| 15 | `core/result/RenewResult` | 组件标签完整 |
| 16 | `server/ServerConfig` | 组件/常量/`load` 契约完整（4 个 private 方法另行补注释，见附录 B） |
| 17 | `server/NotifyEventBridge` | 线程来源、静默丢弃、兜底契约已写清 |
| 18 | `server/session/ServerSession` | inflight 与 `markClosed` 契约已写清 |
| 19 | `server/session/ServerSessionRegistry` | 简单反查表 |
| 20 | `server/dispatch/RequestDispatcher` | 纯映射契约已写清，状态码全表映射有标签（4 个 private 方法另行补注释，见附录 B） |
| 21 | `server/net/ServerChannelInitializer` | pipeline 图与出站顺序原理已写清 |
| 22 | `server/net/EnvelopeCodecHandler` | 守卫边界（编解码失败断连 / 语义非法不断连）已写清 |
| 23 | `server/net/ServerBootstrapFactory` | 参数与返回值已写清（private 构造器另行补注释，见附录 B） |

## 附录 B｜private 成员清单（17 方法 + 1 构造器）

| 文件 | 成员 |
|---|---|
| `core/CoreEngine` | `clampLease`、`fireNotify` |
| `core/lock/LockEntry` | `grant`、`clearLease`、`notifyHeadIfPossible`、`compatibleWithHold` |
| `server/ServerConfig` | `defaultWorkerThreads`、`intOf`、`longOf`、`validate` |
| `server/OpenLatchServer` | `startScheduler` |
| `server/dispatch/RequestDispatcher` | `dispatchAcquire`、`dispatchRelease`、`dispatchRenew`、`envelope` |
| `server/net/ServerSessionHandler` | `handleHandshake`、`helloResponse` |
| `server/net/ServerBootstrapFactory` | private 构造器 |
