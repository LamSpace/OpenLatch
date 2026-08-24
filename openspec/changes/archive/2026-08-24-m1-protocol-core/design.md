# Design: m1-protocol-core

## Context

仓库目前是单模块裸 `pom.xml`（无 packaging/modules，`src/` 为空目录），尚无任何协议或锁语义代码。本机环境的关键约束：

- Java 25.0.3、Maven 3.9.16；Maven 配置固定为 `-s /home/lam/repo/settings.xml`，本地仓库 `/home/lam/repo`，central 镜像为 aliyun；
- 本地仓库已缓存：compiler 3.15.0、surefire 3.5.4、clean/resources/jar/install、JUnit Jupiter 5.11.4 + platform 1.11.4、AssertJ 3.27.7；**未缓存任何 protobuf 相关工件**；
- 无系统 protoc；平台为 linux-x86_64；用户已授权构建期联网经 aliyun 拉取 Maven 依赖。

行为需求见两个 delta spec；动机见 proposal.md。设计说明书 §2/§3/§4/§10/§13.1 是本设计的上位依据，本文只记录实现层面的取舍。

## Goals / Non-Goals

**Goals:**

- 交付 `openlatch-protocol` 与 `openlatch-core` 两个模块，使 `mvn verify` 全绿且 §10.1/§10.2 属 M1 的用例组全部覆盖；
- 插件与依赖版本全部显式锁定，构建可复现，不引入计划外下载；
- core 的并发模型从第一天可论证：单条目锁、无锁序、回调锁外。

**Non-Goals:**

- 不实现 §10.2 中属于服务端行为的部分（type/payload 不匹配时回 `INVALID_REQUEST` 等，归 M2/P1-14）；
- 不实现 §5.5 的幂等响应回放窗口（YAGNI，§4.8 的队列内去重已覆盖 Phase 1 场景）；
- 不做性能优化（批量唤醒、通知头全局索引等，§12 已列入后续阶段）。

## Decisions

### D1：六模块聚合 + 版本锁定

根 `pom.xml` 改 `packaging=pom`，声明六个模块；M2–M4 的四个模块先建占位 pom 与空包目录，使 §2 的依赖约束从 M1 起即可在构建中检查。`pluginManagement` 将插件锁定到本地已缓存版本（compiler 3.15.0、surefire 3.5.4、jar 3.5.0、clean 3.5.0、resources 3.4.0、install 3.1.4），Java 版本用 `maven.compiler.release=25`。

**备选**：M1 只建 protocol 与 core 两模块，后续再加。弃用原因：每次加模块都要动根 pom，且 §13.1 P1-01 的验收本就要求六模块结构与依赖关系成立。

删除根目录空 `src/` 树（用户已确认）：聚合 pom 不编译根源码，留着只会误导。

### D2：Protobuf 代码生成走 protobuf-maven-plugin + protocArtifact

`openlatch-protocol` 用 `kr.motd.maven:os-maven-plugin:1.7.1` 探测平台，`org.xolstice.maven.plugins:protobuf-maven-plugin:0.6.1` 的 `protocArtifact` 从仓库拉取 `com.google.protobuf:protoc:3.25.5:exe:linux-x86_64` 并在 `generate-sources` 阶段生成代码；运行时库取同版本 `protobuf-java:3.25.5`（设计说明书 §1.2 要求 3.x，3.25.5 为 3.x 末版）。用户已授权联网拉取。

**备选**：系统安装 protoc + `protocExecutable`。弃用原因：本机无 protoc，且二进制路径不可移植、无法复现构建。
**备选**：手工生成一次并把生成代码入库。弃用原因：协议演进时需手工重生成，生成物入库污染 diff，违背"构建可复现"。

### D3：`AcquireCommand` 增加 `queueIfBusy` 标志（对 §4.2 的已确认修正）

§4.4 规则 6 需要区分"可排队"与"立即式"，但 §4.2 的命令签名不含 `wait_ms`。选择加 `boolean queueIfBusy`（server 层由 `wait_ms == 0` 映射）而非透传 `wait_ms`：core 对 `-1` 与 `>0` 语义等价（§3.2.2），透传时长会诱使 core 做本地计时，违背"服务端无定时器负担"的设计（§3.2.2 后注）。

**备选**：server 层在调用 core 前自行判断忙闲、直接回 `DENIED`。弃用原因：忙闲判定必须与入队判定在同一临界区内原子完成，前置判断会与并发释放产生竞态。

### D4：并发模型——条目级锁 + 成员回查重试

- `ConcurrentHashMap` 承载锁表，`computeIfAbsent` 建条目；
- 单条目全部状态迁移在 `synchronized(entry)` 内，任何路径最多持一个条目锁；
- **条目销毁竞态的补强**：§4.9.1 只写了"二次检查后 `remove(key, entry)`"。调用方在条目锁内还需回查 `table.get(key) == entry`，不匹配则解锁重试——否则并发线程可能持旧引用在孤儿条目上入队。此模式写入代码注释并配并发测试；
- `CoreEventListener` 回调统一"锁内收集、锁外触发"；
- `LeaseManager` 独立自同步；`expireDue` 先在堆锁内取走到期项，再逐个取条目锁，避免交叉持锁。

**备选**：全局锁或分段锁。弃用原因：§4.1 明确"无全局锁"，条目锁方案临界区短且无锁序。

### D5：租约堆只入不删 + 陈旧校验

授予/续租向最小堆推入 `(expiresAt, key, leaseToken)`；释放与覆盖不删堆，`expireDue` 弹出时回查条目当前凭证与到期时刻，不一致即丢弃。释放路径因此零堆操作，堆锁与条目锁从不嵌套。陈旧记录数量与锁变更频率同阶（§4.6 已论证）。

**备选**：释放时精确删堆（持有堆节点句柄）。弃用原因：要么引入跨条目锁/堆锁的删除路径，要么用延迟删除队列增加簿记，收益不抵复杂度。

### D6：队首响应超时清扫用全表扫描

`sweepNotifiedHeads` 每 500ms 遍历锁表、只处理"队首 `notifyDeadlineMs > 0`"的条目。Phase 1 接受 O(表大小)/tick；表规模在单机场景下有界。

**备选**：维护"已通知队首"的全局时间索引。弃用原因：多一份跨锁簿记与失效同步负担，属 §12 精神的过早优化。

### D7：测试策略——手工时钟 + 记录型监听器

core 测试不使用 `sleep`、不起线程池：`Clock` 用测试手工实现（`advance(ms)`），`CoreEventListener` 用记录型实现（断言通知的会话/请求/键序列）。§10.1 十组用例直接映射为测试类；并发正确性（D4 的重试路径）用少量多线程压力用例覆盖，断言不变量（授予顺序、无丢失、无重复入队）而非时序。

协议测试：round-trip 用"构造 → `toByteArray` → `parseFrom` → 逐字段比对"；未知字段用 `UnknownFieldSet` 附加后验证解码与再序列化保留。

## Risks / Trade-offs

- **[protobuf 3.25.5 生成代码在 Java 25 下的兼容性]** → P1-02 作为首个接触点尽早验证；若生成代码编译失败，回退方案是升级到 4.x 并修订设计说明书 §1.2（需用户确认），不静默变更。
- **[条目销毁竞态修复不彻底导致孤儿等待者]** → D4 的成员回查重试 + 并发测试显式构造"移除与创建交叉"场景。
- **[aliyun 镜像缺少 `protoc` 的 exe 工件]** → 构建首次失败即可暴露；回退方案：用户手工下载 protoc 发行包，插件配置切换为 `protocExecutable` 指向本地路径。
- **[堆内陈旧记录在高换锁频率下积累]** → 与锁变更同阶、到期扫描顺带清理（§4.6）；Phase 1 规模下可接受，基准测试（M4）观察。
- **[全表扫描在超大锁表下的 tick 开销]** → Phase 1 单机场景可接受；若 M4 基准暴露问题，再引入通知头索引（不改 spec，仅实现优化）。
- **[占位模块拉高首次构建噪音]** → 占位模块仅空 pom，不配置插件与依赖，保持最小。

## Migration Plan

绿地改造，无存量行为需迁移。实施顺序即 tasks.md 的 P1-01 → P1-10；任一子任务失败只影响其后续依赖项，可单步回退（git 提交按子任务粒度组织）。回滚策略：M1 全部为新增文件与根 pom 改造，`git revert` 对应提交即可完整回退。
