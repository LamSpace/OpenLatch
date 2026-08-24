# OpenLatch Phase 1（MVP）详细设计说明书

| 项目     | 内容                                                                       |
|----------|----------------------------------------------------------------------------|
| 项目名称 | **OpenLatch**                                                              |
| 文档类型 | 详细设计说明书（Phase 1 / MVP）                                            |
| 依据文档 | 《OpenLatch 概要设计说明书》v1.0、《OpenLatch-总体实施计划与验证方案》v1.0 |
| 版本     | v1.0                                                                       |
| 日期     | 2026-08-23                                                                 |
| 作者     | Lam Tong                                                                   |
| 状态     | 待评审                                                                     |

---

## 1. 概述

### 1.1 范围

本文档覆盖 Phase 1（MVP）全部交付内容，对应实施计划里程碑 M1–M4：

| 模块                            | 里程碑 | 内容                                               |
|---------------------------------|--------|----------------------------------------------------|
| `openlatch-protocol`            | M1     | `.proto` 协议定义与编解码                          |
| `openlatch-core`                | M1     | 纯 Java 锁语义核心（状态机、等待队列、租约、会话） |
| `openlatch-server`              | M2     | Netty 单节点服务器                                 |
| `openlatch-client`              | M3     | 客户端 SDK（异步内核 + 同步包装 + 看门狗 + 重连）  |
| `openlatch-spring-boot-starter` | M4     | Spring Boot 3 自动装配 + 注解 + AOP                |
| `openlatch-examples`            | M4     | 示例与基准测试                                     |

**不在范围内**：Raft 集群（Phase 2）、FairLock/Semaphore/CountDownLatch、监控、控制台、TLS/认证（Phase 3）。协议为这些能力预留字段，但不实现。

### 1.2 技术基线

| 项           | 取值                  | 说明                                                 |
|--------------|-----------------------|------------------------------------------------------|
| Java         | 25                    | 仓库现状；与概要设计（17）的差异已在实施计划 §2 记录 |
| groupId      | `io.github.lamspace`  | 包名根 `io.github.lamspace.openlatch`                |
| Netty        | 4.1.x 当前稳定版      |                                                      |
| Protobuf     | protobuf-java 3.x     |                                                      |
| Spring Boot  | 3.5.x（starter 模块） | 需按实施计划风险 6 验证 Java 25 兼容性               |
| 默认服务端口 | 9410                  | 可配置                                               |

### 1.3 与概要设计的追溯矩阵

| 概要设计章节                    | 本文对应章节 |
|---------------------------------|--------------|
| §5.2 Maven 模块结构             | §2           |
| §6.1 协议设计                   | §3           |
| §6.2 锁获取与等待模型           | §4.5、§5.5   |
| §6.3 租约与看门狗               | §4.6、§6.6   |
| §6.4 会话管理                   | §4.7、§5.4   |
| §6.5 锁类型                     | §4.4         |
| §6.6 客户端 API                 | §6.3         |
| §7 可靠性与错误处理（单机部分） | §7           |
| §8 测试策略（Phase 1 部分）     | §10          |
| §4.3 成功标准                   | §11          |

---

## 2. 模块结构与依赖

```
openlatch-protocol   ← 无依赖（仅 protobuf）
      ▲
openlatch-core       ← 无依赖（纯 Java，不使用 protocol/Netty）
      ▲
openlatch-server     ← core + protocol + netty，可执行 jar
openlatch-client     ← protocol + netty（不依赖 core——语义判定全部在服务端）
      ▲
openlatch-spring-boot-starter ← client
openlatch-examples   ← 全部（不发布到 Maven Central）
```

设计约束：

1. **core 不依赖 protocol**：core 使用自己的命令/结果 record，由 server 层做 protocol ↔ core 的映射。保证核心语义可脱离协议演进独立测试（概要设计 §11 风险 5）。
2. **client 不依赖 core**：客户端不做锁语义判定（是否授予、是否可重入全部由服务端裁决），客户端只维护本地簿记（看门狗、future 关联）。防止双份语义实现漂移。

各模块包结构：

```
io.github.lamspace.openlatch.protocol      协议生成代码
io.github.lamspace.openlatch.core          锁表 / 等待队列 / 租约 / 会话
io.github.lamspace.openlatch.server        服务端启动、配置
  .server.net                              Netty pipeline 与 handler
  .server.dispatch                         请求分发与幂等
  .server.session                          服务端会话簿记
io.github.lamspace.openlatch.client        客户端入口与公开 API
  .client.internal                         多路复用、等待跟踪、看门狗、重连
io.github.lamspace.openlatch.spring        starter：自动装配、注解、切面
```

---

## 3. openlatch-protocol 详细设计

### 3.1 传输与分帧

TCP 长连接。帧格式：`4 字节大端长度 + Protobuf 序列化的 Envelope`。

| 参数     | 值                  | 说明                                                             |
|----------|---------------------|------------------------------------------------------------------|
| 最大帧长 | 1 MiB               | 超限断连（防御异常大 key/消息）                                  |
| 长度字段 | 32 位大端，不含自身 | Netty `LengthFieldPrepender(4)` / `LengthFieldBasedFrameDecoder` |

### 3.2 Envelope 与消息定义

所有消息封装在统一 `Envelope` 中；`request_id` 在 **单条连接内唯一**，响应与推送按它关联（概要设计 §6.1）。

```proto
syntax = "proto3";

package openlatch;

option java_package = "io.github.lamspace.openlatch.protocol";
option java_multiple_files = true;

enum MessageType {
  MESSAGE_TYPE_UNKNOWN = 0;
  HELLO          = 1;   // 会话建立（连接后第一条消息）
  LOCK_ACQUIRE   = 2;
  LOCK_RELEASE   = 3;
  LEASE_RENEW    = 4;
  PING           = 5;   // 心跳，无 payload
  AWAIT_NOTIFY   = 6;   // 服务器 → 客户端 推送
}

enum LockType {
  LOCK_TYPE_REENTRANT = 0;   // 默认：可重入互斥
  LOCK_TYPE_SIMPLE    = 1;   // 不可重入互斥
  LOCK_TYPE_READ      = 2;
  LOCK_TYPE_WRITE     = 3;
}

enum StatusCode {
  OK              = 0;
  QUEUED          = 1;   // 已登记到等待队列
  DENIED          = 2;   // 未排队直接拒绝（立即式 tryLock 且锁被占）
  INVALID_TOKEN   = 3;   // leaseToken 不匹配（误释放/误续租）
  NOT_HELD        = 4;   // 该会话未持有该锁
  SESSION_EXPIRED = 5;   // 会话不存在（重连后的旧请求）
  OVERLOADED      = 6;   // 触发服务端保护限额
  KEY_TOO_LONG    = 7;
  KEY_EMPTY       = 8;
  INVALID_REQUEST = 9;   // 参数非法（如 lease 越界）
  NOT_LEADER      = 10;  // Phase 2 预留，Phase 1 不使用
  INTERNAL_ERROR  = 11;
}

message Envelope {
  int32       protocol_version = 1;   // Phase 1 固定为 1
  MessageType type               = 2;
  int64       request_id         = 3; // 服务端推送（AWAIT_NOTIFY）时为 0
  oneof payload {
    HelloRequest        hello_request        = 10;
    HelloResponse       hello_response       = 11;
    AcquireRequest      acquire_request      = 12;
    AcquireResponse     acquire_response     = 13;
    ReleaseRequest      release_request      = 14;
    ReleaseResponse     release_response     = 15;
    LeaseRenewRequest   lease_renew_request  = 16;
    LeaseRenewResponse  lease_renew_response = 17;
    AwaitNotify         await_notify         = 18;
  }
}
```

#### 3.2.1 会话建立

```proto
message HelloRequest {
  int32  client_protocol_version = 1;
  string client_name             = 2;  // 诊断用（应用名/主机），可为空
  string auth_token              = 3;  // Phase 3 预留，Phase 1 必须为空
}

message HelloResponse {
  StatusCode status                = 1;
  int64      session_id            = 2;  // 服务端分配，连接生命周期内有效
  int32      server_protocol_version = 3;
  int64      default_lease_ms      = 4;
  int64      leader_hint           = 5;  // Phase 2 预留
}
```

握手规则：

- 连接建立后，客户端必须先发 `HELLO`；服务端在收到合法 `HELLO` 前，对任何业务请求回 `INVALID_REQUEST`；
- `client_protocol_version != 1` 时，服务端回 `INVALID_REQUEST` 并断连（不做隐式兼容，版本协商在 Phase 2 一并设计）；
- `session_id` 为服务端 `ThreadLocalRandom` 生成的 long，连接断开即作废； **重连必然得到新 session**（概要设计 §6.4）。

#### 3.2.2 获取与释放

```proto
message AcquireRequest {
  string   key       = 1;
  LockType lock_type = 2;
  int64    thread_id = 3;   // 客户端提供；与 session_id 共同构成锁归属
  int64    lease_ms  = 4;   // 0 = 用服务端默认值；越界返回 INVALID_REQUEST
  int64    wait_ms   = 5;   // -1 = 排队（受客户端总超时约束）；0 = 立即式；>0 由客户端本地计时，语义等价于 -1
}

message AcquireResponse {
  StatusCode status              = 1;  // OK=授予 / QUEUED=排队 / DENIED=拒绝 / 错误码
  int64      lease_token         = 2;  // OK 时有效：解锁与续租凭证
  int64      lease_expires_at_ms = 3;  // 服务端时钟，仅诊断参考
  int32      queue_position      = 4;  // QUEUED 时有效
  int64      granted_lease_ms    = 5;  // OK 时实际生效租约（客户端据此设定看门狗）
}

message ReleaseRequest {
  string key         = 1;
  int64  lease_token = 2;
  int64  thread_id   = 3;
}

message ReleaseResponse {
  StatusCode status          = 1;  // OK / INVALID_TOKEN / NOT_HELD
  bool       fully_released  = 2;  // 可重入计数归零时为 true
}
```

`wait_ms > 0` 的限时等待由 **客户端**本地计时（到点取消 future、发送取消排队请求见 §4.8），服务端不需要知道等待时限——服务端只管队列与通知。这使服务端保持无定时器负担，且与"客户端所有请求路径带超时"（概要设计 §4.3 标准 3）天然一致。

#### 3.2.3 续租与通知

```proto
message LeaseRenewRequest {
  string key         = 1;
  int64  lease_token = 2;
  int64  lease_ms    = 3;   // 请求续到的时长（通常等于原租约）
}

message LeaseRenewResponse {
  StatusCode status              = 1;  // OK / INVALID_TOKEN / NOT_HELD
  int64      lease_expires_at_ms = 2;
}

message AwaitNotify {
  string key           = 1;
  int64  request_id_ref = 2;  // 指向原 ACQUIRE 的 request_id
}
```

`AWAIT_NOTIFY` 是服务端推送（`Envelope.request_id = 0`），客户端按 `request_id_ref` 找到挂起的获取请求并 **以同一 `request_id` 重发 ACQUIRE**（概要设计 §6.2），配合服务端幂等去重（§4.8）保证不二次排队。

### 3.3 编码与版本约定

- 生成代码使用 `java_multiple_files`，避免单文件巨型类；
- 未知字段容忍：双方均不得因收到带未知字段的 Protobuf 消息而报错（proto3 默认保留未知字段，测试用例覆盖，见 §10）；
- `protocol_version` 不匹配的处理见 §3.2.1；
- `PING` 无 payload，双向均可发起；对端收到后仅作为空闲检测的活动信号，不需要应答。

---

## 4. openlatch-core 详细设计

### 4.1 设计目标

1. **纯 Java、零外部依赖、无网络**：全部锁语义在此闭环测试（概要设计 §4.2 可维护性目标）；
2. 线程安全，无全局锁：锁表用 `ConcurrentHashMap`，单 key 的状态迁移在该 key 的条目对象上同步；
3. 时间可注入：通过 `Clock` 接口，测试中用手工时钟推进租约，无需 `sleep`。

### 4.2 包与类总览

```
core/
├── CoreEngine            门面：acquire / release / renew / sessionOpened / sessionClosed / expireDue
├── CoreConfig            限额与默认租约配置（record）
├── Clock                 时间源接口；实现：SystemClock（生产）、测试用手工时钟
├── CoreEventListener     事件出口接口（通知队首等），由 server 层实现
├── command/
│   ├── AcquireCommand    (sessionId, requestId, key, LockType, threadId, requestedLeaseMs)
│   ├── ReleaseCommand    (sessionId, key, leaseToken, threadId)
│   └── RenewCommand      (sessionId, key, leaseToken, requestedLeaseMs)
├── result/
│   ├── AcquireResult     (Outcome, leaseToken, grantedLeaseMs, queuePosition)
│   ├── ReleaseResult     (status, fullyReleased)
│   ├── RenewResult       (status, newExpiresAtMs)
│   └── Outcome           枚举：GRANTED / QUEUED / DENIED / REJECT_KEY / REJECT_QUEUE_FULL / REJECT_SESSION
├── lock/
│   ├── LockTable         key → LockEntry 的映射与条目生命周期
│   ├── LockEntry         单 key 状态：持有者、计数、租约、等待队列（条目内同步）
│   └── Waiter            record(sessionId, requestId, lockType, threadId, enqueuedAtMs, notifyDeadlineMs)；
│                          notifyDeadlineMs > 0 表示"已通知待重发"状态（§4.5）
├── lease/
│   └── LeaseManager      到期最小堆；登记/续期/摘除/扫描到期
├── session/
│   └── SessionRegistry   sessionId → 该会话触及的 key 集合（加速断连清理）
```

### 4.3 关键接口签名

```java
public final class CoreEngine {
    public CoreEngine(CoreConfig config, Clock clock, CoreEventListener listener);

    /** 新会话登记，返回 sessionId。 */
    public long sessionOpened();

    /** 会话关闭：释放其全部持锁、摘除其全部等待项。幂等。 */
    public void sessionClosed(long sessionId);

    public AcquireResult acquire(AcquireCommand cmd);

    public ReleaseResult release(ReleaseCommand cmd);

    public RenewResult renew(RenewCommand cmd);

    /**
     * 到期扫描：释放所有已过期租约，并对被释放 key 触发队首通知事件。
     * 由单一调度线程周期调用（见 §5.6 线程模型）。返回本次释放数量（供未来指标）。
     */
    public int expireDue();

    /**
     * 队首响应超时清扫：移除"已通知但超时未重发"的队首等待项，并对新队首补发通知。
     * 与 expireDue 由同一调度线程周期调用。返回本次移除数量。
     */
    public int sweepNotifiedHeads();
}

public interface CoreEventListener {
    /** key 的队首等待者可以重试获取（对应协议 AWAIT_NOTIFY）。 */
    void notifyHead(long sessionId, long requestId, String key);
}

public interface Clock {
    long nowMs();
}
```

事件模型说明：core **不持有**任何与连接相关的对象，只通过 `CoreEventListener` 向外报告"该通知谁"；server 层实现该接口并把事件翻译成 `AWAIT_NOTIFY` 写回对应 Channel。core 因此保持纯语义、可单测。

### 4.4 锁类型语义

归属键统一为 `Owner = (sessionId, threadId)`，与概要设计 §6.4 一致。`LockEntry` 以统一结构承载四种类型：

| 字段                                      | 用途                                                 |
|-------------------------------------------|------------------------------------------------------|
| `Writer: Owner` + `writeCount`            | 写侧/互斥侧持有者（REENTRANT / SIMPLE / WRITE 使用） |
| `readers: Map<Owner, Integer>`            | 读锁持有者 → 各自重入计数                            |
| `reentrant: boolean`                      | SIMPLE 为 false，其余为 true                         |
| `leaseToken / leaseExpiresAtMs / leaseMs` | 当前生效租约（无持有者时无效）                       |
| `waiters: ArrayDeque<Waiter>`             | FIFO 等待队列                                        |

**判定规则（`acquire`，条目内同步执行）：**

1. **会话校验**：`sessionId` 不在 `SessionRegistry` → `REJECT_SESSION`（对应协议 `SESSION_EXPIRED`）。
2. **key 校验**：空 → `KEY_EMPTY`；超长（> `maxKeyLength`，默认 512 字节 UTF-8）→ `KEY_TOO_LONG`。
3. **无持有者且等待队列为空**：直接授予。若条目无持有者但队列非空（队首正处于"已通知、待重发"窗口，见 §4.5），新请求者 **排队队尾**，不得越过在队者——FIFO 公平性优先于即时授予。
4. **可重入再入**：写侧持有者 == 请求者且 `reentrant` → `writeCount++`， **租约刷新为整段新租期**，返回 `GRANTED`（同 token）。
5. **读锁**：无写持有者 **且** 等待队列为空 → 加入 `readers` 授予。有写持有者或队列非空（哪怕队首是读者）→ 排队。该规则保证严格 FIFO，杜绝写者饥饿（概要设计 §4.1"服务器端 FIFO 公平等待"）。
6. **其余情况**：`wait_ms == 0` → `DENIED`；否则入队，队列长度超过 `maxQueueDepthPerKey`（默认 4096）→ `REJECT_QUEUE_FULL`（对应 `OVERLOADED`）。
7. **队首重发命中**：队列非空但请求者恰为队首、且与当前持有状态兼容（无冲突持有者）→ 出队并授予。这是 `AWAIT_NOTIFY → 重发 ACQUIRE` 的落地路径（§4.5）。

**释放规则（`release`）：**

1. token 不匹配 → `INVALID_TOKEN`（修复概要设计 §2.3 缺陷 1：任意客户端解锁）；
2. 归属不匹配（理论上 token 匹配即归属匹配，防御性保留）→ `NOT_HELD`；
3. 读锁：对应 reader 计数减一，归零移除；写侧：`writeCount` 减一；
4. 计数归零 → 清除持有状态、从 `LeaseManager` 摘除租约，并对队首触发 `notifyHead` 事件；返回 `fullyReleased = true`。

**续租规则（`renew`）：** token 匹配 → `leaseExpiresAtMs = now + leaseMs`（受 `CoreConfig` 上下限钳制），更新 `LeaseManager`；否则 `INVALID_TOKEN` / `NOT_HELD`。

**SimpleLock 的自锁问题**：同 Owner 持有 SIMPLE 锁时再次获取会排队等待自己，直至租约到期才解开。这是不可重入语义的直接推论（与 JUC 非重入锁行为一致），在 Javadoc 中明确警示；租约到期是兜底，不构成死锁残留。

**读写升降级**：Phase 1 不支持锁降级获取（持写再取读）与升级（持读再取写）的特判，一律走通用规则——持读请求写将排队，若该读锁不释放则等到租约到期。文档明确声明，避免误用。

### 4.5 等待队列与唤醒

- 队列严格按入队顺序（`ArrayDeque`），同 `(sessionId, requestId)` 重复入队被幂等去重拒绝（§4.8）；
- **只通知队首，不批量唤醒**（无惊群，概要设计 §6.2）。队首收到通知后由客户端重发 ACQUIRE，走规则 7 授予；
- **队首响应超时**：`notifyHead` 发出后，该队首进入"已通知待重发"状态并记录截止时刻（`now + headReplyTimeoutMs`，默认 5s）。若截止前未收到其重发的 ACQUIRE（客户端已放弃等待但未断连），扫描线程将其移出队列并对新队首补发通知。该机制保证队列不会因静默放弃而停摆；
- 队首被移除（会话关闭/响应超时/客户端断连）时执行统一的"队首前进检查"：若条目 **无持有者**，立即对新队首发 `notifyHead`；否则等待下一次释放事件；
- 读写混合场景：写者释放后队首为读者 → 该读者被授予；其后的读者需等待各自的"授予 → 释放"或"队首前进"事件逐个前进。该串行推进在 Phase 1 接受（读锁竞争热点场景的优化留给 Phase 3 评估，见 §12）。

### 4.6 租约机制（服务端侧）

- 授予即带租约：`leaseMs` 取请求值，钳制到 `[minLeaseMs, maxLeaseMs]`（默认 `[1s, 1h]`），请求 0 用默认值（默认 30s，概要设计 §6.3）；
- `LeaseManager` 维护最小堆 `(expiresAt, key, leaseToken)`：
    - 授予/续租 → 入堆新记录；
    - 释放 → 不立即删堆，靠到期时的 **陈旧校验**跳过（堆记录的 `leaseToken` 或 `expiresAt` 与条目当前值不一致即为陈旧）；
    - `expireDue()`：弹出所有 `expiresAt <= now` 的记录，陈旧者丢弃，有效者执行"强制释放 + 队首通知"。堆中陈旧记录数量与锁变更频率同阶，到期扫描时顺带清理，无需额外簿记；
- **不续租的锁必然释放**（概要设计 §6.3）：`expireDue` 是纯服务端行为，与客户端死活无关。

### 4.7 会话管理（服务端侧）

- `SessionRegistry`：`sessionId → Set<String>`（该会话持有或等待过的 key）；`acquire`/排队时登记；
- `sessionClosed(sessionId)`：遍历该会话的 key 集合（而非全表），逐条同步：写侧归属匹配 → 强制释放并通知队首；`readers` 中匹配 → 移除；等待队列中匹配 → 摘除并按 §4.5 规则补通知。清理完成后移除会话记录；
- 复杂度：O (会话触及的 key 数)，断连清理不随全局锁数退化；
- 与租约构成双保险（概要设计 §6.4）：断连清理是即时主路径，租约到期覆盖"断连未检测到"（半开连接）的残余场景。

### 4.8 幂等与请求去重

重复请求来源：客户端超时重发、`AWAIT_NOTIFY` 后的重发。规则：

- core 层：同 `(sessionId, requestId)` 的 `Waiter` 已在队列中 → 返回 `QUEUED`（带当前位次）， **不二次入队**（概要设计 §7"重复请求"）；
- 授予结果无需服务端缓存：若重发时条目已可授予，走规则 7 正常授予；若尚不可授予，返回 `QUEUED`，等待下一次通知。客户端侧对重复的 `QUEUED` 响应幂等处理（future 已挂起则忽略）。

### 4.9 线程模型与并发论证

| 线程                                             | 职责                                         |
|--------------------------------------------------|----------------------------------------------|
| Netty IO 线程（多条）                            | 调用 `acquire/release/renew/sessionClosed`   |
| 租约扫描线程（1 条，`ScheduledExecutorService`） | 周期调用 `expireDue` 与 `sweepNotifiedHeads` |

并发安全依据：

1. `LockTable` 用 `ConcurrentHashMap`，条目创建用 `computeIfAbsent`，条目销毁惰性化——条目仅在"无持有者且无等待者"时从表中移除（在条目锁内做二次检查后 `remove(key, entry)`），避免移除/创建竞态；
2. 单个条目的所有状态迁移（授予/排队/释放/到期/会话清理）都在该条目的内置锁（`synchronized(entry)`）内完成， **任何调用路径最多持有一个条目锁**，无锁序、无死锁；
3. `LeaseManager` 内部自行同步（独立于条目锁；`expireDue` 先在堆内取出到期项，再逐条获取条目锁执行释放）；
4. `SessionRegistry` 用 `ConcurrentHashMap<Long, Set<String>>` + 并发 Set；
5. `CoreEventListener` 回调在条目锁 **外**触发（先收集待通知列表，解锁后统一回调），防止回调路径（写 Channel）与条目锁交叉持有。

---

## 5. openlatch-server 详细设计

### 5.1 类总览

```
server/
├── OpenLatchServer        main 入口：加载配置 → 组装 core → 启动 Netty
├── ServerConfig           配置 record + Properties 加载
├── net/
│   ├── ServerBootstrapFactory   Netty ServerBootstrap 构建
│   ├── ServerChannelInitializer pipeline 装配
│   ├── IdleEventHandler         空闲检测 → 断连
│   └── EnvelopeCodecHandler     编解码异常 → 错误响应/断连
├── dispatch/
│   ├── RequestDispatcher        Envelope → core 命令；core 结果 → Envelope
│   └── IdempotencyWindow        （可选）已完成 requestId 的短缓存，见 §5.5
├── session/
│   └── ServerSession            Channel 绑定：sessionId、inflight 计数
└── NotifyEventBridge            CoreEventListener 实现：事件 → AWAIT_NOTIFY 写回
```

### 5.2 Netty pipeline

```
入站: LengthFieldBasedFrameDecoder(1MiB,0,4,0,4)
        → ProtobufDecoder(Envelope.getDefaultInstance())
        → IdleStateHandler(read=60s)
        → ServerSessionHandler
出站: ProtobufEncoder
        → LengthFieldPrepender(4)
```

- Boss 组 1 线程，Worker 组默认 `2 × CPU`（可配置）；
- 业务逻辑（core 调用）全部是短临界区、无阻塞调用， **直接在 IO 线程执行**，无需独立业务线程池——与概要设计 §2.3 缺陷 7（每等待者一个线程）形成对照：等待者只是内存队列条目，零线程占用。

### 5.3 会话生命周期

```
客户端                        服务端
  |-- TCP connect ------------->|  channelActive：创建 ServerSession（未认证）
  |-- HELLO ------------------->|  校验版本/参数；分配 sessionId；
  |                               |  core.sessionOpened()；绑定到 Channel
  |<-- HELLO resp(sessionId) ---|
  |  ...业务请求...              |
  |   （空闲 60s 无读）          |  IdleStateHandler 触发 → 关闭连接
  |   或客户端断开               |  channelInactive：
  |                               |  core.sessionClosed(sessionId) —— 即时清理
```

- 未完成握手（未收到合法 HELLO）的连接上的业务请求一律回 `INVALID_REQUEST`；
- `channelInactive` 与 `expireDue` 并发安全：二者都经条目锁串行化，先到者生效，后到者走陈旧校验空转（§4.9）。

### 5.4 请求分发

`RequestDispatcher.dispatch(ServerSession s, Envelope e)`：

| 消息         | 处理                                                                         |
|--------------|------------------------------------------------------------------------------|
| HELLO        | §5.3；重复 HELLO → `INVALID_REQUEST`                                         |
| LOCK_ACQUIRE | 构建 `AcquireCommand` → `core.acquire` → 按 `Outcome` 映射 `AcquireResponse` |
| LOCK_RELEASE | 同上，`ReleaseResponse`                                                      |
| LEASE_RENEW  | 同上，`LeaseRenewResponse`                                                   |
| PING         | 不回复（活动信号已被 IdleStateHandler 计入）                                 |
| 未知/不匹配  | `INVALID_REQUEST`；编解码失败 → 记录日志并断连                               |

响应一律回显请求的 `request_id`。单连接未完成请求数超过 `maxInflightPerConnection`（默认 1024）→ 回 `OVERLOADED`（概要设计 §7 自我保护）。

### 5.5 幂等窗口（可选实现）

§4.8 的队列内去重已覆盖主要重复场景。额外可选：每会话保留最近 1024 条已完成 `(requestId → 响应)` 的 LRU，命中则直接回放响应。Phase 1 判定： **暂不实现**（YAGNI）——客户端对"重复收到 QUEUED/重复授予"均可幂等处理；若集成测试暴露真实问题再补。

### 5.6 租约扫描调度

- 单线程 `ScheduledExecutorService`，周期 `leaseTickIntervalMs`（默认 500ms）依次调用 `core.expireDue()` 与 `core.sweepNotifiedHeads()`；
- 服务关停时先停扫描、再关 Channel、最后 `awaitTermination`，保证关停过程无新通知产生。

### 5.7 配置项

配置文件为 Java Properties，路径由 `-Dopenlatch.config=<path>` 指定，未指定时用内置默认值。

| 配置键                                               | 默认值             | 说明                                 |
|------------------------------------------------------|--------------------|--------------------------------------|
| `openlatch.server.port`                              | `9410`             | 监听端口                             |
| `openlatch.server.worker-threads`                    | `2 × CPU`          | Netty Worker 线程数                  |
| `openlatch.server.session.idle-timeout-ms`           | `60000`            | 连接空闲断开                         |
| `openlatch.server.lease.default-ms`                  | `30000`            | 默认租约（概要设计 §6.3）            |
| `openlatch.server.lease.min-ms` / `max-ms`           | `1000` / `3600000` | 租约钳制区间                         |
| `openlatch.server.lease.tick-interval-ms`            | `500`              | 到期扫描周期                         |
| `openlatch.server.queue.head-reply-timeout-ms`       | `5000`             | 队首收到通知后的重发响应时限（§4.5） |
| `openlatch.server.limit.max-key-length`              | `512`              | key 最大字节数                       |
| `openlatch.server.limit.max-queue-depth-per-key`     | `4096`             | 单 key 等待队列上限                  |
| `openlatch.server.limit.max-inflight-per-connection` | `1024`             | 单连接未完成请求上限                 |

### 5.8 交付形态

- `maven-shade-plugin` 产出可执行 jar，`mainClass = OpenLatchServer`；
- 启动日志打印：端口、协议版本、关键限额配置；
- JVM 退出钩子执行 §5.6 的关停序列。

---

## 6. openlatch-client 详细设计

### 6.1 类总览

```
client/
├── OpenLatchClient          入口：builder 构建；newXxxLock 工厂；shutdown
├── OLock                    同步锁接口（JUC 风格）
├── OReadWriteLock           读写锁门面：readLock() / writeLock()
├── LockGrant                record(leaseToken, grantedLeaseMs)
├── LockLostListener         函数式接口：锁丢失回调
├── OpenLatchException       运行时异常基类
│   ├── LockAcquisitionTimeoutException
│   ├── ServerUnavailableException
│   └── LockLostException
└── internal/
    ├── ConnectionManager    连接与重连状态机
    ├── SessionContext       当前 sessionId、requestId 分配器
    ├── RequestMultiplexer   requestId ↔ CompletableFuture 关联 + 请求超时
    ├── AwaitTracker         QUEUED 挂起 / AWAIT_NOTIFY 处理 / 重发
    ├── Watchdog             持锁续租调度
    ├── HeldLockRegistry     本地持锁簿记（key → token/到期/监听器）
    └── ClientConfig         客户端配置
```

### 6.2 连接与重连状态机（ConnectionManager）

```
DISCONNECTED ──connect()──▶ CONNECTING ──成功──▶ HELLO_SENT ──握手成功──▶ ACTIVE
     ▲                          │ 失败                        │ 断连
     │                          ▼                             ▼
     └──────────────── RECONNECTING ◀────────────────────────┘
                        （指数退避）
```

- 重连退避：初始 200ms，×2，上限 10s，每次 ±20% 随机抖动；`shutdown()` 后不再重连（终态 `CLOSED`）；
- **断连瞬间的本地处理**（与概要设计 §7 一致）：
    1. 所有挂起中的获取/释放/续租 future → `ServerUnavailableException`（等待中快速失败，概要设计 §4.3 标准 2）；
    2. 每个本地持锁计算"失锁时刻" `lostAt = 上次成功续租 + grantedLeaseMs`；
    3. 若重连在 `lostAt` 前成功：旧 session 已被服务端清理，旧锁必然已失效 → 重连成功时 **立即**对全部旧持锁触发 `LockLostListener`；若到 `lostAt` 仍未重连成功 → 到时触发回调（覆盖半开连接场景，此时服务端也是靠租约释放）。
- 重连成功后 `sessionId` 更换，`requestId` 重新从 1 分配；使用旧 session 的残留响应按 `requestId` 无法匹配，直接丢弃。

### 6.3 公开 API

异步内核（概要设计 §6.6）：

```java
public final class OpenLatchClient {
    public static Builder builder();                 // address / 各类超时 / 监听器

    public CompletableFuture<LockGrant> acquireAsync(AcquireSpec spec);

    public CompletableFuture<Void> releaseAsync(String key, long leaseToken, long threadId);

    public OLock newReentrantLock(String key);

    public OLock newSimpleLock(String key);

    public OReadWriteLock newReadWriteLock(String key);

    public void addLockLostListener(LockLostListener l);   // 全局监听

    public void shutdown();                                // 尽力释放持锁后关闭
}
```

同步包装（JUC 风格）：

```java
public interface OLock {
    String key();

    void lock() throws InterruptedException;                       // 受总超时兜底

    boolean tryLock();                                             // 立即式

    boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException;

    void unlock();                                                 // 仅持锁线程可调

    boolean isHeldByCurrentThread();

    void onLockLost(LockLostListener listener);                    // 单锁监听

    CompletableFuture<LockGrant> lockAsync();

    CompletableFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit);
}
```

语义约定：

- `lock()` = `tryLock(defaultWaitTimeout)`；`defaultWaitTimeout` 默认 30s，可配置——保证概要设计 §4.2"任何 API 不存在无限阻塞路径"；
- `unlock()` 在本地非持锁线程调用 → 抛 `IllegalMonitorStateException`（与 JUC 一致）；
- 可重入计数由服务端维护，客户端每次 `unlock()` 发送一次 RELEASE；本地 `HeldLockRegistry` 只记录首个 token 与监听器，不重复计数，避免双账本漂移；
- `tryLock(waitTime)` 到时未授予： **取消即放弃等待**，客户端仅将本地 future 置为超时失败，不向服务端发送取消消息。服务端的兜底机制：
    - 该等待项后续若轮到队首并被通知，因无人重发，将在 **队首响应超时**（§4.5，默认 5s）后被移出，队列继续前进；
    - 若该等待项尚未轮到队首，则保持排队，直到其成为队首后走同一路径被回收，或会话断连时被清理；
    - 队列深度有限额保护（§5.7），静默放弃不会造成无界积压。

> 设计说明：显式"取消排队"消息能更快回收队列位置，但引入新消息类型与竞态（取消与授予同时发生：服务端已授予、取消却在途）。Phase 1 以"队首响应超时"回收代替主动取消，简化优先；代价是放弃者若排在队首，队列位置最长占用一个响应超时窗口。若基准测试显示该窗口造成显著延迟，再在协议中补 `ACQUIRE_CANCEL`。

### 6.4 请求多路复用（RequestMultiplexer）

- `requestId` 由 `AtomicLong` 按连接分配；
- 发送请求 → `inflight: ConcurrentHashMap<Long, PendingRequest>` 登记 `(future, deadline)`，并在 `HashedWheelTimer` 上挂超时任务；
- 响应到达 → 按 `request_id` 摘除并 `complete`；超时任务触发 → `completeExceptionally(OpenLatchTimeoutException)`；
- **每个请求必有超时**（默认 5s，可配置）——概要设计 §4.3 标准 3；不存在无超时的 `take()`/自旋等待（前身缺陷 2 的对照修复）。

### 6.5 等待跟踪（AwaitTracker）

```
acquireAsync(spec)
  → 发送 ACQUIRE(requestId=r)
  → 响应 OK       → future.complete(grant)，登记 HeldLockRegistry + 启动看门狗
  → 响应 QUEUED   → 登记 AwaitTracker(r, key, 用户总超时)；future 保持挂起
  → 响应 DENIED/错误 → future.completeExceptionally
  → AWAIT_NOTIFY(requestIdRef=r) 到达
        → 以同一 requestId=r 重发 ACQUIRE（服务端幂等，§4.8）
        → 响应 OK → complete；仍 QUEUED → 继续挂起
  → 用户总超时到达 → future 超时失败（服务端队列条目惰性移除，§6.3）
```

边界场景处理：

| 场景                          | 行为                                                                                                                            |
|-------------------------------|---------------------------------------------------------------------------------------------------------------------------------|
| 通知到达时 future 已超时/取消 | 按 `requestId` 找不到 tracker → 忽略该通知；不重发                                                                              |
| 重发的 ACQUIRE 得到 QUEUED    | 正常：保持挂起等待下一次通知                                                                                                    |
| 通知与断连同时发生            | 断连处理先行清空所有 tracker（§6.2），通知被忽略                                                                                |
| 同一请求收到两次通知          | 重发幂等：第二次重发返回 QUEUED 或重复授予判定由服务端完成；客户端以首个 OK 为准，重复 OK 时释放（发送 RELEASE 归还）——测试覆盖 |

### 6.6 看门狗（Watchdog）

- 共享一个 `HashedWheelTimer`；每个持锁登记一个续租任务，周期 `grantedLeaseMs / 3`（概要设计 §6.3）；
- 续租请求本身受请求超时约束（默认 5s，且不超过 `lease/3`）；
- 失败判定：
    - 响应 `INVALID_TOKEN` / `NOT_HELD` / `SESSION_EXPIRED` → **锁已失效**：停止续租，触发 `LockLostListener`（携带 `LockLostException` 原因）；
    - 请求超时 → 记录连续失败次数，下一周期重试； **连续 2 次超时** → 判定失效并触发回调；
    - 成功 → 更新本地到期时间，重置失败计数；
- `unlock()` 成功（`fullyReleased`）后注销续租任务；
- 锁丢失回调在独立执行器上调用（默认单线程），用户回调异常被捕获并记日志，不影响其他续租任务。

### 6.7 客户端配置项（Builder）

| 参数                                              | 默认值      | 说明                   |
|---------------------------------------------------|-------------|------------------------|
| `address`                                         | 必填        | `host:port`            |
| `requestTimeout`                                  | 5s          | 单个请求超时           |
| `defaultWaitTimeout`                              | 30s         | `lock()` 总超时兜底    |
| `connectTimeout`                                  | 3s          | TCP + 握手超时         |
| `reconnectInitialBackoff` / `reconnectMaxBackoff` | 200ms / 10s | 指数退避               |
| `workerThreads`                                   | 1           | 客户端 Netty EventLoop |

### 6.8 线程模型

所有网络读写在客户端 EventLoop 线程；用户回调（锁丢失）在专用单线程执行器；`lock()/tryLock()` 的阻塞通过 `future.get(timeout)` 实现，用户线程不持有其他资源锁，无死锁路径。

---

## 7. 可靠性与错误处理汇总

对应概要设计 §7 的 Phase 1 部分：

| 故障场景       | 机制                                  | 客户端可见行为                                       |
|----------------|---------------------------------------|------------------------------------------------------|
| 请求超时       | 客户端每请求超时（§6.4）              | `OpenLatchTimeoutException`，可重试                  |
| 断连（持锁中） | 服务端即时会话清理 + 租约兜底         | 重连成功即触发锁丢失回调；未重连则 `lostAt` 到期触发 |
| 断连（等待中） | 服务端会话清理摘除等待项              | 挂起 future 快速失败                                 |
| 服务器重启     | 内存锁表丢失 = 全部锁释放             | 客户端请求超时 → 重连 → 重新竞争；无残留假锁         |
| 误释放         | `leaseToken` 校验                     | `INVALID_TOKEN`，锁不受影响                          |
| 重复请求       | `(sessionId, requestId)` 幂等（§4.8） | 无二次排队                                           |
| 服务端过载     | key 长度 / 队列深度 / inflight 限额   | `KEY_TOO_LONG` / `OVERLOADED`，明确错误码            |

错误码 × 场景矩阵：

| 错误码                   | ACQUIRE       | RELEASE              | RENEW | HELLO             |
|--------------------------|---------------|----------------------|-------|-------------------|
| QUEUED                   | ✔ 排队       | —                    | —     | —                 |
| DENIED                   | ✔ 立即式失败 | —                    | —     | —                 |
| INVALID_TOKEN            | —             | ✔                   | ✔    | —                 |
| NOT_HELD                 | —             | ✔                   | ✔    | —                 |
| SESSION_EXPIRED          | ✔            | ✔                   | ✔    | —                 |
| OVERLOADED               | ✔            | ✔（不占队列的限额） | ✔    | —                 |
| KEY_TOO_LONG / KEY_EMPTY | ✔            | ✔                   | ✔    | —                 |
| INVALID_REQUEST          | ✔            | ✔                   | ✔    | ✔ 版本/参数非法  |
| INTERNAL_ERROR           | ✔            | ✔                   | ✔    | ✔ 未预期异常兜底 |

## 8. openlatch-spring-boot-starter 详细设计

### 8.1 自动装配

- 注册文件：`META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`（Spring Boot 3 机制，概要设计 §5.3）；
- `OpenLatchAutoConfiguration`：
    - `@EnableConfigurationProperties(OpenLatchProperties.class)`；
    - Bean `OpenLatchClient`（`@ConditionalOnMissingBean`，允许用户自定义覆盖）；
    - Bean `OpenLatchAspect`（`@ConditionalOnProperty("openlatch.enabled", default true)`）；
    - 应用关闭时调用 `client.shutdown()`（Bean destroy 回调）。

### 8.2 配置属性（`openlatch.*`）

| 属性                                                                      | 默认                 | 说明                           |
|---------------------------------------------------------------------------|----------------------|--------------------------------|
| `openlatch.enabled`                                                       | `true`               | 关闭后注解不生效（切面不注册） |
| `openlatch.server-host` / `openlatch.server-port`                         | `127.0.0.1` / `9410` |                                |
| `openlatch.request-timeout`                                               | `5s`                 | Duration                       |
| `openlatch.default-wait-timeout`                                          | `30s`                | `lock()` 兜底                  |
| `openlatch.reconnect-initial-backoff` / `openlatch.reconnect-max-backoff` | `200ms` / `10s`      |                                |

### 8.3 注解与切面

```java

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface OpenLatch {
    String key();                          // SpEL 表达式

    LockMode type() default LockMode.REENTRANT;   // REENTRANT/SIMPLE/READ/WRITE

    long waitTime() default -1;            // -1 = 用 lock()（受全局兜底超时）；0 = 立即式；>0 = 限时

    long leaseTime() default 0;            // 0 = 服务端默认 30s

    TimeUnit timeUnit() default TimeUnit.SECONDS;
}
```

切面流程（`OpenLatchAspect`，`@Around("@annotation(openLatch)")`）：

1. **key 求值**：SpEL `StandardEvaluationContext` 注入方法形参（`#paramName`）；表达式编译结果按"方法 + 表达式"缓存；
    - 前置要求：编译开启 `-parameters`（Starter README 明示；参数名解析用 `DefaultParameterNameDiscoverer`）；
    - 求值结果必须为非空字符串，否则抛 `OpenLatchException`。
2. **获取锁**：按 `(key, type)` 从 `OLock` 缓存取锁句柄；`waitTime < 0 → lock()`，`== 0 → tryLock()`，`> 0 → tryLock(waitTime)`；
3. 获取失败（超时）→ 抛 `LockAcquisitionTimeoutException`，业务方法不执行；
4. 获取成功 → `try { 执行业务 } finally { unlock() }`；
5. READ/WRITE 类型映射到 `newReadWriteLock(key).readLock()/writeLock()`。

**事务交互说明**：切面默认 Bean 方法级拦截；若业务同时用 `@Transactional`，锁切面的顺序（`@Order`）默认低于事务切面，即 **锁在事务外层**——保证事务提交后才释放锁。该顺序写入 README 并在测试中锁定。

### 8.4 starter 模块的 Java 版本策略

若风险 6 验证结果为 Spring Boot 3.5.x 不完全支持 Java 25：优先升级 Spring Boot 4.x；若仍不可行，starter 模块单独以 `--release 17` 编译（其余模块保持 25）。该决策在 M4 启动时记录于本文档修订版。

## 9. openlatch-examples

| 示例                      | 演示点                                                           |
|---------------------------|------------------------------------------------------------------|
| `QuickStartExample`       | 编程式 API：lock/tryLock/unlock                                  |
| `ConcurrencyExample`      | 多线程竞争同一互斥锁，打印授予顺序（FIFO 可观察）                |
| `ReadWriteExample`        | 读写并发                                                         |
| `WatchdogExample`         | 长任务 + 看门狗续租日志；人为停止续租观察锁丢失回调              |
| `SpringAnnotationExample` | SB3 应用 + `@OpenLatch`（SpEL key）                              |
| `BenchmarkMain`           | 单节点 `tryLock` 吞吐与授予延迟（JMH 或手写热身+计时，记录基线） |

---

## 10. 测试设计

### 10.1 core 单元测试（M1，无网络）

| 用例组       | 用例                                                                                                           |
|--------------|----------------------------------------------------------------------------------------------------------------|
| 互斥         | 两会话竞争同 key，一授予一排队；释放后队首授予                                                                 |
| 重入         | 同 Owner 重入计数；按计数释放；跨会话同 threadId 不视为重入                                                    |
| 读写         | 多读者并发授予；写者互斥；读者持有时写者排队；写者持有时读者排队；严格 FIFO（读者到达早于写者 → 写者不能越过） |
| FIFO 公平    | 授予顺序 == 入队顺序（含读写混合序列断言）；通知窗口内新到者排队尾不越位（规则 3）                             |
| 队首响应超时 | 通知队首后其不重发（模拟放弃）→ 超时后移出并向新队首补通知；队列不停摆                                         |
| 租约         | 手工时钟推进到期 → 自动释放 + 队首通知；续租延长到期；过期后旧 token 续租/释放均被拒                           |
| 误释放       | 错误 token → `INVALID_TOKEN` 且锁不变                                                                          |
| 会话         | `sessionClosed` 释放该会话全部持锁与等待项；补通知逻辑正确                                                     |
| 限额         | key 超长/空、队列深度超限、未知会话 → 对应拒绝码                                                               |
| 幂等         | 同 `(session, requestId)` 重复 acquire → 单次入队                                                              |

### 10.2 协议测试（M1/M2）

- 全部消息编解码 round-trip；
- 带未知字段的消息解码不报错且字段保留；
- `Envelope` 缺失/错误 payload 与 `type` 不匹配时的服务端行为（`INVALID_REQUEST`，不断连）。

### 10.3 集成测试（M2/M3，真实 server + client）

- 多线程（≥16）竞争同一互斥锁 N 轮：临界区计数器断言互斥成立、无丢失；
- tryLock 超时组合：立即式、限时成功、限时失败；
- 读写并发组合矩阵；
- 看门狗端到端：租约 3s 的任务持有 10s，期间锁不过期（续租生效）；
- 公平性端到端：按发起顺序断言授予顺序；
- 幂等时序：通知后重发、超时与通知竞争（§6.5 场景表逐项覆盖）。

### 10.4 故障注入（M3）

| 场景         | 注入方式                                  | 断言                                                              |
|--------------|-------------------------------------------|-------------------------------------------------------------------|
| 持锁中断连   | 客户端获取后直接关闭 Channel（不 unlock） | 一个租约期内服务端自动释放，其他客户端可获取（测试用短租约 1–2s） |
| 等待中断连   | 排队后关闭 Channel                        | future 快速失败；队列中无残留（后续授予顺序正确）                 |
| 杀服务端进程 | 进程级终止                                | 客户端全部请求在超时内失败；服务端重启后自动重连并恢复服务        |
| 半开连接     | 客户端进程挂起（暂停 EventLoop 模拟）     | 服务端空闲检测断连 → 会话清理                                     |

### 10.5 基准（M4）

- 指标：单节点 `tryLock`（无竞争）吞吐、竞争场景吞吐、授予延迟 P99；
- 记录为基线（防退化）， **不作发布门槛**（概要设计 §8）。

## 11. Phase 1 验收标准

与概要设计 §4.3 逐条对应：

| # | 概要设计成功标准                                                      | 验收证据                                                  |
|---|-----------------------------------------------------------------------|-----------------------------------------------------------|
| 1 | 互斥、重入计数、读写互斥、FIFO 公平、租约到期释放全部有自动化测试覆盖 | §10.1 用例组全绿（CI 报告）                               |
| 2 | 故障注入：持锁断连→租约释放；等待断连→快速失败                        | §10.4 前两项通过                                          |
| 3 | 客户端所有请求路径带超时，无死等                                      | §6.4/§6.7 超时参数 + 集成测试"杀服务端"用例（无挂起线程） |
| 4 | SB 3.2+ 应用仅加依赖与注解即可使用                                    | §10.3 starter 集成测试 + examples 人工接入                |

## 12. 已知局限与遗留项

| # | 局限/遗留                                                                                         | 处理                                                                         |
|---|---------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------|
| 1 | 读者逐个串行推进（§4.5），高竞争读场景吞吐受限                                                    | Phase 3 评估批量授予优化                                                     |
| 2 | 客户端放弃等待不主动取消排队（§6.3），放弃者位于队首时队列位置最长占用一个响应超时窗口（默认 5s） | 队首响应超时机制保证回收与队列前进；必要时后续补 `ACQUIRE_CANCEL` 以即时回收 |
| 3 | 单机内存锁，重启即全部释放                                                                        | 概要设计 §7 已声明；Phase 2 解决                                             |
| 4 | `wait_ms > 0` 的计时在客户端，客户端时钟回拨可能使等待略长                                        | 可接受；服务端不依赖客户端时间                                               |

## 13. 实施子任务拆分

**粒度定义**：每个子任务是一个可独立交付、可独立验证的工作单元（通常对应一次代码评审）；"验证"列是该子任务自身的完成判据，里程碑退出标准仍按实施计划执行。编号（P1-xx）稳定，用于进度跟踪。

### 13.1 M1：协议与纯 Java 核心

| ID    | 子任务            | 内容与交付物                                                                                                    | 前置  | 验证                                                              |
|-------|-------------------|-----------------------------------------------------------------------------------------------------------------|-------|-------------------------------------------------------------------|
| P1-01 | Maven 多模块骨架  | 父 pom（Java 25、`io.github.lamspace`）、6 模块目录与依赖关系（§2）、protobuf 代码生成插件配置                  | —     | `mvn verify` 通过；依赖关系符合 §2（core 无 Netty/protocol 依赖） |
| P1-02 | `.proto` 协议定义 | 按 §3.2 完成全部枚举与消息定义                                                                                  | P1-01 | protoc 生成代码成功；字段编号、枚举取值与 §3.2 逐项一致           |
| P1-03 | 协议编解码测试    | §10.2 全部用例：round-trip、未知字段、type/payload 不匹配                                                       | P1-02 | §10.2 用例全绿                                                    |
| P1-04 | core 骨架         | `Clock`/`SystemClock`、`CoreConfig`、command/result records、`CoreEventListener`、`CoreEngine` 类骨架（方法桩） | P1-01 | 编译通过；公开签名与 §4.3 一致                                    |
| P1-05 | 互斥与可重入语义  | `LockTable`/`LockEntry`/`Waiter` 骨架；判定规则 1/2/3/4/6（无队列快路径）与 Owner 模型                          | P1-04 | §10.1 互斥、重入用例组全绿                                        |
| P1-06 | 读写锁语义        | `readers` 映射、规则 5（读写严格 FIFO）、升降级不做特判的声明                                                   | P1-05 | §10.1 读写用例组全绿                                              |
| P1-07 | 等待队列与通知    | 规则 7（队首重发命中）、`notifyHead` 事件（条目锁外回调）、队首响应超时 `sweepNotifiedHeads`（§4.5）            | P1-05 | §10.1 FIFO 公平、队首响应超时用例组全绿                           |
| P1-08 | 租约机制          | `LeaseManager` 最小堆、登记/陈旧校验、`expireDue` 强制释放 + 队首通知                                           | P1-05 | §10.1 租约用例组全绿（手工时钟，无 sleep）                        |
| P1-09 | 会话管理          | `SessionRegistry`、`sessionOpened/sessionClosed` 三类清理（写持有/读持有/等待项）                               | P1-05 | §10.1 会话用例组全绿                                              |
| P1-10 | 幂等与限额        | `(sessionId, requestId)` 去重、key 校验、队列深度限额、未知会话拒绝                                             | P1-07 | §10.1 幂等、限额用例组全绿；**M1 退出**：7 类语义回归全绿         |

### 13.2 M2：单节点服务器

| ID    | 子任务             | 内容与交付物                                                                                    | 前置  | 验证                                         |
|-------|--------------------|-------------------------------------------------------------------------------------------------|-------|----------------------------------------------|
| P1-11 | 配置与启动骨架     | `ServerConfig` Properties 加载（§5.7 全项）、`OpenLatchServer` main、§5.6 关停序列              | M1    | 进程可启动监听；关停顺序测试通过             |
| P1-12 | Netty pipeline     | 按 §5.2 装配 handler 链；1 MiB 帧长限制                                                         | P1-11 | EmbeddedChannel 半包/粘包/超帧长断连测试通过 |
| P1-13 | HELLO 握手         | `ServerSession`、sessionId 分配、握手前拒绝、版本校验断连                                       | P1-12 | §3.2.1 握手规则用例通过                      |
| P1-14 | 请求分发           | `RequestDispatcher`（§5.4 映射表）、requestId 回显、inflight 限额 → `OVERLOADED`                | P1-13 | 分发表逐消息单元测试通过                     |
| P1-15 | 通知桥与扫描调度   | `NotifyEventBridge`（事件 → `AWAIT_NOTIFY`）、单线程周期调用 `expireDue` + `sweepNotifiedHeads` | P1-14 | 通知 → 队首重发 → 授予 端到端用例通过        |
| P1-16 | 断连清理与空闲检测 | `channelInactive` → `sessionClosed`；IdleStateHandler 超时断连                                  | P1-14 | 断连清理事件测试、空闲超时测试通过           |
| P1-17 | 可执行 jar 与冒烟  | shade 打包、HELLO→ACQUIRE→RENEW→RELEASE 冒烟脚本                                                | P1-16 | jar 独立启动；冒烟脚本通过；**M2 退出**      |

### 13.3 M3：客户端 SDK

| ID    | 子任务         | 内容与交付物                                                                                  | 前置  | 验证                                                 |
|-------|----------------|-----------------------------------------------------------------------------------------------|-------|------------------------------------------------------|
| P1-18 | 客户端骨架     | `ClientConfig`、Builder（§6.7 默认值）、EventLoop、共享 `HashedWheelTimer`                    | M2    | 编译通过；默认值与 §6.7 一致                         |
| P1-19 | 连接与握手     | `ConnectionManager` CONNECTING/HELLO_SENT/ACTIVE 段、`SessionContext`                         | P1-18 | 建连握手集成用例通过                                 |
| P1-20 | 请求多路复用   | `RequestMultiplexer`：requestId 分配、inflight 登记/摘除、每请求超时                          | P1-19 | 响应关联、超时失败单元测试通过                       |
| P1-21 | 等待跟踪       | `AwaitTracker`：QUEUED 挂起、AWAIT_NOTIFY 重发、重复 OK 归还                                  | P1-20 | §6.5 边界场景表逐项用例通过                          |
| P1-22 | OLock 同步包装 | `HeldLockRegistry`、lock/tryLock/unlock/isHeldByCurrentThread、`IllegalMonitorStateException` | P1-20 | JUC 风格语义用例通过                                 |
| P1-23 | 看门狗         | `lease/3` 续租、失败判定（明确错误码即时失锁 / 连续 2 次超时）、回调执行器、解锁注销          | P1-22 | 续租成功与各类失败路径用例通过                       |
| P1-24 | 重连与锁丢失   | 指数退避状态机、断连快速失败、`lostAt` 计算与回调时机（§6.2）                                 | P1-23 | 重连时序用例通过（含半开连接）                       |
| P1-25 | 集成测试套件   | §10.3 全套（并发互斥、超时组合、读写矩阵、看门狗端到端、公平性、幂等时序）                    | P1-24 | §10.3 全绿                                           |
| P1-26 | 故障注入套件   | §10.4 全套（持锁断连、等待断连、杀进程、半开连接）                                            | P1-25 | §10.4 全绿；**M3 退出**：概要设计 §4.3 标准 1–3 达成 |

### 13.4 M4：Starter、示例与文档

| ID    | 子任务                 | 内容与交付物                                                                                 | 前置  | 验证                                        |
|-------|------------------------|----------------------------------------------------------------------------------------------|-------|---------------------------------------------|
| P1-27 | Spring Boot 兼容性验证 | 最新 Spring Boot 3.5.x × Java 25 兼容矩阵（实施计划风险 6）；不兼容时定案备选方案并回写 §8.4 | M3    | 验证结论记录在案                            |
| P1-28 | 自动装配               | `OpenLatchProperties`、`AutoConfiguration.imports`、client Bean、shutdown 回调               | P1-27 | 上下文加载、Bean 注入、enabled 开关测试通过 |
| P1-29 | 注解与切面             | `@OpenLatch`、SpEL 求值与缓存、`-parameters` 要求、与 `@Transactional` 顺序                  | P1-28 | 切面行为单元测试通过（含锁在事务外层断言）  |
| P1-30 | Starter 集成测试       | 内嵌 SB 3.2+ 上下文：互斥执行、SpEL 按参数求值、获取失败抛异常、READ/WRITE                   | P1-29 | 集成用例全绿                                |
| P1-31 | examples               | §9 六个示例全部可独立运行                                                                    | P1-30 | 逐示例运行通过                              |
| P1-32 | 基准基线               | §10.5 指标：吞吐（无竞争/竞争）、授予延迟 P99；记录基线                                      | P1-31 | 基线报告存档                                |
| P1-33 | 文档与验收闭环         | README、公开 API Javadoc 全覆盖、§11 四条验收证据收集                                        | P1-32 | §11 验收表逐项闭环；**Phase 1 发布**        |
