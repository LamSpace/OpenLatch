# Tasks: m1-protocol-core

对应设计说明书 §13.1 子任务 P1-01 ~ P1-10。所有 `mvn` 命令必须携带 `-s /home/lam/repo/settings.xml`。

## 1. Maven 多模块骨架（P1-01）

- [x] 1.1 删除根目录空 `src/` 树（`src/main/java`、`src/main/resources`、`src/test/java`）
- [x] 1.2 根 `pom.xml` 改为 `packaging=pom`，声明六个模块（protocol/core/server/client/spring-boot-starter/examples），`maven.compiler.release=25`
- [x] 1.3 `pluginManagement` 锁定本机已缓存插件版本（compiler 3.15.0、surefire 3.5.4、jar 3.5.0、clean 3.5.0、resources 3.4.0、install 3.1.4）；`dependencyManagement` 锁定 JUnit Jupiter 5.11.4、AssertJ 3.27.7
- [x] 1.4 创建六个模块目录与占位 pom；server/client/starter/examples 暂为空占位，模块间依赖按设计说明书 §2 声明（占位模块不含实质代码）
- [x] 1.5 验证：`mvn -s /home/lam/repo/settings.xml verify` 通过；`dependency:analyze` 确认 core 模块无外部依赖

## 2. 协议定义与代码生成（P1-02）

- [x] 2.1 `openlatch-protocol` 引入 `protobuf-java:3.25.5` 依赖；配置 `os-maven-plugin` 扩展与 `protobuf-maven-plugin:0.6.1`（`protocArtifact` 拉取 `protoc:3.25.5:exe:linux-x86_64`）
- [x] 2.2 编写 `src/main/proto/openlatch.proto`：`MessageType`/`LockType`/`StatusCode` 枚举与 `Envelope`、9 个 payload 消息，字段编号逐项对齐设计说明书 §3.2
- [x] 2.3 验证：构建触发 protoc 生成代码成功；以核对清单逐项比对字段编号与枚举取值与 §3.2 一致

## 3. 协议编解码测试（P1-03）

- [x] 3.1 round-trip 测试：全部消息类型构造 → 序列化 → 反序列化 → 逐字段相等
- [x] 3.2 未知字段容忍测试：经 `UnknownFieldSet` 附加未知字段的消息可解码，且再序列化后保留
- [x] 3.3 验证：`mvn -s /home/lam/repo/settings.xml -pl openlatch-protocol test` 全绿（§10.2 属 M1 的两类用例覆盖）

## 4. core 骨架（P1-04）

- [x] 4.1 按设计说明书 §4.2 建立包结构：`command/`、`result/`、`lock/`、`lease/`、`session/`
- [x] 4.2 实现 `Clock`/`SystemClock`、`CoreConfig`（record，含全部默认值）、`CoreEventListener`、command/result records（`AcquireCommand` 含 `queueIfBusy`，见 design.md D3）与 `Outcome` 枚举
- [x] 4.3 `CoreEngine` 方法桩（签名与 §4.3 一致）；测试用手工时钟与记录型监听器工具类
- [x] 4.4 验证：编译通过；公开签名与 §4.3 一致

## 5. 互斥与可重入语义（P1-05）

- [x] 5.1 `LockTable`/`LockEntry`/`Waiter` 骨架：条目级同步、`computeIfAbsent` 建条目、惰性销毁（含 design.md D4 的成员回查重试）
- [x] 5.2 `SessionRegistry` 最小可用版（登记/存在性检查，清理逻辑留待 9）；`sessionOpened` 分配标识
- [x] 5.3 实现判定规则 1/2/3/4/6：会话与 key 校验、无竞争快路径、可重入计数与租约刷新、`queueIfBusy=false` 的 DENIED 路径；授予返回凭证与生效租约
- [x] 5.4 测试：§10.1 互斥、重入用例组（含跨会话同 threadId 不重入、SIMPLE 自锁排队）
- [x] 5.5 验证：相关用例组全绿

## 6. 读写锁语义（P1-06）

- [x] 6.1 `readers: Map<Owner, Integer>` 与规则 5：无写者且队列空才授予读者，否则排队（严格 FIFO）
- [x] 6.2 升降级不做特判：持读请求写一律排队（Javadoc 声明该语义）
- [x] 6.3 测试：§10.1 读写用例组（多读者并发、写者互斥、读者先到阻止写者越位）
- [x] 6.4 验证：相关用例组全绿

## 7. 等待队列与通知（P1-07）

- [x] 7.1 规则 7：队首以原 `(会话, 请求标识)` 重发且与持有兼容 → 出队授予
- [x] 7.2 `notifyHead` 事件：锁让出且队列非空时仅通知队首；统一"锁内收集、锁外触发"
- [x] 7.3 队首响应超时：通知后记录截止时刻；`sweepNotifiedHeads` 移除超时队首并对新队首补通知（design.md D6 全表扫描）
- [x] 7.4 队首被移除（超时/会话关闭/其他）后的统一队首前进检查
- [x] 7.5 测试：§10.1 FIFO 公平、队首响应超时用例组（手工时钟推进，无 sleep）
- [x] 7.6 验证：相关用例组全绿

## 8. 租约机制（P1-08）

- [x] 8.1 `LeaseManager` 最小堆 `(expiresAt, key, leaseToken)`：只入不删、自同步
- [x] 8.2 租约钳制 `[1s, 1h]`、0 取默认 30s；授予/续租入堆
- [x] 8.3 `expireDue`：弹出到期项 → 陈旧校验（凭证与到期时刻比对）→ 有效者强制释放 + 队首通知
- [x] 8.4 过期后旧凭证的释放/续租请求返回无效
- [x] 8.5 测试：§10.1 租约用例组（手工时钟推进到期、续租延长、过期拒绝）
- [x] 8.6 验证：相关用例组全绿

## 9. 会话管理（P1-09）

- [x] 9.1 `SessionRegistry` 补全：`sessionId → 触及 key 集合`，授予/排队时登记
- [x] 9.2 `sessionClosed` 三类清理：写持有强制释放 + 通知、读持有移除、等待项摘除 + 队首前进检查；完成后移除会话记录；幂等
- [x] 9.3 测试：§10.1 会话用例组（断连清理持锁与等待、补通知正确、重复关闭幂等）
- [x] 9.4 验证：相关用例组全绿

## 10. 幂等与限额、M1 回归（P1-10）

- [x] 10.1 `(sessionId, requestId)` 队列内去重：已在队 → 返回排队结果与当前位次，不二次入队
- [x] 10.2 限额收口：队列深度超限拒绝；确认 key 校验与未知会话拒绝已在 5 覆盖并补对应用例
- [x] 10.3 并发压力用例：多线程竞争下不变量成立（授予顺序、无丢失、无重复入队、无孤儿等待者）
- [x] 10.4 M1 退出验证：`mvn -s /home/lam/repo/settings.xml verify` 全绿；§10.1 七类语义回归全绿；确认 core 模块零依赖、无网络调用
