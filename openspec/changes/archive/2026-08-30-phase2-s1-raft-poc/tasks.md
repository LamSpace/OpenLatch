## 1. P2-01 PoC 环境与共享骨架

- [x] 1.1 建 `poc/raft-selection/` 独立 reactor（poc-harness / poc-ratis / poc-jraft 三子模块，parent 指根 pom 不入 `<modules>`）；`mvn -s /home/lam/repo/settings.xml -pl openlatch-core -am install` 后 `mvn -f poc/raft-selection/pom.xml verify` 编译通过
- [x] 1.2 poc-harness 定义 `PocNodeAdapter` SPI（start/stop、propose→CompletableFuture、isLeader、onLeaderChange、triggerSnapshot、installState、digest）与节点行协议服务端（ACQ/REL/OPEN/STAT/SNAP/DUMP）
- [x] 1.3 PoC 本地 `raft.proto`（§4.2 子集：SESSION_OPEN/LOCK_ACQUIRE_ENTRY/LOCK_RELEASE_ENTRY/NOOP + RaftLogEntry），生成代码编译通过
- [x] 1.4 driver：负载脚本（单键 acquire+release 混合、5 分钟、延迟蓄水池）、计时器（双段杀主）、不变式监视器、results JSON schema 落盘
- [x] 1.5 no-raft 伪节点适配器（SPI 直通 CoreEngine + EntryClock），一键脚本 `poc-run.sh <candidate> <round>`
- [x] 1.6 `EntryClock`（apply 线程 thread-local 条目时刻，回落系统时钟）+ 影子状态表（shadow map）工具类 + 确定性回放单测（同序列两次应用逐字段一致）
- [x] 1.7 Java 25 启动冒烟：ratis-server（gRPC）与 jraft-core（bolt）各起 3 节点 echo 状态机完成选主（D8 风险项首检）
- [x] 1.8 验证 P2-01 退出：任一库 echo 集群跑通 `poc-run.sh` 全脚本骨架 → 记录"任一库可组集群并完成选主"

## 2. P2-02 Ratis 接入原型

- [x] 2.1 `RatisNodeAdapter`：RaftServer 生命周期、gRPC 传输、`StateMachineAdapter` 挂 CoreEngine（EntryClock 注入）、`RaftLogEntry` ↔ ByteString 转换
- [x] 2.2 Leader 路径：客户端行协议 → `RaftClient.io()` 提案 → apply 完成回执；NOOP 当选确认；杀主后锁保留路径
- [x] 2.3 快照：`notifySnapshot/StreamSnapshot` 保存影子表二进制 + `DUMP` 摘要比对；日志压缩触发生效
- [x] 2.4 功能跑通：echo 升级为锁语义冒烟套件（组网/授予/互斥/杀主/快照追赶）全绿；cloc 记录胶水 LOC，开摩擦日志

## 3. P2-03 SOFAJRaft 接入原型

- [x] 3.1 `JRaftNodeAdapter`：NodeManager/RaftGroupService 生命周期、bolt 传输、`StateMachine`（onApply/Task 完成回调）挂 CoreEngine（EntryClock 注入）
- [x] 3.2 Leader 路径：行协议 → `Node.apply(Task)` → FSMCaller 应用回执；`LeaderObserver`/election 事件映射 NOOP 语义；杀主后锁保留路径
- [x] 3.3 快照：`onSnapshotSave/onSnapshotLoad`（file writer/reader）影子表二进制 + `DUMP` 比对；`truncate` 后追赶生效
- [x] 3.4 功能跑通：与 2.4 同一冒烟套件全绿；cloc 记录胶水 LOC，开摩擦日志（重点：protobuf/bolt shading 冲突清单）

## 4. P2-04 对比评估与选型定案

- [x] 4.1 配置冻结检查单：两库默认参数表 + 对称手动项登记（D7），确认无单侧调优
- [x] 4.2 四组实验 × 两候选 × 3 轮 A/B/A/B：基准 P99（含 no-raft floor）、杀主双段计时 + 锁保留断言、10 万条目快照追赶（`DUMP` 全量比对）、不变式零违例；results JSON 入库 `poc/raft-selection/results/`
- [x] 4.3 门槛逐项判定表：P99 < 20ms、恢复 < 10s、快照 < 30s、core 零改动（git diff 证据快照）；违例轮次标注
- [x] 4.4 §2.2 权重维度定性评分（状态机集成/快照/传输层/性能/成员变更桌面调研/社区维护）+ 侵入度档案并表
- [x] 4.5 输出 `docs/raft-selection-report.md`：逐项数据 + 原始 JSON 路径引用 + 淘汰/定案结论；Java 25 兼容风险项（如有）单列
- [x] 4.6 详设回写 v1.1：§2.1 定案与理由、§12 风险 4 关闭、修订记录；报告 + 详设一并提请评审 → S1 退出
