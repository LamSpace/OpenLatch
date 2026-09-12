# fault-drill-harness Delta Specification

## ADDED Requirements

### Requirement: 演练夹具端口分配纪律

凡进程级测试夹具需为**子进程**预留监听端口的，其端口分配 MUST 避开操作系统临时端口带（Linux 为 `ip_local_port_range`）——"探测-关闭-延后重绑"的探针模式与内核出站源端口抽取共享该带时，存在端口在探针关闭后、子进程 bind 前被同机连接占为源端口的竞态（EADDRINUSE），预留端口取带外固定窗口可使该竞态从根消除。探针 MUST 以禁用地址复用的严格 bind 验证可绑定性（不得放行 TIME_WAIT 残留端口）。同一夹具内多次抽样的端口 MUST 去重。带外窗口取值 MUST 有界随机并带重试上限，全窗口不可得时以明确错误失败。

#### Scenario: 子进程端口不被自家连接抢占

- **WHEN** 演练在同一 JVM 内同时维持大量客户端/gRPC 出站连接，且逐个拉起三节点子进程集群
- **THEN** 各节点的业务端口与 Raft 端口在其 bind 时刻必然可绑定，不出现探针通过后子进程 EADDRINUSE 启动失败

#### Scenario: 带外窗口不可得时显式失败

- **WHEN** 预留窗口内候选端口连续 100 次绑定失败
- **THEN** 夹具以明确异常终止（不静默回退到临时端口带内取样）
