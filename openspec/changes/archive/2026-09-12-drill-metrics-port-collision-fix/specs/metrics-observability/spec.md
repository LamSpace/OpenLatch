# metrics-observability Delta Specification

## ADDED Requirements

### Requirement: 同宿主多节点测试夹具管理端口隔离

单机同时拉起多个服务端节点的进程级测试夹具（故障演练、混沌测试等），其每节点配置 MUST 显式设置互不相同或操作系统分配（`openlatch.server.metrics.port=0`）的管理端口，MUST NOT 依赖默认 9412 在多实例间共存（默认端口按 fail-fast 规格占用即整机启动失败，多节点必失多数派）。夹具 MUST 保持管理 HTTP 的真实绑定路径（以临时端口而非关闭指标实现隔离），使指标端点行为在被测链路上依然成立。

#### Scenario: 三节点同宿主演练可启动

- **WHEN** 进程级演练以 shaded jar 在单机拉起三节点集群
- **THEN** 三节点全部完成启动并就绪（业务/raft/管理端口零冲突），集群在选举超时内产生 Leader

#### Scenario: 滚动重启后管理端口不冲突

- **WHEN** 演练逐台重启节点（配置模板含 `metrics.port=0`）
- **THEN** 重启节点获得新临时管理端口，与存活节点无冲突
