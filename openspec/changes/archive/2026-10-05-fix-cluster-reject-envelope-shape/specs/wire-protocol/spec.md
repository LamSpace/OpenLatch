# Spec Delta: wire-protocol

## ADDED Requirements

### Requirement: 拒绝应答码形同型

客户端接入车道可见的拒绝应答 SHALL 与请求消息类型同型：`Envelope.type` 回显请求类型，且 `payload` MUST 携带该类型对应的 Response 消息并以对应状态码自述拒绝结果——MUST NOT 以其他类型的 Response 载荷或空载荷承载拒绝。本协议将既有工程纪律"拒绝状态码线路可见（客户端裁决依赖）"成文为不变式，覆盖接入层门控拒绝（版本/形状/钳制）、集群路径角色门拒绝（`NOT_LEADER`）与提交失败拒绝全部拒绝码形。`leader_node_id`/`leader_address` 提示字段仍仅由 v2 定义的三类应答（Acquire/Release/LeaseRenew）承载——`ATOMIC_OP`/`BARRIER_*`/`LATCH_*`/`QUEUE_OP` 的同型拒绝沿无提示字段判例，改道由客户端 Leader 发现与故障转移机制兜底。`StatusCode.OK` 为枚举零值的事实 MUST NOT 被任何拒绝路径间接利用：缺载荷应答在收发任一侧按 oneof 缺省解读时呈现 protobuf 默认实例即"成功空应答"，属码形违例。

#### Scenario: QUEUE_OP 被非 Leader 节点拒绝码形同型

- **WHEN** v7 会话向非当值 Leader 的集群节点发送任意 `QUEUE_OP`（含只读 op：SIZE/PEEK），该节点角色门拒绝或提交以在途可重试原因失败
- **THEN** 应答信封 payload 为 `queue_op_response`、`status = NOT_LEADER` 且 `op` 回显请求操作，不产生日志条目、连接保持；客户端对同型拒绝码执行既有退避改道

#### Scenario: LATCH 拒绝码形同型杜绝假绿

- **WHEN** `LATCH_COUNT_DOWN` 或 `LATCH_AWAIT` 被集群路径拒绝（角色门或提交失败）
- **THEN** 应答分别携带 `latch_count_down_response` / `latch_await_response` 且 `status = NOT_LEADER`——客户端 MUST NOT 可能读到默认实例形态的 `OK`（`remaining=0`/"屏障已破"假绿形态在协议层不可达）

#### Scenario: 全类型拒绝码形表驱动门禁

- **WHEN** 构建门禁枚举客户端接入车道全部请求 `MessageType`，逐一构造被各拒绝码形路径拒绝的应答并检查 payload
- **THEN** 每一类型的拒绝应答都携带与请求同型的 Response 载荷；任何新增消息类型未通过本门禁（无同型拒绝 case）即构建红
