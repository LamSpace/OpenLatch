# Tasks

红先纪律：组 1 全部**预期判红**并以输出留证——它们是根因假说的实证收口；若任一测试不判红或形态与预判不符（design D5/T1），停下回探索修正 spec delta，不带病进入修复组。

## 1. 确定性判红（根因实证，先红后修）

- [x] 1.1 服务端码形单测（`openlatch-server` raft 测试面）：向非权威角色的 `handleQueueOp` 喂 `QUEUE_OP`（SIZE 与 TAKE 各一）、并构造 `RetryableCommitException` 提交失败路，断言应答 `hasQueueOpResponse() ∧ status=NOT_LEADER ∧ op 回显`。**预期判红**（现拿到 type=QUEUE_OP、payload=acquire_response 的异型形态）——保存失败输出为根因证据
- [x] 1.2 同型单测覆盖 `LATCH_COUNT_DOWN`（提交失败路）与 `LATCH_AWAIT`（角色门路），断言各携同型 `*_response{NOT_LEADER}`。**预期判红**；同时验证 `handleLatchCountDown` 拒绝是否确实经 `notLeaderEnvelope`（design D5：不经过则修正 delta 范围并回报）
- [x] 1.3 客户端 Scripted 单测（`RemoteBlockingQueueScriptedTest` 车道）：注入 `type=QUEUE_OP`、`request_id` 匹配、载荷仅 `acquire_response` 的异型应答——断言 `take`/`size` 不交付 `""`/`0` 而按瞬态重发、至时限显式失败；另注入 `OK`-缺 `element_bytes` 的 TAKE 应答——断言显式协议违例异常。当前必伪成功，**预期判红**
- [x] 1.4 `ClusterHarness` 确定性 E2E（服务端模块测试面）：三节点起集群、经权威路 `put` 两元素、构造**直连钉死在已知 follower** 的客户端发 `size`/`take`——断言退避改道后读回 2、交付 "a"（拒绝窗必然命中、无时序竞态）。**预期判红**（现读 0/收空）

## 2. 服务端拒绝码形修复

- [x] 2.1 `ClusterRequestHandler.notLeaderEnvelope` 补 `QUEUE_OP`/`LATCH_COUNT_DOWN`/`LATCH_AWAIT` 三 case（沿 `ATOMIC_OP` 判例：同型载荷、`NOT_LEADER`、`op` 回显、无 leader 提示字段，design D1/D3）；default 分支加 WARN 日志以暴露未来漏配（D4）；同步修正该方法 Javadoc 的过时载荷枚举，并按 CLAUDE.md §5 同步触达面内全部受影响 Javadoc。验证：1.1、1.2 转绿；`handleQueueOp` 提交失败路径的 `queue.total{op,status}` 指标断言记为 `not_leader`（派生症状修复，Non-Goal 之历史不追改）

## 3. 客户端护栏

- [x] 3.1 `RemoteBlockingQueue` 的 `exchange`/`parkLoop`：应答解包处判 `hasQueueOpResponse()`——缺码形按瞬态车道裁决（重取 `latchRoute`、同信封重发，沿用既有退避口径，design D2）；`op==TAKE` 且 `OK`-缺 `element_bytes` 显式抛 `OpenLatchException` 协议违例（严格限定 TAKE，`PEEK`/`SIZE` 的 OK-缺省合法形态不误伤）；护栏收口私有方法不建跨车道抽象；同步 `parkLoop`/`exchange` Javadoc 与 `OBlockingQueue`/`ODelayQueue` 契约注释中的应答裁决表述。验证：1.3、1.4 转绿
- [x] 3.2 LATCH 车道对同型 `NOT_LEADER` 的分支归属验证（design D3 兼容注记）：读码确认 `RemoteCountDownLatch` 的 `doAwait`/`countDown` 对 NOT_LEADER 走既有瞬态/异常口径且无新失败形态，补 Scripted 用例锁行为（`latch_await_response{NOT_LEADER}` 注入 → 不假绿、不悬挂）。验证：新增用例绿 + 既有 latch 测试零回归

- [x] 3.3 换主窗改道收敛扩面判负与收口（用户批准方案 1）：`noteLaneNotLeader` 共享强制发现接线实测使锚点 4/6→1/6 绿（异步发现撞重发环 mid-flight 竞态），全量回退零残留（`RemoteBlockingQueue`/`OpenLatchClient` 复跑既有 Scripted+LeaderDiscovery 全绿）；改道承诺收窄为安全性质（spec 场景改写、锚双分支），残余缺口登记 WATCHLIST W11 带判负数据与候选设计（design D6 记录）

## 4. 防复发门禁

- [x] 4.1 表驱动码形门禁测试（server 测试夹具面，经 `ClusterHarness`/既有通道触达，不新增公开 API，design D4）：枚举客户端接入车道全部请求 `MessageType`（HELLO 除外），对单机 `errorResponse` 门控拒绝与集群非权威拒绝两类码形逐一断言应答携同型 Response 载荷且状态码正确；验证：当前全绿（修复后），且临时注释掉任一 case 可复现门禁红（自证防线有效，验后还原）

## 5. 锚恢复与收口

- [x] 5.1 `ClientClusterIT` 队列锚摘除 `@Disabled`，javadoc 与断言改为**安全性质双分支**定案口径（错值伪交付必红；收敛轮全链路 a→b→null→c→1，W11 驻留窗/启动提示滞后窗内任意请求以显式 `OpenLatchException` 收场即绿——全量复验后豁免面扩至整序列）；验证：`git diff` 无 `@Disabled` 残留、锚编译通过
- [x] 5.2 队列锚连续 6 轮全绿（安全性质口径——收敛轮与驻留窗显式失败轮皆为绿，对齐 W10 取证 3/6 红的轮次基线）；留各轮输出证据
- [x] 5.3 WATCHLIST.md 移除 W10 整行（维护规约"处理完从本表移除"），并顺手清掉该文件当前未提交的空行差异；验证：`git diff` 只含预期行删除、表格结构完整
- [x] 5.4 全反应堆验证：`mvn -s /home/lam/repo/settings.xml clean verify`（0 失败 0 错误、无跳过——W10 锚恢复后 skip 归零）+ `bash scripts/check-source-citations.sh` 零命中；spec delta 与主规格同步按归档流程执行
