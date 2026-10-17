# Tasks

## 1. 现场与红先证据

- [x] 1.1 `WATCHLIST.md` W21 行追加第二次命中现场：CI run `39012354774` 内层第一轮 `[CHAOS] kills=16 restarts=14 grants=118 conflicts=1`（`ClientChaosIT.java:267`，`expected: 0L but was: 1L`），第二轮 `kills=15 restarts=13 grants=106 conflicts=0` 绿；验证=行内容与 API 日志一致、表完整性复检（16 行、每行 6 竖线）。
- [x] 1.2 新增确定性反向夹具 `legalHandoffAfterHolderNodeDeathIsNotDoubleGrant`：**旧口径下必红**（`合法易手不构成双授: Expecting value to be false but was true`），改造后转绿；夹具同时断言服务端读数只含 B 且 stamp 一致。
- [x] 1.3 新观察登记 **W24**：立即式 acquire 在"不确定授予 + 会话换代"后以 `DENIED` 收场（实测 5/20，`heldRef` 证实持有者为旧会话同一 threadId）；立即式语义下重试即化解 ⇒ 归因不可辨，非契约违背。

## 2. 判据改造

- [x] 2.1 授予时点核对：按 `leaseToken` stamp 在存活节点上定位权威读数，holders ≥2 个不同 `Holder` 才计 `conflicts`；否则记 `handoffs`。验证=夹具 + 常规档无假阳性。
- [x] 2.2 周期全节点采样 `sampleDoubleHoldAway`：混沌每步遍历存活节点 `heldEntries()`，测试 key `holders().size() >= 2` 即计一次（同一步同 key 只计一次）。验证=20 轮常规档 `conflicts=0`。
- [x] 2.3 授予可核验性：5s 内找到匹配 stamp 的读数，否则 `unverified++` 并在收尾断言为 0。验证=20 轮 `unverified=0`。
- [x] 2.4 `[CHAOS]` 汇总行扩为含 `handoffs` 与 `unverified`；收尾断言保持 `conflicts == 0`。

## 3. 验证

- [x] 3.1 常规混沌档 **20 轮全绿**（`0/20 红`，各轮 `conflicts=0 handoffs=0 unverified=0`）；验证=轮次日志 `w21-class-final/`。
- [x] 3.2 全反应堆 `mvn -s /home/lam/repo/settings.xml clean verify` **BUILD SUCCESS**；citation gate（含 `--selftest`）零命中；验证=构建与门禁输出。

## 4. 收口

- [x] 4.1 `WATCHLIST.md` W21 行销账退役（编号留空、维护规约补记本 change 归档）；W24 保留在册；验证=**16 行**、每行 6 竖线、id 单调、全表引用回读通过。
- [x] 4.2 change 归档到 `openspec/changes/archive/2026-10-17-fix-chaos-conflict-authority/`；验证=`openspec validate --strict --all` 通过、`openspec list` 无活动 change。
