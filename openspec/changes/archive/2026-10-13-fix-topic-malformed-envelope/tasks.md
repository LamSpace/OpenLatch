# Tasks

## 1. 红先：确定性复现

- [x] 1.1 扩展 `RemoteTopicScriptedTest`：同步 `publish` 的 `TOPIC_OP` 应答**缺码形**（`type` 回显而载荷 oneof 缺 `topic_op_response`）；断言**不返回 `topic_seq=0`**、按同 `op_seq` 重发。验证=未改实现前**红**（`topic_seq=0` 被当受理返回）。
- [x] 1.2 同夹具覆盖 `publishAsync`：断言 `future` **异常完成**（非以 `topicSeq=0` 正常完成），且 `countType(TOPIC_OP)` 不增长（不重发）。验证=未改实现前**红**。
- [x] 1.3 同夹具覆盖 `subscribe`：断言**不绑定 `subscription_id=0`**、按幂等重投。验证=未改实现前**红**（绑到 id 0）。
- [x] 1.4 同夹具覆盖 `rebindAsync` 重挂路径：断言既有绑定**不被拆除**、不错绑默认 id。验证=未改实现前**红**。

## 2. 实现：presence 判定（按 design D1 分流）

- [x] 2.1 `executePublish`：读 `status` 前增 `!answer.hasTopicOpResponse()` → `continue`（同信封同 `op_seq` 重发）。验证=1.1 由红转绿；`ClientTopicIT` 全绿。
- [x] 2.2 `publishAsync`：增判定 → `future.completeExceptionally(...)`（至多一次不可重发，D2）。验证=1.2 由红转绿。
- [x] 2.3 `executeSubscribe`：增判定 → `continue`（幂等重投，D4）。验证=1.3 由红转绿。
- [x] 2.4 `rebindAsync`：增判定 → `return`（放弃本次重挂，D5）。验证=1.4 由红转绿。
- [x] 2.5 方法级 Javadoc 同步（各路径缺码形处置，"不得读默认实例"）。

## 3. 契约与文档

- [x] 3.1 接口级 Javadoc：`OTopic` 声明缺码形处置（同步重发、异步显式失败、重挂放弃）。`OTopicSubscription` 无受影响读点（`unsubscribe` 为 fire-and-forget、不读应答），故不虚改。验证=`verify-javadoc` 在全仓 `clean verify` 内逐模块通过（`show=private`）。
- [x] 3.2 双语排障指南：`docs/guide/{zh,en}/09-troubleshooting.md` 的 topic 排查节增一条排查项——"`publishAsync` 报受理却无交付 / `subscribe` 后无消息"→ 复核是否缺码形假受理/错绑；该族现按缺码形重发或显式失败、不再假成功。验证=中英双侧对应段落均已更新、语义一致。

## 4. 集成回归

- [x] 4.1 客户端全量套件（单测 + IT）复跑。验证=零失败、无 flaky。
- [x] 4.2 跨模块收口：全仓 `clean verify` 与引用门禁自查。验证=`mvn -s /home/lam/repo/settings.xml clean verify` 通过，`bash scripts/check-source-citations.sh` 零命中。
