# Tasks: deepen-javadoc-contract-detail

深化内容以 design.md D4 大纲为准；写注释前必须读对应实现，分支语义以代码为准。

## 1. 准备

- [x] 1.1 摸底：`mvn -s /home/lam/repo/settings.xml clean verify` 确认当前全绿
- [x] 1.2 根 `pom.xml` 的 maven-javadoc-plugin 配置增加 `<show>private</show>` → verify: 告警清单与计划补齐的私有成员完全对应，无意外告警（实施发现：`missing` 类别要求全部私有成员含字段均有注释，已修正 design D3；字段注释随 2.x/3.x 任务补齐后告警清零）

## 2. core 模块深化

- [x] 2.1 `CoreEngine` 类级与 `acquire`/`release`/`renew`/`sessionClosed`/`expireDue`/`sweepNotifiedHeads` 深化（线程模型、校验顺序、惰性到期契约、陈旧校验、幂等性，design D4）+ private 方法 `clampLease`、`fireNotify` 补详细 Javadoc → verify: core 组完成后统一运行（见 2.4 后）
- [x] 2.2 `LockEntry` 类级状态机总述与 `acquire`/`release`/`renew`/`forceExpire`/`sweepNotifiedHead`/`removeSession` 规则自足化 + private 方法 `grant`、`clearLease`、`notifyHeadIfPossible`、`compatibleWithHold` 补详细 Javadoc → verify: 同上
- [x] 2.3 `LockType` 类级补兼容性矩阵、重入归属与"同 key 同类型"约定，各常量补语义 → verify: 同上
- [x] 2.4 `Outcome` 与 `ReleaseStatus` 各常量补"在哪一校验步骤、以什么优先级返回" → verify: 同上

## 3. server 模块深化

- [x] 3.1 `ServerSessionHandler` 类级补连接生命周期状态机与处理矩阵、`markClosed` 一次性清理契约与清理顺序原因 + private 方法 `handleHandshake`、`helloResponse` 补详细 Javadoc → verify: server 组完成后统一运行（见 3.4 后）
- [x] 3.2 `OpenLatchServer` 类级补线程模型（IO 线程同步执行业务、扫描单线程、通知回调线程来源）+ private 方法 `startScheduler` 补详细 Javadoc → verify: 同上
- [x] 3.3 `ServerConfig` 的 private 方法 `defaultWorkerThreads`、`intOf`、`longOf`、`validate` 补详细 Javadoc → verify: 同上
- [x] 3.4 `RequestDispatcher` 的 private 方法 `dispatchAcquire`、`dispatchRelease`、`dispatchRenew`、`envelope` 补详细 Javadoc；`ServerBootstrapFactory` private 构造器补用途说明 → verify: server 组 `verify` 全绿（含 6 个简洁文件按修正后 D3 补字段注释）

## 4. 逐一确认（不深化文件）

- [x] 4.1 按 design 附录 A 逐一核对 23 个"维持简洁"文件：确认注释与代码语义一致，发现失真或标签小疵顺手修正，不重写 → verify: 探索期全量读过 + 实施期逐一触达字段告警；除按修正后 D3 补字段一句话注释外无内容修改
- [x] 4.2 核对 18 个测试文件维持上轮标准（类级注释 + 夹具方法标签），不改动 → verify: 18/18 文件类级注释存在（机械核验），零改动

## 5. 收尾验证

- [x] 5.1 全量验证：`mvn -s /home/lam/repo/settings.xml clean verify` 全绿（78 测试通过 + javadoc 零告警，含 `show=private` 范围）
- [x] 5.2 反向验证防回归：删除 `clampLease` 的 `@param` 后 `verify` 失败（`no @param for requested`），恢复后全绿
- [x] 5.3 归档前自查：7 个深化文件的 Javadoc 满足"只读注释即可懂契约"；残留设计文档引用 6 处均为"详见"式深入指引（规则本身已自足写出），另有 3 处内联注释引用不在本条标准范围
