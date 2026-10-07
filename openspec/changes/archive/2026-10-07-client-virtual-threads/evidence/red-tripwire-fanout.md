# 红先证据：OTopic 扇出平台线程数绊线（任务 1.2）

- 日期：2026-10-07（改动前 HEAD=080c237）
- 命令：`mvn -s /home/lam/repo/settings.xml -pl openlatch-client -am verify -Dit.test=ClientVirtualThreadsIT -Dtest=NONE ...`
- 结果：`Tests run: 1, Failures: 1`（BUILD FAILURE @ openlatch-client）
- 红因（精确钉住主断言）：
  `Assert "500 订阅扇出下平台线程增量（派发线程虚拟形态承诺）": Expecting actual: 500 to be less than or equal to: 100`
- 交付正确性断言先行通过（500×5 恰收、串行不重入、seq 升序、零丢弃），awaitUntil 于 60s 限内达成
- 插曲记录：首跑红因错位（(session,key) 幂等覆盖致同 key 多订阅只存续一路由，投递永不齐），
  扇出形态修正为 500 key × 1 订阅后重跑得到上述精确红。design D5 "每 key ≤64 摊 10 key" 前提据此更正。
