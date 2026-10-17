# Tasks

## 1. 共享工具迁移

- [x] 1.1 `TestPorts`/`TestPortsTest` 从 `openlatch-client` 测试源码迁到
  `openlatch-server/src/test/java/io/github/lamspace/openlatch/test/`；类与
  `freeNonEphemeral()` 改 `public`，包名改 `io.github.lamspace.openlatch.test`。
  验证=`TestPortsTest` 3/3 绿（`/tmp/w2-testports.log` 与 surefire 报告）。
- [x] 1.2 `openlatch-client`/`openlatch-console` 的 pom 增加 `openlatch-server`
  `test-jar` 测试依赖；`ClientClusterIT`/`ClientSemaphoreClusterIT` 补 import。
  验证=`mvn -pl openlatch-client,openlatch-console -am install -DskipTests` EXIT=0。

## 2. 七处改造

- [x] 2.1 client 四处：`IdleNodeGracefulStopIT`、`ClientChaosIT`、
  `ClientProcessKillIT`、`ClientHandshakeTest` 改用 `TestPorts.freeNonEphemeral()`，
  删除私有 `freePort()` 与不再使用的 `ServerSocket` import。验证=`ClientHandshakeTest`
  3/3、`ClientChaosIT` 3 跑 1 跳过、`ClientProcessKillIT` 1/1、`IdleNodeGracefulStopIT`
  1/1（`-Pdrill`，69.85s）绿。
- [x] 2.2 `ScriptedServer` 改用 `TestPorts.freeNonEphemeral()`，删除私有实现与
  `ServerSocket` import。验证=代表使用方 `LeaderDiscoveryTest` 5/5 绿；其余使用方由
  全量 verify 覆盖。
- [x] 2.3 `ClusterHarness` 改用 `TestPorts.freeNonEphemeral()`，删除私有实现与
  `ServerSocket` import。验证=`ClusterMembershipTest` 3/3、`ClusterReplicationTest`
  12/12 绿。
- [x] 2.4 `ConsoleClusterTest` 改用 `TestPorts.freeNonEphemeral()`，删除私有实现与
  不再使用的 import。验证=`ConsoleClusterTest` 4/4 绿。

## 3. 验证

- [x] 3.1 全反应堆 `mvn -s /home/lam/repo/settings.xml clean verify` **BUILD SUCCESS**
  （总耗时 13:26，日志 `/tmp/w2-full-verify.log`）。验证=构建输出。
- [x] 3.2 受影响场景造载复跑（16 燃烧进程 × 3 轮 × 5 组）：13/15 绿；2 次红均为
  `ClusterReplicationTest#failoverKeepsExpiryDriving:313`（expected OK but was QUEUED），
  全部 15 轮 `bind_errors=0`。`8fe4fad` 基线等量负载 3 轮中 2 轮同断言红（另 1 次
  `queuedPathDoesNotWriteLog:125`），确认非 W2 改造引入；已登记 W26/W27。
  验证=`/tmp/w2-load/summary.txt` 与 `/tmp/w2-baseline-load/`。
- [x] 3.3 `bash scripts/check-source-citations.sh` 与 `--selftest` 零命中。
  验证=门禁输出（citation=0、selftest=OK）。

## 4. 收口

- [x] 4.1 `WATCHLIST.md` 删除 W2 行、维护规约补记销账，并登记造载新观察 W26/W27；
  验证=数据行 17、每行 5 列，W2 仅出现在维护规约销账句。
- [x] 4.2 change 归档到 `openspec/changes/archive/2026-10-17-harden-w2-remaining-port-samplers/`；
  `openspec validate --strict --all` 通过、`openspec list` 无活动 change。验证=命令输出。
