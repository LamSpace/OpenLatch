# Tasks

## 1. 现场留档

- [x] 1.1 `WATCHLIST.md` W2 行补记第二次命中（CI run `39019655682` 内层第一轮 `Failed to bind to address 0.0.0.0:36599` ⇒ `ClientSemaphoreClusterIT`）与该处改造；余下持有者保持登记；验证=行内容与日志一致、表完整性复检（17 行、每行 6 竖线）。

## 2. 改造该处

- [x] 2.1 `ClientSemaphoreClusterIT#startCluster` 的两处取口改用 `TestPorts.freeNonEphemeral()`，删除私有 `freePort()`。验证=切换后 20 轮**无端口类红**；其中 2 红为另行登记的 W25（`DIFFERENT_LEADER`，与端口无关）。
- [x] 2.2 **`TestPorts` 预留已发放端口**：切换后的首轮试跑实测 `n1`/`n3` 拿到同一 Raft 口（`28544`）⇒ 同批取样重号；实现 `HANDED_OUT` 预留并加 `TestPortsTest#handedOutPortsAreNeverRepeated`（200 次取样全不同）。验证=该单测 3/3 绿。
- [x] 2.3 反向验证（预留前状态即对照）：预留前 10 轮出现 1 次重号（`28544` 两节点同口）；预留后 20 轮**零重号**。验证=日志对比 `w2-csema/`（预留前）与 `w2-csema2`+`w2-csema3`（预留后）。

## 3. 验证

- [x] 3.1 全反应堆 `mvn -s /home/lam/repo/settings.xml clean verify` **BUILD SUCCESS**；citation gate（含 `--selftest`）零命中；验证=构建与门禁输出。

## 4. 收口

- [x] 4.1 change 归档到 `openspec/changes/archive/2026-10-17-harden-csema-it-port-allocation/`；验证=`openspec validate --strict --all` 通过、`openspec list` 无活动 change。
