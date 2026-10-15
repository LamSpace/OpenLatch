# Tasks

## 1. 判据先行（本 change 的红先形态）

- [x] 1.1 取样器单测：`TestPorts.freeNonEphemeral()` 连续取样 20 次，断言每次都落在文档化的非临时带 `[20000, 29999]`。验证=该断言对**旧取口法**（`new ServerSocket(0)`）为红、对新取样器为绿（旧法实测 50 次采样 in-band 计数 = 0，证据见 tasks 附注）。→ 实测：旧法 `inBand[20000,29999]=0`（min=32869 max=46657）⇒ 该断言对旧法必红；新取样器 `TestPortsTest` 2/2 绿
- [x] 1.2 策略有效性断言（OS 无关）：取内核自动分配的口 50 次（`new ServerSocket(0)`），断言**无一**落在取样带内；相交即成显式红灯（提示换带），而不是留作偶发 flake。验证=该断言在本机为绿（内核口实测 32869–46657），且不读 procfs（沙箱下 procfs 不可读）。→ `TestPortsTest#kernelAllocatedPortsDoNotFallInsideSamplingBand` 绿；判据以"实测内核样本"替代读 procfs（本沙箱 JVM 读该文件得 `"3"`）

## 2. 实现：取样器与红点改造

- [x] 2.1 新增测试树包私有取样器 `TestPorts`：带常量 + 随机候选 + `setReuseAddress(false)` + 至多 100 次换候选，带完整 Javadoc（含"为什么必须避开临时池"的机制说明）。验证=`TestPortsTest`（1.1/1.2）全绿。→ `TestPorts.java` 新建；`TestPortsTest` 2/2 绿
- [x] 2.2 `ClientClusterIT.freePort()` 改为委托 `TestPorts.freeNonEphemeral()`（**只改这一处**，其余 W2 持有者不动）。验证=该类的集群装配用例（含本次 CI 红的那两枚 `acquireSucceedsThroughSeedFollowerAndRedirectsOnLeaderKill`、`waiterRequeuesOnNewLeaderAfterFailover`）连续 ≥2 轮全绿。→ 私有 `freePort()` 与其失效的 `ServerSocket` 导入一并摘除（两处调用点直连取样器）；`ClientClusterIT` 连跑 2 轮 **9/9 绿**（53.61s / 前轮同绿）

## 3. 文档与治理

- [x] 3.1 `TestPorts` 类 Javadoc 自洽：机制（探针-重绑窗口为何不可消除、为何"带外取样"能移走竞争者）、与 drill 侧同款实现的关系。验证=`bash scripts/check-source-citations.sh` 零命中且注释只读自洽。→ 类级 Javadoc 覆盖机制/治法/`setReuseAddress(false)` 之由/有效性判据；citation 门禁零命中
- [x] 3.2 `WATCHLIST.md` W2 行：登记本次两次 CI 出现（`ClientClusterIT.startCluster:115`，端口 43869/43937）、改造内容与"余下持有者保持登记、红一次改一处"的口径。验证=行内容与实测一致；表完整性复检（行 id 集合不变、每行 6 竖线）。→ 已登记（含确定性红证据的机制描述）；**表复检时抓到并修复一处**：我的 `old_string` 带走了列尾 `|` 致两列粘连（5 竖线），补回后复检 17 行、每行 6 竖线、行 id 集合不变

## 4. 收口

- [x] 4.1 客户端 IT 相（含 `ClientClusterIT`、`ClientSemaphoreClusterIT`、`SessionSwitchReproIT` 等本次不动的持有者）连续 ≥2 轮全绿。验证=各轮报告。→ 连跑 2 轮 **98/98 绿**（各 1 跳过＝既有）；逐类抽检：`ClientClusterIT` 9/9、`ClientSemaphoreClusterIT` 2/2、`SessionSwitchReproIT` 3/3
- [x] 4.2 `mvn -s /home/lam/repo/settings.xml clean verify` 全反应堆通过（0 失败 0 错误）。验证=构建输出。→ **BUILD SUCCESS**，8/8；客户端 **145 单测**（含新增 `TestPortsTest` 2 枚）+ **98 IT**（0 失败 1 跳过）
- [x] 4.3 门禁：citation 零命中、links 零死链。验证=命令退出码。→ 两脚本退出码均 0

## 附注（判据来源）

旧取口法的确定性红证据（本机实测，临时探针，用完即删）：

```
PROBE legacy samples=50  min=32869  max=46657  inBand[20000,29999]=0
```

即：旧法取样**恒在内核临时带内**（本机 32768–60999），与"探到的口刚被内核回收、紧接着被同机并发 connect 拿走"完全吻合——CI 上 `startCluster` 的 `Address already in use` 即此机制显形。

**遗留说明**：drill 侧（`LeaderKillDrillIT`）有一份同策略的私有实现，本 change 按"红一次改一处"未动它；两处并存为已知状态，待其自然接触时再合并。
