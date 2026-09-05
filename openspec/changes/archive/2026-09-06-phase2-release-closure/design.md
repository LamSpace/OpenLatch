# Design: phase2-release-closure

## Context

Phase 2 十九子任务的实现与常规回归证据已在 S1–S4 归档收口（详见各 `s*-exit-checklist.md`）；本 change 只处理四笔收口账（见 proposal.md - Why）。约束：

- `PartitionDrillIT` 已按宿主语义写完（`ip netns` + veth + 网桥 + `iptables FORWARD` 双向隔离少数派、接入面保留观察通道；`openlatch-client/src/test/java/.../PartitionDrillIT.java`），特权探测用 `sudo -n true`，不满足即 `assumeTrue` 显式跳过——**代码零改动即可跑**，缺的只是特权；
- 本机 `sudo -n true` 失败（需交互认证），`ip`/`iptables` 均在 `/usr/sbin`，docker 可用；
- `ClientChaosIT` 现行为 ~18s 短租约有界混沌窗口（随机杀/重启 + 不变式检查器），负载时长为内建常量语义，无系统属性入口；
- S3 检查单与滚动重启报告均警示"reactor 刚退出的热机下崩溃选举对 CPU 争抢敏感"——凡计时/错误率类证据须静息态执行。

## Goals / Non-Goals

**Goals:**
- §11-3 主轨证据落地为 `docs/partition-drill-*.md`，或以定夺记录豁免——二选一，结果三态收口（D3），不留静默 ⏳；
- 详设 §10 字面 ≥10 分钟混沌 soak 有一轮真实执行数据入验收报告；
- 详设 §13.3 记账补齐（v1.5）；
- 验收报告镜像 Phase 1 先例结构：`§11 验收标准逐项闭环`（七项各含判据/证据路径/结果）+ `基准数据汇总` + `遗留与偏差记录`，外加总体计划 §5.4 DoD 五条款自检。

**Non-Goals:**
- 除 D8 关停缺陷修复外不改产品代码运行时行为（原"core/server/protocol 零触碰"口径因 3.1 首跑暴露 P0 关停缺陷、按 D3 预案扩入修复范围——修复决策见 D8）；`ClientChaosIT` 仅加缺省语义不变的开关；
- 不做 CI 特权化（主轨维持"按需 + 显式门"，不并入默认构建——沿 S4 `-Pdrill` 辅轨 profile 口径）;
- 不回补 S1 "评审中"措辞等 Phase 2 之前的记账（raft-selection 报告已由 S2 起采用 Ratis 的事实定案）；
- 不重跑已归档 S1–S4 的既有证据（引用路径即可）。

## Decisions

### D1 特权获取级联：宿主 sudoers drop-in 首选 → docker `--privileged` 回退 → D7 豁免裁决穷尽

IT 内部全部特权调用经四条命令（`ip`、`iptables`、探测用 `true`、D6 新增 `modprobe br_netfilter` 参数钉死；java 进程本身是经 `sudo ip netns exec` 拉起的），故最小授权面为一次性 drop-in `/etc/sudoers.d/openlatch-drill`：

```
lam ALL=(ALL) NOPASSWD: /usr/sbin/ip, /usr/sbin/iptables, /usr/bin/true, /usr/sbin/modprobe br_netfilter
```

（`true` 为 `assumePrivileges()` 探测命令；`modprobe` 以参数钉死仅允许加载 `br_netfilter`，防授权面扩大为任意模块。）

由用户以 `! sudo visudo -f /etc/sudoers.d/openlatch-drill` 交互写入（我不能代输密码）；演练闭环后 `sudo rm` 撤销（任务列明）。

**为何不是 `sudo -v` 凭据缓存**：sudo 默认 `tty_tickets`，缓存按登录终端记账——用户终端 `sudo -v` 刷新的票不保证被我 Bash 工具的无终端子进程命中，且 15 分钟窗口对含构建的演练链路过脆。**为何把 docker 降为回退**：`--privileged` 容器虽可满足 `ip netns`/iptables，但镜像需自备 JDK 25 + 挂载 shaded jar，且容器 netns 与宿主 netfilter 的交叠使"真分区"证据的环境纯度存疑（报告需额外论证容器网络栈语义）；宿主路径是 IT 的写作对象，证据零解释成本。**为何豁免仍是穷尽项而非首选**：D7 自设"主轨为准"，一次可重跑的真演练同时满足字面判据并消掉口径债；豁免裁决留永久证据降级注记，仅在 a1/a2 均物理不可达时启用，且启用即在验收报告记录论证链（辅轨已证少数派无法授予 + Raft 多数派论证：分区少数派不具 quorum ⇒ 不可提交 ⇒ 不可授予）。

`ip`/`iptables` 以 root 等价于网络层全权——授权窗口尽量窄（写入→跑完→撤销在同一工作段内完成）。

### D2 soak 开关：`-Dopenlatch.chaos.soak-minutes`，缺省语义逐字节不变

- 未设或非正值 → 现行 ~18s 短窗口回归路径，常规 `-Pdrill` 门禁时长与断言零变化（防回归）；
- 设 `N > 0` → 负载驱动改为**墙钟预算驱动**：以 wall-clock 截止持续"随机杀节点 + 重启 + 共享 key 竞争"≥ N 分钟，随后不变式检查（同 key 至多一写持有者）与排空断言（停载 + 一个租约期后副本锁表空）原样执行；
- 补跑执行：静息态、`-pl openlatch-client` 独立单跑（`verify -Dit.test=ClientChaosIT -Dopenlatch.chaos.soak-minutes=10`；本类无 `@Tag("drill")`，属默认 failsafe 回归，不加 `-Pdrill`——加了反而被 groups 过滤），热机瞬态会扭曲计时面证据（S3 观察在案）。

**备选否决**：新增长版 IT 类（复制驱动逻辑，双份维护）；Test 注解 timeout（不作用于负载循环）。属性名对齐 IT 现有 `openlatch.*` 配置命名系。

### D3 主轨结果三态收口，禁止静默 ⏳

| 执行结果 | 收口 |
|---|---|
| 绿 | §11-3 以主轨关闭；详设 §11/§13.4 P2-18 行由"⏳ 待补跑"改"✅（主轨 `partition-drill-*.md`）"；s4 检查单不改（归档不可篡改），收口以新报告为准 |
| 红且归因产品缺陷 | 启用缺陷预案：撤销本 change `skip_specs`、补 delta spec 修复、重跑至绿——带病不发布 |
| 环境穷尽（D1 级联 a1/a2 均不可达） | 启用 D7 备选裁决：辅轨 + 多数派论证豁免，报告"遗留与偏差"记录理由与论证链，详设对应行写"豁免（定夺人/日期）" |

### D4 详设 v1.5 回写口径

镜像 v1.3 对 S2 的补回写模式：§13.3 段标补"（已完成，v1.5 回写）"、四行验证列逐项 ✅ + 证据路径（`openspec/changes/archive/2026-08-31-s3-leader-discovery-failover/`、`docs/failover-drill-2026-08-31.md`），并补一句"实现回写见 v1.2"以对齐历史；§11 第 3 项、§13.4 P2-18/P2-19 行、§10 混沌行按 D3/D2 结果同步；版本表升 v1.5、修订记录补条目、状态行"待评审"改判。既有 v1.2 已定稿的 §3.2/§6 等实质内容不动——只补记账，不改契约。

### D5 演练工具适配三处（写码期预演发现，S4 只验过跳过路径的欠账到期）

`PartitionDrillIT` 的非跳过路径在 S4 从未真实执行，静态预演暴露三处"执行即翻车"的环境适配缺陷（均为演练工具层，非产品缺陷，`skip_specs` 不受影响）：

1. **`br_netfilter` 未加载 ⇒ 分区规则形同虚设**：同桥段二层交换帧默认不进 iptables FORWARD 链，本机 `/proc/sys/net/bridge` 不存在（模块未载）——不加载则 iptables DROP 拦不住少数派。修复：`setupTopology()` 首步 `modprobe br_netfilter`（模块加载即 `bridge-nf-call-iptables=1`）。不采用"子网分路由使流量走真 FORWARD"的替代：拓扑改写面大且偏离 S4 原设计意图。
2. **桥无宿主侧地址 ⇒ 观察通道无源可发**：宿主测试 JVM 连 `10.199.0.x` 时桥段上无本地源地址，SYN 无法成帧（端口探测/接入直连全灭）。修复：`ip addr add 10.199.0.254/24 dev olbr`。宿主自发流量走 OUTPUT 链不受分区 FORWARD 规则影响，观察通道保真。
3. **裸 `java` 在 sudo 语境不可达**：`sudo ip netns exec` 下 PATH 为 `secure_path`，不含用户目录 SDKMAN 部署。修复：取 `ProcessHandle.current().info().command()` 绝对路径（兼得"与测试进程同 JDK"），root 豁免用户目录遍历权限，jar/配置直读无障碍。

配套：模块加载与 `10.199.0.254` 地址随 `teardownTopology()` 删桥一并消失（地址随设备销毁）；`br_netfilter` 模块驻留可接受（加载幂等、不改宿主既有桥语义之外的行为），不列撤销义务。

### D6 验收报告证据链以引用汇总，不复制正文

七项逐项列判据/证据路径/结果（复用四份检查单与三份演练报告的原文数据，标注执行日期与轮次）；基准汇总段集中杀主 P99/恢复、快照大小/序列化/落盘/恢复、滚动重启双顺序错误率、soak 结果、分区主轨结果五组数字；"遗留与偏差"如实收录：滚动重启 R2 离群轮（4 轮未复现）、chaos 常规回归为短窗口（10 分钟补跑单轮后注明"字面达标一轮，常规回归为短窗口"）、详设 v1.x 演进史。发布宣告在报告尾注（版本、日期、对应 commit）。

### D8 关停缺陷修复（3.1 首跑触发 D3 预案：产品缺陷态）

soak 首跑在负载窗口 ~7 分钟处挂死（60s 双 jstack 栈位零移动），归因为 Ratis 3.3.0 优雅关停链缺陷：`RaftServerProxy.close` 以 fire-and-forget 把组关停派发进 cached proxy 池，而该池启动任务后 60s 即零 worker——此后任何优雅 stop 的派发可能永不被执行，同线程 `shutdownAndWait` 挂 1 天。机制链、触发算式与全部源码取证见 `observations-ratis-3.3.0-soak-shutdown-hang.md`；生产影响定级 **P0**（空闲集群的优雅停机必中，数据面与 SIGKILL 故障转移面不受影响）。

修复取**装配层钉死三组线程池为非缓存固定池**（`RaftSubsystem.start()`：proxy/server/client cached=false、size=4）——从构造上消除"零 worker 时刻派发"前件，兼防 `RaftServerImpl.close` 内同构的 client/server executor `shutdownAndWait`；不取看门狗强断（半途强断会留下未收口的状态机更新器与非守护线程，正确性劣于挂起）。规格化入 `cluster-node-lifecycle` delta（"长空闲后优雅关停有界"场景），回归 `IdleNodeGracefulStopIT`（drill 档，静默 65s 使触发前件确定化）。

按 D3 预案同步撤销本 change 的 `skip_specs`。soak 复跑（3.1）兼作发布证据与关停全链复核；上游 3.3.1 若发布可复核是否根治再评估解除钉死。

### D9 leader 复制停摆的归因与处置（6.1 第二腿现场捕获，存量 P1）

`-Pdrill` 全套首跑中 RollingRestartDrillIT 先主后从序红（26.02%，新 leader NOOP 永不提交、写门闸 211s 不自愈）。差分实验（含 `git stash` 摘除 D8 钉池重建原 jar 复跑，仍中）证明**存量缺陷非 D8 引入**——S4 归档"24.22% 瞬态离群、4 轮未复现"的定判据由此更正：同一签名，双稳态，机器热态调概率。完整取证、频率矩阵与跟进计划见 `defects/leader-replication-stall-ratis-3.3.0.md`。

处置：本 change 不修复（正确性无损、根因在库内，独立 change 立项——含产品侧 leader 自愈看门狗、Ratis 3.3.1 升级跟踪、运维"先从不先主"推荐序入部署文档）；§11-5 验收行按实记录（09-02 双序绿证据 + 本档案复现史），以 DoD §5.4-3"P1 在案+跟进计划"承载；发布与否为验收报告层面的显式定夺，不由收口流程默认吸收。

## Risks / Trade-offs

- [演练期间宿主并发负载造成瞬态误判] → 静息态执行 + 失败轮次如实入库并复跑判定（沿 failover-drill 的取证模式），单轮红不直接归因产品；
- [sudoers drop-in 遗忘撤销] → 撤销列入任务清单且排在验收报告之前；报告记录授权窗口存在时段；
- [10 分钟 soak 暴露短窗口从未触发的罕见时序缺陷] → D3 缺陷预案即为此设，非风险即目的——发布前暴露优于发布后；
- [netns/iptables 残留污染宿主网络栈] → `teardownTopology()` 尽力清理已内建；任务加一道兜底核验（`ip netns list` 无 `oln*`、`ip link` 无 `olbr`）；
- [豁免路径证据口径永久降级] → 故置于级联末端；且豁免一旦采用，遗留段必须写明"后续获得特权环境时应补跑主轨"为技术债。

## Open Questions

- 静息态补跑窗口（soak 10 分钟 + 主轨演练约 15–20 分钟）依赖本机届时无其他负载——执行时定，不影响任务拆分。
