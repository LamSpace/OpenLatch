# T4 验收证据（phase3-t4-security）

对齐详设 §7 测试设计与 §8 验收清单。验证命令基线：
`mvn -s /home/lam/repo/settings.xml`。本节列出的证据均来自本变更实施期间的真实运行。

## 机器已验证证据（自动化）

| 判据（详设 §7 T4 / §8-5） | 测试套件 | 结果 |
|---|---|---|
| 明文客户端被拒 | `TlsGatingTest.plaintextClientRejectedWhenTlsEnabled`（NotSslRecord → close，零会话） | ✅ 真 server 端口 0 |
| 正确证书通过 / mTLS 无证书被拒 | `TlsGatingTest.tlsClientHandshakeSucceeds` / `mtlsRejectsClientWithoutCert` / `mtlsAcceptsClientWithCert` | ✅ |
| 握手超时断开 | `TlsGatingTest.silentConnectionClosedByHandshakeTimeout`（<8s 断开，非 60s 读空闲） | ✅ |
| 坏证书配置启动失败 | `TlsGatingTest.badCertConfigFailsStartFast` | ✅ |
| TLS 配置解析/校验 | `TlsConfigTest`（8 例） | ✅ |
| 空/错令牌断连、同形不泄露、零会话 | `AuthHandshakeTest.authOnEmptyAndWrongRejectedIdenticallyNoLeak` | ✅ |
| 多令牌双活 / 摘旧令牌被拒 | `AuthHandshakeTest.authOnCorrectTokenSucceeds` / `removedTokenRejectedAfterRotation` | ✅ |
| 默认关兼容守卫（§8-6 行为不变） | `AuthHandshakeTest.authOffKeepsPhase1Guard` + 既有 `HandshakeTest`（10 例） | ✅ |
| 认证配置解析/命中 | `AuthConfigTest`（6 例） | ✅ |
| 常量时间原语（等长补齐、长度不等不早退） | `ConstantTimeTest`（4 例）；admin 迁移回归 `AdminProtocolTest`（15 例） | ✅ |
| 客户端 TLS/令牌消费（client） | `ClientTlsAuthIT`（6 例：TLS 正确/错 trust-store/mTLS 缺证书、携证书/认证对错令牌） | ✅ |
| 客户端握手/重连/builder 无回归 | `ClientHandshakeTest`(3)/`ClientReconnectTest`(4)/`OpenLatchClientBuilderTest`(5) | ✅ |
| starter 属性透传 / 零配置缺省（§8-6） | `OpenLatchAutoConfigurationTest`（10 例，含新增 2 例） | ✅ |
| console→TLS/认证节点；缺令牌节点降级 | `ConsoleTlsAuthTest`（2 例） | ✅ |
| 跨模块回归（默认关零扰动） | 全 reactor `-pl openlatch-server/openlatch-client/openlatch-console/openlatch-spring-boot-starter -am test`：server 04:09 / client 35.6s / console 14.7s 全 SUCCESS | ✅ |

## 验收清单（§8）逐项对照

1. ✅ FairLock 顺序回归常开——T1 已交付，未受 T4 影响（全量测试含 FairOrderingSuite 通过）。
2. ✅ Semaphore/CDL 语义 + 集群切换——T1 已交付；T4 变更未触 core（全量 server test 含集群档通过）。
3. ✅ 指标清单 + Prometheus 联调——T2 已交付；T4 未触碰 `ServerMetrics`（词表测试通过，见 T2 证据在案）。
4. ✅ 控制台冒烟 + 管理通道强制认证——T3 已交付；T4 console→TLS/认证冒烟 `ConsoleTlsAuthTest` 通过，管理令牌独立性由 `AuthHandshakeTest`/`ConsoleTlsAuthTest` 与既有 `AdminProtocolTest` 锁定。
5. ✅ 安全拒绝用例全绿（明文连接、错误凭证、未认证管理请求）——本变更套件：明文被拒、空/错令牌同形断连、mTLS 缺证书被拒、错误 trust-store 不建会话；未认证管理请求拒绝由 T3 `AdminProtocolTest` 既有矩阵 + `ConsoleTlsAuthTest` 缺令牌降级锁定。
6. ✅ 默认值关闭不改变既有行为——TLS/auth 均默认关：兼容守卫保留 Phase 1 规则（`AuthHandshakeTest.authOffKeepsPhase1Guard`、`HandshakeTest` 全绿）；全 reactor `test`（含 v1/v2 兼容与协议冻结用例）零失败。

## 说明 / 待运维环境执行（诚实标注）

- **集群档未认证 HELLO 零状态副作用**：认证门闩分叉在 `ServerSessionHandler.handleHandshake` 的
  `cluster.sessionCoordinator().handleHello`（SESSION_OPEN 复制）之前——代码路径结构保证 +
  单机档零会话副作用锁定；`ClusterHarness` 直驱 `ClusterRuntime` 不经该门闩，集群级三节点
  OpenLatchServer 夹具另立小变更评估（见 change tasks 3.6 注记）。
- **L3 手工走查 / `openssl s_client` 抓取 / 真多进程重启滚动**：本环境为无 TTY 沙箱，未作为
  本证据执行；步骤见 design.md D9 与 README 安全章节，运维/CI 环境执行后补图补据。
- **`-Pdrill` 进程级演练**：按仓库收口纪律在本沙箱不可靠（进程拉起/杀-9 时序受限，clean-master
  A/B 判定为环境性），等价进程内故障演练含于全量 server test（LeaderFailover/MinorityQuorum/
  ClusterMetrics 等）通过。

## 复现命令

```bash
mvn -s /home/lam/repo/settings.xml -pl openlatch-server -am test -Dtest='TlsConfigTest,TlsGatingTest,AuthConfigTest,ConstantTimeTest,AuthHandshakeTest,HandshakeTest,AdminProtocolTest'
mvn -s /home/lam/repo/settings.xml -pl openlatch-client -am test -Dtest='ClientTlsAuthIT,ClientHandshakeTest,ClientReconnectTest,OpenLatchClientBuilderTest'
mvn -s /home/lam/repo/settings.xml -pl openlatch-console -am test -Dtest='ConsoleTlsAuthTest'
mvn -s /home/lam/repo/settings.xml -pl openlatch-spring-boot-starter -am test -Dtest='OpenLatchAutoConfigurationTest'
```
