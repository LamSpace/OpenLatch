# Proposal

## Why

`ShadowTable.expireUpTo`(`LEASE_EXPIRE_ENTRY` 应用点的影子到期清扫)的无租约家族跳过清单含 LATCH/BARRIER/QUEUE/ATOMIC/TIMER 而独缺 PHASER——phaser 镜像条目到期字段恒 0,任一租约到期即把 phaser 键从副本镜像误摘,幻影键并随 `freed_keys` 计入到期释放计数线。这是 v11 落地审查发现、ROADMAP"工程改进"首项登记的镜像投影缺陷:引擎账簿无恙,但管理面对静置的 phaser 键**无限期失踪**(自愈依赖该键后续镜像事件,静置键永无后续),`lease.expired.total` 累计值**永久虚增**(累计计数无自愈)。

## What Changes

- `ShadowTable.expireUpTo` 跳过清单补 PHASER 家族判定(核心一行);内联注释括号列举与方法级 Javadoc 同步补全(该列举在 TIMER 落地时即已欠账,本次一并修正为完整常驻家族清单)。
- 跳过谓词提取为静态单一判定方法,配 **LockType 全量取值矩阵绊线单测**:六个租约形态(0–5)不在跳过集、其余全部在——未来新增形态不分类即红,终结"每家族补一行"的判例式打补丁。
- 红先回归夹具:`StateMachinePhaserTest` 新增混合序列用例(phaser 注册/到场 + 短租约锁获取 + 到期条目),断言修复前恒红:镜像 phaser 条目存续、三计数逐项保真、`freed_keys` 逐元素恰等(仅含真实到期锁键,同时钉住管理面失踪与计数虚增两面)、双份回放摘要一致。
- `core-lock-engine` 规格 MODIFIED:"PHASER 条目生命周期与会话清理"需求补"租约到期影子清扫零触碰"半句与专属场景,措辞逐字对齐队列"到期清扫与租约机制对队列零触碰"先例。
- ROADMAP"工程改进"首项状态更新:立项即"进行中",归档时"已落地"。
- 无 wire 变更、无协议版本门变更、无 SDK/管理接口/指标口径面变更(修复仅让实现回到既有"照常命中"与"实际释放计数"承诺)。

## Capabilities

### New Capabilities

(无)

### Modified Capabilities

- `core-lock-engine`:"PHASER 条目生命周期与会话清理"需求增加租约到期影子清扫零触碰条款与专属场景(镜像条目存续不依赖该 key 后续变异;影子清扫 MUST NOT 摘除无租约家族镜像条目、幻影键 MUST NOT 进入期释放计数面)——既有契约欠账的成文化,判例沿队列。

## Impact

- 代码:`openlatch-server` `ShadowTable`(`expireUpTo` 跳过清单、谓词提取、注释/Javadoc)、`StateMachinePhaserTest`(红先夹具)、新增绊线矩阵单测。
- 不改:wire/协议/握手门、引擎(`CoreEngine`/`PhaserEntry`)、`ReplicationGateway`/`LeaseExpiryDriver` 逻辑(其输入 `freed_keys` 收敛后行为自然正确)、`admin-observability` 与 `metrics-observability` 规格文本(phaser 键详情照常命中、`lease.expired.total`"实际释放计数"均为既有承诺,属实现违反非规格缺口)、双语用户指南(非对外 API 面,指南无影子/清扫字面)、WATCHLIST(无对应观察行)。
- 验证:全反应堆 `mvn clean verify` 与 `check-source-citations`/`check-links` 门禁;`-Pdrill` 不需要(纯镜像投影一行修复,无进程/时序面变化,判例同原子家族误扫修复)。
