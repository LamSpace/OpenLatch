# Design

## Context

现行链路:`LockStateMachineCore.applyExpire` 解析租约到期条目并经回放守卫,驱动 `engine.expireDue()`(引擎侧正确,phaser 不入到期堆),随后 `ShadowTable.expireUpTo(entryTimeMs)` 清扫影子镜像,摘除 key 进 `ApplyResult.freed_keys`;下游 `LeaseExpiryDriver` 按 `freedKeysCount` 累计 `lease.expired.total`,`ReplicationGateway` 遍历 `freed_keys` 驱动队首唤醒。`expireUpTo` 的无租约家族跳过清单现列 LATCH/BARRIER/QUEUE/TIMER/ATOMIC 而独缺 PHASER,phaser 镜像条目到期字段恒 0 故被任意租约到期误摘。

约束:

- 跨副本回放确定性恒等——修复前的误摘与修复后的不摘在各副本同步收敛,摘要互恰不受两态影响,修复不撕裂副本间比对。
- SEMAPHORE 不在跳过清单而安全:`release()` 于持有归零时摘除 key,凡不在跳过清单的存活镜像条目必携真实租约——PHASER 是该不变式唯一破坏者(常驻且到期恒 0)。
- 判例族:原子误扫修复、队列"零触碰"条款、timer 同型跳过清单均为"每家族在此处补一行/补一句成文"的文字枚举形态,新增常驻家族须人工记忆补行,v10 实证漏补。

动机见 proposal.md - Why。

## Goals / Non-Goals

**Goals:**

- 以最小实现恢复"租约到期影子清扫对无租约家族零触碰":一条判定 + 单一判定点。
- 机械防复发:LockType 全量取值矩阵绊线,新形态不判类即红。
- 红先钉住双危害面:管理面静置键无限期失踪(镜像摘除)+ `lease.expired.total` 累计虚增(幻影键入 `freed_keys`)。

**Non-Goals:**

- 不为幻影键唤醒遍历加网关侧防御——`freed_keys` 收敛后该遍历对 phaser 键自然空转,无需改动。
- 不追溯修正历史 `lease.expired.total` 数值——累计计数进程重启归零,口径自然修复。
- 不改 `admin-observability`/`metrics-observability` 规格文本——"phaser 键详情照常命中"与"实际释放计数"均为既有承诺,本修复令实现回归合规而非立新约。
- 不重定义 `expireUpTo` 语义——回放守卫、条目时刻口径、与 `engine.expireDue()` 的一致性判定全部不动。

## Decisions

**D1 沿用类型白名单,不反转判定为 `expiresAtMs > 0` 哨兵。** 白名单是"常驻家族不参与到期语义"的意图声明,与规格文字判例(队列条款)同构、可 grep 可审计;哨兵反转依赖"凡不在跳过清单的存活条目到期非 0"的隐式不变式——该不变式现由 SEMAPHORE"持有归零即摘除"这一附带实现耦合撑起,非契约约定。备选"哨兵对 v12+ 新家族自动免疫"因意图与症状倒置被否。

**D2 跳过谓词提取为静态单一判定点 + 全量矩阵绊线。** `expireUpTo` 的内联条件提取为 `ShadowTable` 静态方法(暂名 `isLeaselessFamilyType(int)`,实现期定名),配单测遍历 `LockType.values()` 断言:0–5(REENTRANT/SIMPLE/READ/WRITE/FAIR/SEMAPHORE)不在跳过集、其余协议形态全在。判据来源注于提取点 Javadoc(获取路径入租约到期堆者即租约形态)。新增类型未判类时矩阵测试必红,把"每家族补一行"终结为"一处判定 + 一红守卫"。备选"仅补一行不提取"(ROADMAP 一行修复的最小口径)因复发依赖人审被否——绊线为用户已裁口径。

**D3 夹具置于状态机回放道,复刻原子判例形态并加强。** `StateMachinePhaserTest` 新增用例:sessionOpen + phaser REGISTER/ARRIVE("p") + REENTRANT 短租约 acquire("mk") + expire("mk") 混合序列,断言 ① `shadow().isPhaser("p")` 为真且 phase/registered/arrived 逐项保真(钉"存续含字段重建完整",非空壳);② expire 应用结果 `freed_keys` 逐元素恰等 `["mk"]`(钉幻影键——同时钉管理面与计数面两害);③ 双份回放 digest 相等(跨副本确定性纪律)。修复前 ①② 恒红。备选直调 `expireUpTo` 的表级单测——离应用点链远且既有判例(原子)皆回放道;回放道为主夹具,表级判定面由 D2 绊线覆盖。

**D4 规格成文仅动 `core-lock-engine`。** 沿队列"到期清扫与租约机制对队列零触碰"逐家族成文纪律,为 PHASER 存续需求补影子半句与同名场景(措辞对齐判例);管理面/指标面承诺已在各自规格,加 delta 即重复成文。

**D5 注释/Javadoc 同步范围。** `expireUpTo` 方法体内家族枚举括注在 TIMER 落地时即已欠账(缺 TIMER),本次连同 PHASER 补全为完整常驻家族清单;方法级 Javadoc 补"跳过家族语义"一句——分支语义即契约。此为被改方法只读自洽的仓库注释约定要求,非夹带改进。

## Risks / Trade-offs

- [修复改变镜像存续集→快照导出与管理面读数随之变] → 跨副本确定性回放恒等收敛;回放/摘要断言与快照往返套件全量验证;回滚面 = revert,无迁移态。
- [绊线以值域 {0–5} 判"租约形态",未来若引入新租约有值形态需同步判类] → 判据来源(获取路径入租约到期堆)注于提取点 Javadoc;值域演化会同时令矩阵绊线与协议冻结测试红,红先兜底。
- [历史 `lease.expired.total` 虚增不抹除] → 重启归零 + 告警基线通常一次性重校;PR 描述记录口径,不另立清理路径。
- [静置键可见性无自愈通道,本修复为唯一保障] → 正因如此,红先夹具按"后续零操作"形态钉住(D3),存续面不靠镜像事件巧合回归;新场景逐字入规格可复核。

## Migration Plan

常规发布,无数据迁移(影子镜像为可重建投影,引擎账簿从未受影响);回滚 = revert 提交,无兼容窗口、无配置开关、无协议版本变化。
