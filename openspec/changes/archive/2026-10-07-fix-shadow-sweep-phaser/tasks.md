# Tasks

## 1. 红先钉住(测试先行)

- [x] 1.1 在 `StateMachinePhaserTest` 新增混合序列用例(D3 形态):sessionOpen + phaser REGISTER/ARRIVE("p") + REENTRANT 短租约 acquire("mk") + expire("mk"),断言 ① `shadow().isPhaser("p")` 为真且 phase/registered/arrived 逐项保真;② expire 应用结果 `freed_keys` 逐元素恰等 `["mk"]`;③ 双份回放 digest 相等。完成判据:修复前运行 `mvn -s /home/lam/repo/settings.xml -pl openlatch-server test -Dtest=StateMachinePhaserTest#<新用例名>` 于 ①② 两处恒红,红输出记录入 PR 描述。

## 2. 核心修复

- [x] 2.1 `ShadowTable.expireUpTo` 内联跳过条件提取为静态单一判定点(暂名 `isLeaselessFamilyType(int)`,实现期定名;Javadoc 注判据来源:获取路径入租约到期堆者即租约形态,按 D1/D2),清单补 PHASER 家族判定。完成判据:1.1 用例转绿(`-Dtest=StateMachinePhaserTest` 类级全绿)。
- [x] 2.2 同步被改方法注释(D5):方法体内家族枚举括注补全为完整常驻家族清单(补 TIMER 既有欠账与 PHASER),方法级 Javadoc 补"无租约家族跳过"分支语义一句。完成判据:`bash scripts/check-source-citations.sh` 零命中,且重读注释与实现互洽。

## 3. 绊线(防复发)

- [x] 3.1 新增 LockType 全量取值矩阵单测:遍历 `LockType.values()`,断言 0–5(REENTRANT/SIMPLE/READ/WRITE/FAIR/SEMAPHORE)不在跳过集、其余形态全部在。完成判据:新测试绿;并验证其红性——临时注释 2.1 谓词中 PHASER 分支运行该测试必红,复现后还原(临时改动不入库,验证记录入 PR)。

## 4. 登记与文档

- [x] 4.1 `ROADMAP.md` "工程改进"首项状态"未启动"→"进行中"并附本 change 链接,按该文件既有惯例随行更新"最后更新"日期;备注列不改(单一事实源纪律:细节沉淀规格)。完成判据:目检 + `bash scripts/check-links.sh` 零死链。

## 5. 集成验证

- [x] 5.1 全反应堆 `mvn -s /home/lam/repo/settings.xml clean verify` 8/8 模块 BUILD SUCCESS(0 失败 0 错误),快照往返与跨副本摘要套件随全量绿。
- [x] 5.2 `openspec validate fix-shadow-sweep-phaser --strict` 通过;`bash scripts/check-source-citations.sh` 与 `bash scripts/check-links.sh` 零命中。归档时依 ROADMAP 维护规约将首项状态改"已落地"(归档流程动作,不预先勾改)。
