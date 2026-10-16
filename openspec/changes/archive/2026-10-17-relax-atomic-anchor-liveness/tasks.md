# Tasks

## 1. 判据与纪律（测试支撑）

- [x] 1.1 `LeaderKillLedger`：纯判定（`assertAdvanceResolved`／`assertAdvanceResolvedRef`／`assertDurable`）+ 有界推进（`advanceScalar`／`advanceRef`）+ 有界读（`readScalarStamped`／`readRefStamped`）。三条纪律＝复核先行（不盲目重发）、有界（真 wedge 仍红）、非法态即 `AssertionError`（与显式异常族刻意分开，绝不被 catch 吞）。验证=编译通过且被两枚锚使用。→ 落定；`RefRead` 载体把"读到 null 态"与"读失败"分开，免得"已提交载荷丢失"被静默当读不可用
- [x] 1.2 复核读同样有界（读幂等可重发但不得无限）：读面整体不可用即判失败，**不得静默略过断言**。验证=1.1 的 `read*Stamped` 上限分支。→ `MAX_ATTEMPTS=16` + 30s 时限

## 2. 负向控制（证明放宽≠删判据）

- [x] 2.1 非法态必拒：双加值（`pre+2δ`）、第三方载荷、换主后漂移/丢失（值丢、双加、版本漂）——喂给纯判定，逐一断言 `AssertionError`。验证=用例红到绿（判据存在则绿）。→ 3 枚用例绿
- [x] 2.2 合法态必过：未生效／已生效一次两侧都不得误判（挡"过严"）。验证=用例绿。→ 绿
- [x] 2.3 **端到端**错值注入：脚本化桩对 `ATOMIC_OP` 回"双加过的值"，断言推进判据必拒（判据真挂在读数上，不只是纸面规则）。验证=用例绿。→ 绿（`injectedDoubleAppliedServerValueIsRejected`）

## 3. 两枚锚改写

- [x] 3.1 `ClientClusterIT#committedAtomicValueSurvivesLeaderKillWithoutDoubleApply`：前置态→换主→**换主后已提交态严格存续**（值/版本逐项）→换主后续写有界收敛；Javadoc 写明口径变更、复核先行纪律与"本锚不再覆盖成功率"。验证=用例绿且非法态仍红（2.1/2.3）。→ 改写完成，类 Javadoc 同步
- [x] 3.2 `ClientClusterIT#committedReferencePayloadSurvivesLeaderKillWithoutDrift`：同款（CAS 形态 + 载荷字节面）。验证=同 3.1。→ 改写完成

## 4. 验证

- [x] 4.1 负向控制 5 枚绿（surefire）。→ `Tests run: 5, Failures: 0`
- [x] 4.2 **同造载口径复测**（2×nproc 燃烧进程 + 客户端模块 failsafe-only 跑 `ClientClusterIT`，4 轮）：两枚放宽后的锚**全绿**（此前同口径 pin 侧 2/4 红、master 侧 4/4 红）。验证=逐轮日志。→ 第 2/3/4 轮全绿；第 1 轮红在**未改动**的 W11 收敛契约锚（`:402`，1/4，与 master 侧 2/4 同型）。日志 `$CLAUDE_JOB_DIR/tmp/w19-relax-round{1..4}.log`；燃烧进程已杀
- [x] 4.3 全反应堆 `clean verify` 通过（含 `verify-javadoc`）。验证=构建输出。→ `BUILD SUCCESS`（全反应堆 8/8，含 `verify-javadoc`）；客户端模块 `LeaderKillLedgerTest` 5/5 与 `ClientClusterIT` 9/9 皆绿（`w19c-verify-full.log`）
- [x] 4.4 门禁：`bash scripts/check-source-citations.sh` 与 `--selftest` 零命中。验证=退出码。→ 两者皆 0

## 5. 台账与收口

- [x] 5.1 `WATCHLIST.md` W19 行：(3) 落账＝取"放宽到契约面"、附带更强形态（不跳安全面）+ 负向控制 + 同口径 4 轮绿 + **判据面收窄的显式声明**（不再覆盖成功率）。验证=行内容与实测一致、表完整性复检。→ 20 行、每行 6 竖线、id 单调无重
- [x] 5.2 归档 change（`skip_specs: true`，零规格面）、按 test/docs 拆分提交、推送开 PR；CI 两检查绿（PR 侧回读）。验证=PR 检查列表回读。→ 见 PR
