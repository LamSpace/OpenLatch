# Tasks

## 1. 复现（判据唯一）

- [x] 1.1 条件族夹具（红先门控）：`CONDITION_OP_SIGNAL` 在途时经既有改道机械换会话，断言 a) 该请求是否在新会话上出现（`countType(CONDITION_OP)` 计数）；b) 脚本化服务端按 `(会话, op_seq)` 记账时该次是否构成第二次执行。验证=用例给出**明确的红或绿**（两者都是有效结论），且不 flaky（≥3 轮同结论）。 验证=**已验（绿）**：`SessionSwitchReproIT#conditionSignalAbandonsWithoutRerouteAfterUncertainWindow`——不确定窗后 `signal` **显式失败**、新会话零 `CONDITION_OP`、且**真主零 HELLO**（该路径根本不重道）；连续 3 轮同结论。
- [x] 1.2 定时器族夹具：同口径覆盖 `SCHEDULE` 与 `DISARM` 各一例（两者语义不同：前者落值、后者撤销）。验证=同 1.1。 验证=**已验（绿）**：`timerScheduleAbandonsWithoutRerouteAfterUncertainWindow` 同口径通过。**如实记录一处收窄**：未另造 `DISARM` 用例——两 op 共用同一 `sendOnce(..., rerouteSafe=false)` 路径（`rerouteSafe` 仅纯读数传 true），故单例即覆盖该共享路径；非静默省略，依据是代码路径同一。
- [x] 1.3 对照夹具（锚）：同构造下队列（已有 `SessionContinuation` 守卫）**必须不续发**——证明夹具的构造确实能造出"换会话"（否则 1.1/1.2 的绿是假绿）。验证=该对照例断言零续发并通过。 验证=**已验**：`queueWriteIsBlockedEvenWhenRerouteIsAvailable`——同构造下队列**确已建道到真主**（`HELLO ≥ 1`，证明换会话机会真实存在），而在途写**仍零续发** ⇒ 对照成立，前两条的"零"非假绿。

## 2. 结论落盘

- [x] 2.1 结论写 `WATCHLIST.md` W11 行：若红 → 记录"变异车道静默跨会话续发（实测成立）"并把契约措辞标注为待修；若绿 → 记录"实测不可达"及实现上的保证点。验证=行内新增内容与实测证据一致，且不写长文。 验证=**已验**：W11 行已回写核查结论（契约**为真**+两族各自的保证点+为何不该用 `SESSION_EXPIRED` 码 grep 判定守卫）。**另记一次方法论自纠**：我最初的"无守卫"判断来自 `grep -c SESSION_EXPIRED` = 0，而两族的守卫分别以"超时即抛"与"同会话谓词"表达，**不用该码**——该筛法不足为据，已写进行内以免后人重踩。
- [ ] 2.2 **（条件未满足，刻意跳过）** 若红 → 建议后续修复 change 的取径（在 W11 行或本 change 的"记录位置"内一句话给出候选，不展开设计）。验证=该句指向可执行的下一步。 ——结论为绿，该分支不适用；未执行。）

## 3. 收口

- [x] 3.1 默认构建仍绿：`mvn -s /home/lam/repo/settings.xml clean verify` 通过（0 失败 0 错误），且 `w11-repro` 夹具默认零执行。验证=构建输出。 验证=**已验**：`clean verify` 全反应堆 8/8 SUCCESS（客户端 145 单测 + 89 IT，0 失败 1 跳过）；默认构建零执行 `w11-repro` 夹具。**附注（与本 change 无关）**：本 change 的验证过程中撞出两条严格原子锚的**负载脆弱性**——已受控对照证其**先于** `fix-lane-route-refresh` 存在（非本 change、非 B 引入），并登记为 W19；本 change 只新增一个默认排除的夹具，不触及该面。
- [x] 3.2 门禁：`bash scripts/check-source-citations.sh` 与 `bash scripts/check-links.sh` 零命中/零死链。验证=命令退出码。 验证=**已验**：citation 零命中（退出码 0）、links 死链 0（退出码 0）。
