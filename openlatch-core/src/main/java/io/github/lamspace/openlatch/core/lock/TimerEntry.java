/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.lamspace.openlatch.core.lock;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.core.KeyFamily;
import io.github.lamspace.openlatch.core.TimerOpType;
import io.github.lamspace.openlatch.core.command.TimerOpCommand;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.TimerOpResult;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 单 key 的延时触发（JDK {@code Timer} 定时单次标记的可判定分布式子集）状态机。
 *
 * <p><b>职责</b>：承载"装载一枚会在未来某刻响一次、全体共见的标记"这一语义的
 * 复制侧账簿——装载代次、绝对到期时刻、装载态（PENDING/DISARMED）、装载去重
 * 与挂起等待集的唤醒回收。操作集为 {@link #timerOp} 单一命令通道（五操作词
 * 表），经 {@link io.github.lamspace.openlatch.core.CoreEngine} 门面分派。
 *
 * <p><b>核心不变式——到期是派生谓词而非状态迁移</b>：账簿 MUST NOT 存在任何
 * "已到期"驻留位；到期共见判据恒为纯函数
 * {@code marked = armed ∧ 判定时刻 ≥ fireAtMs}（"到期不是迁移，是时间的兑现"）。
 * 装载（SCHEDULE）与撤销（DISARM）是唯二改变复制态的操作——它们在应用点
 * 迁移 {@code (generation, armed, fireAtMs)} 三元组；时钟走过 {@code fireAtMs}
 * 不产生任何条目、不改写任何字段、不进任何 digest。唤醒投递（tick 扫描命中
 * 谓词后推 {@code AWAIT_NOTIFY}）是纯提示——漏扫不丢状态（谓词对同一复制
 * 数据随时重评即自洽），等待方重发时重评谓词即了结。本类因此与
 * {@code LeaseExpiryDriver}（到期即破坏性迁移、必须入日志复制）构成判例族两端。
 *
 * <p><b>状态要素</b>（复制侧账簿 vs Leader 本地态以快照为准——前者入日志与
 * 快照、跨副本确定重放；后者进程易失、换主清零）：
 * <ul>
 *   <li>{@code generation}（装载代次，自 0 起每次 SCHEDULE 严格 +1、不取模、
 *       永不回退——纯观察序，非等待谓词输入；等待闭环无需"已见代次"，因标记
 *       是二值粘滞，当下重评即完备，对照 {@code PhaserEntry} 的
 *       {@code expected_phase} 入参——相位是连续推进史不可由当值重建，代次是
 *       当下快照可重建）；</li>
 *   <li>{@code armed} ∧ {@code fireAtMs}（装载二元：{@code armed=true} 时
 *       {@code fireAtMs} 为应用点 {@code 条目时刻 + delay} 折算的绝对到期时刻，
 *       随复制日志确定化、Leader 切换与副本回放不改判——判例 v7 队列延时形态
 *       逐字适用；{@code armed=false}（DISARMED 代终结粘滞）时 {@code fireAtMs}
 *       保持撤销前值作历史观察读数、不重写）；</li>
 *   <li>每会话装载去重槽 {@code slots}（{@code sessionId → Slot}，单槽覆盖式
 *       ——同 (会话, 请求) 重发命中即回放回声三元组、不双换代、不双改态；新
 *       装载覆盖旧槽，旧请求在覆盖后的迟到重发按新变异执行=声明竞态，判例 v7
 *       队列交付槽。{@code DISARM} 亦经槽承载回声（幂等：已 DISARMED 再 DISARM
 *       回 OK 零迁移零唤醒））；</li>
 *   <li>挂起等待集 {@code waiters}（{@code (sessionId, requestId) → 登记时刻}
 *       到达序队列）——<b>Leader 本地态，不入日志与快照</b>；唤醒即出集（不留
 *       "已通知待重发"位——了结由纯谓词重评承载，重发命中即 {@code OK}/{@code
 *       DENIED}，无需有界了结记录窗口，这是相对 {@code PhaserEntry} 的再简化）。
 *       本集合仅在单机形态由条目直接持有；集群形态等待住 {@code TimerRegistry}
 *       （Leader 簿记，见 {@code clusterMode} 守卫）。</li>
 * </ul>
 *
 * <p><b>状态机（单代视角）</b>：
 * <pre>
 *   装载(SCHEDULE delay) ─▶ generation++ ∧ armed=true ∧ fireAtMs=now+delay
 *         │                        （换代清钟：粘滞 marked 随 armed 重置归伪）
 *         ├──判定 now &lt; fireAtMs──▶ PENDING（等待可挂起；旁观者续等新代）
 *         ├──判定 now ≥ fireAtMs──▶ FIRED（armed ∧ 到期 → marked 真，全员共见）
 *         └──DISARM─────────────▶ armed=false（代终结粘滞，fireAtMs 保持）
 *                                   └─▶ 全体在等旁观者以 DENIED 了结（唤醒靠事件）
 *   SCHEDULE(重装载) ─▶ 开新代回到 PENDING/FIRED（round 语义由"粘滞+换代清零"承载）
 * </pre>
 *
 * <p><b>改期对在等者的契约</b>：重装载换代清钟，已挂起的 {@code AWAIT} 谓词
 * 重评以最新代为准——原等旧时刻者可能因改期推后至新时刻多睡、因
 * 改期提前先醒（两向皆合法，无"旧代承诺"补偿；推荐 timed await 自救）。到期
 * 唤醒靠 tick（精度 {@code timer-ready-tick-ms}，正确性不依赖精度——ODelayQueue
 * 同款契约句）；撤销唤醒靠 apply 事件（即时，不等 tick——撤销是已提交变异，
 * 等待者的 {@code DENIED} 终态无钟依赖）。
 *
 * <p><b>死亡语义——触发绑定 key 不绑定会话</b>：{@code SESSION_CLOSE} 对账簿
 * {@code (generation, armed, fireAtMs)} 三值<b>零扰动</b>——装载者死亡钟照响
 * （本原语相对进程本地 {@code java.util.Timer} 的核心增量：触发是与发起方生命
 * 无关的未来事件；队列"死亡不吞元素"同轴）。死亡仅摘除该会话在等待集的挂起
 * 位（订阅随会话灭，判例 {@code PhaserEntry} 等待摘除同点）。与 Barrier
 * "死亡即破障"、Phaser "死亡=隐式摘除配额"并列为死亡语义三形态。
 *
 * <p><b>护栏分界（配置非判定——"钳制属接入层"判例纪律）</b>：本条目 MUST NOT
 * 依据 {@code max-timer-horizon-ms} 拒绝装载——该判定唯一在受理点（单机门面/
 * 集群 Leader 预检），条目应用侧恒宽容（已提交条目在任何配置下照常回放，
 * 否则配置漂移撕裂账簿）。唯一消费 {@code cfg} 的规则是 {@code AWAIT} 的合并
 * 等待深度（{@code maxQueueDepthPerKey}，超限 {@link Outcome#REJECT_QUEUE_FULL}）。
 *
 * <p><b>并发模型</b>：与 {@link PhaserEntry} 相同——全部迁移在
 * {@code synchronized(this)} 内完成；唤醒收集经 {@code notify} 参数、由调用方
 * 在条目锁外触发（本类 MUST NOT 持锁外呼）；时钟一律由调用方注入（{@code now}），
 * 本类不读系统时钟。等待项被唤醒即出集，故 {@link #sweepNotifiedHead} 恒
 * {@code false}（无"已通知待重发"形态）。
 *
 * <p><b>无租约家族</b>：{@link #leaseToken()}/{@link #leaseExpiresAtMs()} 恒 0，
 * 永不入到期堆，{@link #forceExpire} 不可达（抛 {@link IllegalStateException}）。
 *
 * <p><b>条目存续</b>：{@code isEmpty()} 恒 {@code false}——代次单调依赖条目存续
 * （回收丢"key 即 timer 身份"的持续观察语义，判例 {@code PhaserEntry}/{@code
 * BarrierEntry} 条目存续约定）；DISARMED 代终结亦保留账簿供读数；无上限 key
 * 由引擎 maxKeys 护栏兜底。
 */
public final class TimerEntry implements KeyEntry {

    /**
     * 每会话装载去重槽读数（复制态导出与快照形态）。单槽覆盖式：该会话最近
     * 一次装载/撤销的回声——同请求重发回放三元组、不双换代。
     *
     * @param sessionId     装载会话（引擎内部 sid，镜像侧折算逻辑 id）
     * @param requestId     该会话最近一次 SCHEDULE/DISARM 的请求 id
     * @param scheduleOp    槽内操作类型（{@code true}=SCHEDULE 回声、{@code
     *                      false}=DISARM 回声——armed 由 op 推导，不另设位）
     * @param echoGeneration 首次回执的代次回显（重发回放源）
     * @param echoFireAtMs  首次回执的绝对到期时刻回显（DISARM 回声=撤销前保持值）
     */
    public record Slot(long sessionId, long requestId, boolean scheduleOp,
            long echoGeneration, long echoFireAtMs) {
    }

    /**
     * 挂起等待项读数（观察面专用；已唤醒项不出现在此）。
     *
     * @param sessionId    等待会话
     * @param requestId    等待请求 id（唤醒通知的关联键）
     * @param enqueuedAtMs 登记时刻（引擎时钟，毫秒；换主重挂后重新计时）
     */
    public record WaiterView(long sessionId, long requestId, long enqueuedAtMs) {
    }

    /**
     * 条目的复制态全量导出（影子表镜像与摘要比对的数据源）：仅含账簿，
     * 不含等待集（Leader 本地态）。槽列表按会话 id 升序确定性导出。
     *
     * @param generation 装载代次
     * @param armed      装载态
     * @param fireAtMs   绝对到期时刻
     * @param slots      每会话装载去重槽（会话 id 升序）
     */
    public record ReplicatedState(long generation, boolean armed, long fireAtMs,
            List<Slot> slots) {
    }

    /**
     * 挂起等待项（不可变；唤醒即出集，无"已通知"中间态）。
     *
     * @param sessionId    等待会话（引擎内部 sid）
     * @param requestId    等待请求 id（唤醒通知的关联键）
     * @param enqueuedAtMs 登记时刻（毫秒，观察值）
     */
    private record Wait(long sessionId, long requestId, long enqueuedAtMs) {
    }

    /** 延时触发键。 */
    private final String key;
    /** 装载代次（自 0 起每次 SCHEDULE 恰 +1，永不回退；纯观察序）。 */
    private long generation;
    /** 装载态（{@code true}=PENDING 待响、{@code false}=DISARMED 代终结）。 */
    private boolean armed;
    /** 绝对到期时刻（应用点折算；DISARM 后保持历史值不重写）。 */
    private long fireAtMs;
    /** 每会话装载去重槽（会话 id → 槽；覆盖式，LinkedHashMap 保插入序供观察）。 */
    private final Map<Long, Slot> slots = new LinkedHashMap<>();
    /** 挂起等待集（到达序；Leader 本地态/单机条目态，不入快照）。 */
    private final ArrayDeque<Wait> waiters = new ArrayDeque<>();

    /**
     * 构造延时触发条目（定型通道：TIMER 家族唯一的 SCHEDULE 建条目路径，
     * 引擎门面在无条目时装配；初始 zero 账簿——generation 0、未装载）。
     *
     * @param key 延时触发键
     */
    public TimerEntry(String key) {
        this.key = key;
    }

    /**
     * 快照重建工厂：以复制账簿直写装配，不经迁移规则（恢复不重演装载/撤销
     * 判定——代次、装载态、到期时刻、去重槽原值回灌；等待集恢复为空）。
     *
     * @param key        延时触发键
     * @param generation 装载代次
     * @param armed      装载态
     * @param fireAtMs   绝对到期时刻
     * @param slots      每会话装载去重槽（账簿插入序=该会话首装先后导入）
     * @return 快照初态的条目
     */
    public static TimerEntry restored(String key, long generation, boolean armed,
            long fireAtMs, List<Slot> slots) {
        TimerEntry e = new TimerEntry(key);
        e.generation = generation;
        e.armed = armed;
        e.fireAtMs = fireAtMs;
        for (Slot s : slots) {
            e.slots.put(s.sessionId(), s);
        }
        return e;
    }

    /**
     * 统一操作入口（单机受理形态，等价 {@code clusterMode=false} 的便捷重载）。
     *
     * @param cmd    操作命令
     * @param now    判定时刻（毫秒——单机为本地时钟，集群 apply 为条目时刻）
     * @param cfg    限额配置（合并深度）
     * @param notify 唤醒收集列表，唤醒发生时由调用方在条目锁外触发
     * @return 操作结果（三元组为裁决时刻账簿快照）
     */
    public synchronized TimerOpResult timerOp(TimerOpCommand cmd, long now, CoreConfig cfg,
            List<Waiter> notify) {
        return timerOp(cmd, now, cfg, notify, false);
    }

    /**
     * 统一操作入口（五操作词表分派）。
     *
     * <p><b>集群守卫</b>：{@code clusterMode=true} 时 {@code AWAIT}/{@code
     * CANCEL}/{@code {@code QUERY}} 抵达即拒（等待登记/摘除/读数在集群形态由
     * Leader 侧 {@code TimerRegistry} + 影子表直连裁决，MUST NOT 抵达条目——
     * 抵达即门面分派缺陷；判例 {@code PhaserEntry}）。{@code SCHEDULE}/{@code
     * DISARM} 两类变异在两种形态均正常处理，但 {@code clusterMode=true} 时
     * 不采集条目内唤醒（等待住 registry，唤醒排空在网关侧据结果标记完成）。
     *
     * @param cmd         操作命令（形状与尺寸已由受理点裁决）
     * @param now         判定/应用时刻（毫秒）
     * @param cfg         限额配置（合并深度；horizon 不在此复核）
     * @param notify      唤醒收集列表（仅 {@code clusterMode=false} 采集）
     * @param clusterMode 集群应用形态（true 时等待不登记条目内、不采集唤醒）
     * @return 操作结果（三元组为裁决时刻账簿快照）
     */
    public synchronized TimerOpResult timerOp(TimerOpCommand cmd, long now, CoreConfig cfg,
            List<Waiter> notify, boolean clusterMode) {
        if (clusterMode && (cmd.op() == TimerOpType.AWAIT
                || cmd.op() == TimerOpType.CANCEL || cmd.op() == TimerOpType.QUERY)) {
            return TimerOpResult.rejected(Outcome.REJECT_TIMER_NO_ENTRY);
        }
        return switch (cmd.op()) {
            case SCHEDULE -> schedule(cmd, now);
            case DISARM -> disarm(cmd, now, notify, clusterMode);
            case AWAIT -> await(cmd, now, cfg, clusterMode, notify);
            case CANCEL -> {
                waiters.removeIf(w -> w.sessionId() == cmd.sessionId()
                        && w.requestId() == cmd.awaitRequestId());
                yield TimerOpResult.granted(generation, armed, fireAtMs, verdict(now));
            }
            case QUERY -> TimerOpResult.granted(generation, armed, fireAtMs, verdict(now));
        };
    }

    /**
     * 装载/重装载：换代清钟。同 (会话, 请求) 重发命中去重槽回放回声（不双
     * 换代）；否则代次 +1、{@code armed=true}、{@code fireAtMs = now + delay}
     * （绝对到期时刻在应用点由条目时刻折算，判例 v7 队列延时形态）。
     *
     * @param cmd 装载命令（{@code delayMs} 已由受理点保证非负且不越 horizon）
     * @param now 应用/判定时刻（毫秒）
     * @return GRANTED 携新代三元组与到期读数（delay=0 时 {@code marked} 即刻为真）
     */
    private TimerOpResult schedule(TimerOpCommand cmd, long now) {
        Slot prior = slots.get(cmd.sessionId());
        if (prior != null && prior.requestId() == cmd.requestId() && prior.scheduleOp()) {
            // 同请求重发装载：回放首次回声，代次不前进、到期时刻不重写。
            boolean marked = prior.echoFireAtMs() >= 0 && now >= prior.echoFireAtMs();
            return TimerOpResult.granted(prior.echoGeneration(), true,
                    prior.echoFireAtMs(), marked);
        }
        long delay = cmd.delayMs() == null ? 0L : cmd.delayMs();
        generation += 1;
        armed = true;
        fireAtMs = now + delay;
        slots.put(cmd.sessionId(), new Slot(cmd.sessionId(), cmd.requestId(), true,
                generation, fireAtMs));
        return TimerOpResult.granted(generation, armed, fireAtMs, now >= fireAtMs);
    }

    /**
     * 撤销装载：当代转 DISARMED（代终结粘滞，{@code fireAtMs} 保持撤销前值）。
     * 单机形态同关键区采集全体在等旁观者唤醒（{@code DENIED} 终态了结——撤销
     * 唤醒靠事件即时、不等到期 tick）；幂等：已 DISARMED 再 DISARM 零迁移零唤醒。
     *
     * @param cmd         撤销命令
     * @param now         应用时刻（毫秒）
     * @param notify      唤醒收集列表
     * @param clusterMode 集群形态（true 时不采集条目内唤醒——等待住 registry）
     * @return GRANTED 携当代三元组（{@code disarmed=true} 表发生迁移，驱动
     *         集群侧 registry 排空唤醒；幂等回声 {@code disarmed=false}）
     */
    private TimerOpResult disarm(TimerOpCommand cmd, long now, List<Waiter> notify,
            boolean clusterMode) {
        Slot prior = slots.get(cmd.sessionId());
        if (prior != null && prior.requestId() == cmd.requestId() && !prior.scheduleOp()) {
            // 同请求重发撤销：回放首次回声，不重复迁移、不重复唤醒。
            return TimerOpResult.disarmed(prior.echoGeneration(), prior.echoFireAtMs(), false);
        }
        boolean transitioned = armed;
        armed = false;
        slots.put(cmd.sessionId(), new Slot(cmd.sessionId(), cmd.requestId(), false,
                generation, fireAtMs));
        if (transitioned && !clusterMode) {
            for (Wait w : waiters) {
                notify.add(new Waiter(w.sessionId(), w.requestId(),
                        io.github.lamspace.openlatch.core.LockType.TIMER, 1, 0,
                        w.enqueuedAtMs(), 0));
            }
            waiters.clear();
        }
        return TimerOpResult.disarmed(generation, fireAtMs, transitioned);
    }

    /**
     * 等待当代标记：以判定时刻重评谓词——当代 {@code armed} 且已到期即刻
     * {@code GRANTED(marked=true)}（粘滞共见：fire 之后到达者恒即刻通过）；
     * 已 DISARMED 即刻 {@code DENIED}（等待不可再满足的显式终态）；未到期
     * {@code QUEUED} 登记（幂等：同 (会话,请求) 重发命中在集项返回登记回执、
     * 不二次入集）。集群形态（{@code clusterMode=true}）本方法不可达（入口守卫
     * 已拒），仅在单机条目态登记条目内等待。
     *
     * @param cmd         等待命令
     * @param now         判定时刻（毫秒）
     * @param cfg         限额配置（合并等待深度）
     * @param clusterMode 集群形态（预留——入口守卫已拦截，恒 false 抵达）
     * @param notify      唤醒收集列表（即刻了结形态不产出）
     * @return GRANTED（已共见）/ DENIED（已撤销）/ QUEUED（挂起）/ REJECT_*
     */
    private TimerOpResult await(TimerOpCommand cmd, long now, CoreConfig cfg,
            boolean clusterMode, List<Waiter> notify) {
        if (!armed) {
            return TimerOpResult.denied(generation, fireAtMs);
        }
        if (now >= fireAtMs) {
            return TimerOpResult.granted(generation, true, fireAtMs, true);
        }
        for (Wait w : waiters) {
            if (w.sessionId() == cmd.sessionId() && w.requestId() == cmd.requestId()) {
                return TimerOpResult.queued(generation, fireAtMs);
            }
        }
        if (waiters.size() >= cfg.maxQueueDepthPerKey()) {
            return TimerOpResult.rejected(Outcome.REJECT_QUEUE_FULL);
        }
        waiters.addLast(new Wait(cmd.sessionId(), cmd.requestId(), now));
        return TimerOpResult.queued(generation, fireAtMs);
    }

    /**
     * 到期共见谓词（读面派生，不回写账簿）：{@code armed ∧ now ≥ fireAtMs}。
     *
     * @param now 判定时刻（毫秒）
     * @return 当代是否已到期共见
     */
    private boolean verdict(long now) {
        return armed && now >= fireAtMs;
    }

    /**
     * 到期唤醒扫描（单机形态就绪驱动消费，判例 {@code QueueEntry.wakeReadyHeads}）：
     * 对当代 {@code armed ∧ now ≥ fireAtMs} 且存在挂起等待项的条目，摘除并收集
     * 全部在等旁观者的唤醒到 {@code notify}（唤醒即出集，了结由重发谓词重评
     * 承载）。无等待者或未到期的条目零动作（fire 的"发生"无需通知不在场者）。
     *
     * @param now                扫描时刻（毫秒）
     * @param headReplyTimeoutMs 队首响应超时（本家族唤醒即出集，不消费，签名对齐判例）
     * @param notify             唤醒收集列表
     * @return 本条目本轮唤醒的等待项数
     */
    public synchronized int wakeReadyHeads(long now, long headReplyTimeoutMs,
            List<Waiter> notify) {
        if (!armed || now < fireAtMs || waiters.isEmpty()) {
            return 0;
        }
        int n = 0;
        for (Wait w : waiters) {
            notify.add(new Waiter(w.sessionId(), w.requestId(),
                    io.github.lamspace.openlatch.core.LockType.TIMER, 1, 0,
                    w.enqueuedAtMs(), 0));
            n++;
        }
        waiters.clear();
        return n;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public KeyFamily family() {
        return KeyFamily.TIMER;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public long leaseToken() {
        return 0;
    }

    @Override
    public long leaseExpiresAtMs() {
        return 0;
    }

    @Override
    public synchronized int waiterCount() {
        return waiters.size();
    }

    /**
     * 会话关闭清理（{@code SESSION_CLOSE} 应用点与单机断连同路径）：摘除该
     * 会话在等待集的挂起位与其装载去重槽行，<b>账簿三元组
     * {@code (generation, armed, fireAtMs)} 零扰动</b>——装载者死亡钟照响
     * （触发绑定 key 的契约体现，与 Phaser 隐式摘除配额、Barrier 破障刻意分轨）。
     * 去重槽行随会话摘除是幂等辅助结构的生命周期而非契约面（回声已无重放方，
     * 判例 v10 配额行随死亡删除的同构）；恢复侧对孤儿槽行的过滤与导出因此对称
     * （快照中不存在死者槽行）。死亡会话无在等旁观项时零副作用。
     *
     * @param sessionId          要清理的会话
     * @param now                当前时刻（毫秒，不消费）
     * @param headReplyTimeoutMs 队首通知响应超时（毫秒，不消费）
     * @param notify             通知收集列表（死亡不代为唤醒——等者已随会话灭）
     */
    @Override
    public synchronized void removeSession(long sessionId, long now, long headReplyTimeoutMs,
            List<Waiter> notify) {
        waiters.removeIf(w -> w.sessionId() == sessionId);
        slots.remove(sessionId);
    }

    /**
     * 不可达：延时触发条目无租约、永不入到期堆（引擎 {@code expireDue} 依
     * {@link #leaseToken()} 恒 0 短路）。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知响应超时（毫秒）
     * @param notify             通知收集列表
     * @throws IllegalStateException 恒抛出（延时触发条目无到期语义）
     */
    @Override
    public synchronized void forceExpire(long now, long headReplyTimeoutMs, List<Waiter> notify) {
        throw new IllegalStateException("timer entry has no lease to expire");
    }

    /**
     * 恒 {@code false}：本条目无"已通知待重发"形态（唤醒即出集，重发的到期
     * 判定由谓词重评幂等自决，判例 {@code PhaserEntry}）。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知响应超时（毫秒）
     * @param notify             通知收集列表
     * @return 恒 {@code false}
     */
    @Override
    public synchronized boolean sweepNotifiedHead(long now, long headReplyTimeoutMs,
            List<Waiter> notify) {
        return false;
    }

    /**
     * 装载代次（观察面与镜像导出读取；须持锁）。
     *
     * @return 当前装载代次
     */
    public synchronized long generation() {
        return generation;
    }

    /**
     * 装载态（观察面与镜像导出读取；须持锁）。
     *
     * @return 已装载（PENDING）为 {@code true}，代终结（DISARMED）为 {@code false}
     */
    public synchronized boolean armed() {
        return armed;
    }

    /**
     * 绝对到期时刻（观察面与镜像导出读取；须持锁）。
     *
     * @return 绝对到期时刻（毫秒；DISARM 后保持历史值）
     */
    public synchronized long fireAtMs() {
        return fireAtMs;
    }

    /**
     * 明细只读快照：条目锁内拷贝账簿三元组（家族外字段零值形；挂起等待
     * 明细不入本快照——timer 等待经 {@link #waitersSnapshot()} 独立视图
     * 导出，判例 phaser 并列不并号）。呈现面不折算 marked——原始读数无
     * 时钟歧义。
     *
     * @param now 采样基准时刻（毫秒，本家族不消费）
     * @return 明细快照
     */
    public synchronized io.github.lamspace.openlatch.core.CoreInspection.KeySnapshot
            snapshot(long now) {
        return new io.github.lamspace.openlatch.core.CoreInspection.KeySnapshot(
                key, KeyFamily.TIMER, false,
                0, 0, 0, 0,
                List.of(), List.of(),
                0, 0, 0, 0, List.of(),
                null, 0, 0, 0,
                0, 0, 0, false, null, null, null,
                0, 0, 0, 0, null,
                0, 0, 0,
                generation, armed, fireAtMs);
    }

    /**
     * 复制态全量导出（影子表镜像与跨副本摘要的权威读口，须持锁）：账簿三元组
     * + 每会话去重槽（<b>账簿插入序=该会话首次装载先后</b>——判例 v10 配额表
     * 的插入序条款：内部 sid 升序跨副本不可比（引擎 sid 逐副本随机），插入序
     * 由 apply 序唯一决定，digest 跨副本逐字节一致的确定性前提）。
     *
     * @return 复制态快照（不含等待集）
     */
    public synchronized ReplicatedState replicatedState() {
        return new ReplicatedState(generation, armed, fireAtMs,
                List.copyOf(slots.values()));
    }

    /**
     * 挂起等待集快照（单机形态观察面；集群形态恒空，等待住 TimerRegistry）。
     *
     * @return 等待项视图列表（登记到达序）
     */
    public synchronized List<WaiterView> waitersSnapshot() {
        List<WaiterView> out = new ArrayList<>();
        for (Wait w : waiters) {
            out.add(new WaiterView(w.sessionId(), w.requestId(), w.enqueuedAtMs()));
        }
        return List.copyOf(out);
    }
}
