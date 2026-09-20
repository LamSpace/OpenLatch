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
import io.github.lamspace.openlatch.core.CoreInspection;
import io.github.lamspace.openlatch.core.KeyFamily;
import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.command.BarrierActionDoneCommand;
import io.github.lamspace.openlatch.core.command.BarrierAwaitCommand;
import io.github.lamspace.openlatch.core.command.BarrierLeaveCommand;
import io.github.lamspace.openlatch.core.result.BarrierActionDoneResult;
import io.github.lamspace.openlatch.core.result.BarrierAwaitResult;
import io.github.lamspace.openlatch.core.result.BarrierFinal;
import io.github.lamspace.openlatch.core.result.BarrierLeaveResult;
import io.github.lamspace.openlatch.core.result.Outcome;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 单 key 的循环屏障（CyclicBarrier 语义）状态机。
 *
 * <p><b>职责</b>：承载多方会合的可复用世代结构——到场计次、合拢判定、
 * 动作两阶段放行、离场即破障与世代回卷。操作集为
 * {@link #await}/{@link #actionDone}/{@link #leave} 三条命令通道，
 * 全部经 {@link io.github.lamspace.openlatch.core.CoreEngine} 门面分派。
 *
 * <p><b>状态要素</b>：
 * <ul>
 *   <li>{@code parties}（定型许可数，首个带非零断言的到场请求定型后不变）
 *       与 {@code generation}（当前世代号，自 1 起每 key 单调递增，永不回退）；</li>
 *   <li>当前世代到场账簿 {@code arrivals}（{@code (sessionId, requestId)}
 *       的插入序集合——到场身份，也是重发幂等去重的权威判据；集合规模
 *       上界 {@code parties}）；</li>
 *   <li>在队等待队列 {@code awaiters}（FIFO，复用 {@link Waiter} 形态，
 *       {@code lockType} 恒 {@link LockType#BARRIER}、{@code threadId} 恒 0、
 *       {@code permits} 恒 1）——位次与通知时序为 Leader 本地态，不入
 *       复制状态与快照；</li>
 *   <li>动作挂账 {@code actionPending}（被指定执行 barrierAction 的
 *       {@code (sessionId, requestId, generation)} 或空）；</li>
 *   <li>了结记录 {@code completed}（最近一个完结世代：{世代号,
 *       {@link BarrierFinal}, 该世代全部到场身份, 动作执行者会话或 0}
 *       或空）——旧世代等待项重发的幂等了结依据，有界存储：仅保留
 *       最近一个完结世代，窗口外迟到的旧重发按新到场计入当前世代
 *       （显式声明的竞态，与锁/Latch 等待队列不随切换迁移同口径）。</li>
 * </ul>
 *
 * <p><b>状态机（单世代视角）</b>：
 * <pre>
 *   空世代 ──到场×k──▶ 部分到场 ──第 parties 次到场──▶
 *       ├─ 无动作形态：即时合拢（TRIPPED 入 {@code completed}、
 *       │   generation++、全体在队者标记已通知并广播）
 *       └─ 动作形态：动作待决（actionPending 挂账，其余等待者暂不放行）
 *               ├─ 执行者回报 actionDone ──▶ 合拢（同上，执行者会话入记录）
 *               └─ 任一当前世代到场者离场（leave/会话死亡/执行者死亡）
 *                       ──▶ 破障（BROKEN 入 {@code completed}、generation++、
 *                           全体在队者以 BARRIER_BROKEN 了结广播）
 * </pre>
 *
 * <p><b>离场即破障</b>：当前世代内任何已到场身份的离场——超时、本地中断、
 * 显式 {@code breakBarrier()}、会话死亡（经 {@link #removeSession}）——
 * 均即时打破该世代（全体在队者收破障裁决），作用域限当前世代：破障后
 * 新到场自然进入新世代并正常合拢，无粘滞、无条目级 reset。从未到场的
 * 会话不构成参与者，其死亡不触发破障（{@code parties} 是期望数非名册）。
 *
 * <p><b>幂等口径</b>：{@code (sessionId, requestId)} 为等待项身份——同身份
 * 重发在任何阶段（在队、已了结、执行者待决）都 MUST NOT 重复计次，应答
 * 回显其所属世代的裁决；{@link #actionDone} 对已合拢世代幂等成功、对
 * 已破障世代回破障裁决；{@link #leave} 对已终结世代与无对应到场记录者
 * 幂等无操作。
 *
 * <p><b>与租约机制的关系</b>：{@link #leaseToken()}/{@link #leaseExpiresAtMs()}
 * 恒 0，条目永不入到期堆，{@link #forceExpire} 不可达（引擎侧无登记路径）；
 * 等待者断连随 {@link #removeSession} 摘除，不参与看门狗。
 *
 * <p><b>并发模型</b>：与 {@link LockEntry}/{@link LatchEntry} 相同——全部
 * 迁移在 {@code synchronized(this)} 内完成，通知经 {@code notify} 参数收集、
 * 调用者锁外触发；生命周期方法实现 {@link KeyEntry} 契约。时钟一律由
 * 调用方注入（{@code now}），本类不读系统时钟。
 *
 * <p><b>条目存续</b>：{@code isEmpty()} 恒 {@code false}——循环语义下同 key
 * 的世代回卷是常态，定型与世代号连续性依赖条目存续；回收归零会丢
 * parties 定型与世代单调性。无上限 key 由引擎 maxKeys 护栏兜底。
 */
public final class BarrierEntry implements KeyEntry {

    /**
     * 到场身份：(会话, 请求) 二元组，等待项幂等去重与世代了结的键。
     *
     * @param sessionId 到场会话
     * @param requestId 原到场请求 id
     */
    public record Arrival(long sessionId, long requestId) {
    }

    /**
     * 条目的复制态全量导出（影子表镜像与摘要比对的数据源）：仅含
     * 经命令迁移确定的世代状态，不含在队队列与通知时序（Leader 本地态）。
     * 列表均为插入序快照，序列化确定。
     *
     * @param parties           定型许可数
     * @param generation        当前世代号
     * @param currentArrivals   当前世代到场账簿
     * @param actionSession     动作挂账会话（0=无）
     * @param actionRequest     动作挂账请求 id
     * @param completedGeneration 最近完结世代号（0=无）
     * @param completedResult   完结世代了结形态（null=无）
     * @param completedArrivals 完结世代到场账簿
     * @param completedExecutor 完结世代执行者会话（0=无）
     */
    public record ReplicatedState(long parties, long generation, List<Arrival> currentArrivals,
            long actionSession, long actionRequest, long completedGeneration,
            BarrierFinal completedResult, List<Arrival> completedArrivals,
            long completedExecutor) {
    }

    /**
     * 动作挂账：被指定执行 barrierAction 的等待项与其世代。
     *
     * @param sessionId  执行者会话
     * @param requestId  执行者到场请求 id
     * @param generation 挂账所属世代号
     */
    private record ActionPending(long sessionId, long requestId, long generation) {
    }

    /**
     * 完结世代的了结记录。
     *
     * @param generation     完结的世代号
     * @param result         了结形态（合拢或破障）
     * @param arrivals       该世代全部到场身份（插入序，序列化确定）
     * @param executorSession 动作执行者会话（0=该世代无动作挂账）
     */
    private record Completed(long generation, BarrierFinal result,
            Set<Arrival> arrivals, long executorSession) {
    }

    /** 屏障键。 */
    private final String key;
    /** 定型许可数（构造后不变）。 */
    private final long parties;
    /** 当前世代号（自 1 起，每次世代完结恰 +1，永不回退）。 */
    private long generation;
    /** 当前世代到场账簿（插入序；合拢/破障时随 {@code completed} 归档后清空）。 */
    private Set<Arrival> arrivals;
    /** 在队等待队列（FIFO；已了结世代的重发者命中即出队，遗弃者经 {@link #sweepNotifiedHead} 清扫）。 */
    private final ArrayDeque<Waiter> awaiters = new ArrayDeque<>();
    /** 动作挂账（null=无待决动作）。 */
    private ActionPending actionPending;
    /** 最近完结世代的了结记录（null=尚无完结世代）。 */
    private Completed completed;

    /**
     * 构造循环屏障条目（定型通道：首个携带非零 {@code parties} 的到场请求）。
     *
     * @param key     屏障键
     * @param parties 定型许可数（调用方保证 {@code > 0}，引擎建条目预检把关）
     */
    public BarrierEntry(String key, long parties) {
        this.key = key;
        this.parties = parties;
        this.generation = 1L;
        this.arrivals = new LinkedHashSet<>();
    }

    /**
     * 快照重建工厂：以复制状态直写装配，不经迁移规则（恢复不重演到场
     * 判定）；在队等待队列恒空（Leader 本地态，不入快照）。
     *
     * @param key             屏障键
     * @param parties         定型许可数
     * @param generation      当前世代号
     * @param currentArrivals 当前世代到场账簿（插入序）
     * @param actionSession   动作挂账会话（0=无挂账）
     * @param actionRequest   动作挂账请求 id（{@code actionSession>0} 时有效）
     * @param completedGeneration 完结世代号（0=无完结记录）
     * @param completedResult     完结世代了结形态（{@code null} 时按无记录处理）
     * @param completedArrivals   完结世代到场账簿（插入序）
     * @param completedExecutor   完结世代执行者会话（0=无）
     * @return 快照初态的条目
     */
    public static BarrierEntry restored(String key, long parties, long generation,
            List<Arrival> currentArrivals, long actionSession, long actionRequest,
            long completedGeneration, BarrierFinal completedResult,
            List<Arrival> completedArrivals, long completedExecutor) {
        BarrierEntry e = new BarrierEntry(key, parties);
        e.generation = generation;
        e.arrivals = new LinkedHashSet<>(currentArrivals);
        if (actionSession > 0) {
            // 动作挂账恒属当前世代（待决即未完结，快照存续的唯一形态）。
            e.actionPending = new ActionPending(actionSession, actionRequest, generation);
        }
        if (completedGeneration > 0 && completedResult != null) {
            e.completed = new Completed(completedGeneration, completedResult,
                    new LinkedHashSet<>(completedArrivals), completedExecutor);
        }
        return e;
    }

    /**
     * 到场（含旧世代重发）。判定顺序（首个命中者即为结果）——
     * <ol>
     *   <li><b>parties 断言</b>：非零主张与定型值不符 →
     *       {@link Outcome#REJECT_BARRIER_PARTIES}（零扰动）；</li>
     *   <li><b>了结记录命中</b>：等待项属最近完结世代 → 摘除其在队残留，
     *       按了结形态回 {@link Outcome#GRANTED} 或
     *       {@link Outcome#BARRIER_BROKEN}（不重复计次）；</li>
     *   <li><b>执行者重发</b>：等待项即动作挂账者 → 重复回执行者标记的
     *       {@link Outcome#QUEUED}（应答丢失的兜底重发）；</li>
     *   <li><b>在队去重</b>：同身份已在当前世代队列 → {@link Outcome#QUEUED}
     *       携当前位次；</li>
     *   <li><b>队列已满</b> → {@link Outcome#REJECT_QUEUE_FULL}（MUST NOT
     *       计入到场）；</li>
     *   <li><b>入队到场</b>：计入当前世代账簿并入队；到场数恰达 parties 时
     *       当回合拢——无动作形态即时定型 TRIPPED、回卷世代、广播其余在队
     *       者，本请求直答 {@link Outcome#GRANTED}；动作形态挂账执行者、
     *       其余等待者暂不放行，本请求回 {@link Outcome#QUEUED} +
     *       执行者标记；否则回 {@link Outcome#QUEUED}。</li>
     * </ol>
     *
     * <p>与 Latch {@code await} 的口径差异：屏障到场改变世代状态（计次、
     * 合拢、执行者指定），因此重发判定以复制侧到场账簿为权威、队列仅作
     * 位次来源；Latch 归零态无此竞态。
     *
     * @param cmd                到场命令（{@code parties} 断言与创建判定由
     *                           引擎侧完成）
     * @param now                当前时刻（毫秒）
     * @param cfg                限额配置（队列深度上限）
     * @param headReplyTimeoutMs 已通知等待者的响应超时（毫秒，合拢广播与
     *                           既有队首清扫机制同口径）
     * @param notify             通知收集列表，合拢广播时由调用方在条目锁外触发
     * @return 到场结果（判别位与世代/执行者回显见 {@link BarrierAwaitResult}）
     */
    public synchronized BarrierAwaitResult await(BarrierAwaitCommand cmd, long now, CoreConfig cfg,
            long headReplyTimeoutMs, List<Waiter> notify) {
        // 规则 1：parties 断言（0 不主张）。
        if (cmd.parties() != 0 && cmd.parties() != parties) {
            return BarrierAwaitResult.rejected(Outcome.REJECT_BARRIER_PARTIES, parties);
        }
        Arrival arrival = new Arrival(cmd.sessionId(), cmd.requestId());

        // 规则 2：最近完结世代的幂等了结（重发命中账簿，顺带摘除在队残留）。
        Completed done = completed;
        if (done != null && done.arrivals().contains(arrival)) {
            awaiters.removeIf(w -> w.sessionId() == arrival.sessionId()
                    && w.requestId() == arrival.requestId());
            return done.result() == BarrierFinal.TRIPPED
                    ? BarrierAwaitResult.tripped(done.generation(), parties, false)
                    : BarrierAwaitResult.broken(done.generation(), parties);
        }

        // 规则 3：动作挂账者的应答丢失重发——重复回执行者形态。
        ActionPending pending = actionPending;
        if (pending != null && pending.sessionId() == cmd.sessionId()
                && pending.requestId() == cmd.requestId()) {
            return BarrierAwaitResult.queuedExecutor(pending.generation(), parties);
        }

        // 规则 4：当前世代在队去重（通知丢失的重发兜底）。已标记"已通知"
        // 的在队项属已完结世代——其了结本应由规则 2 命中，走到这里说明
        // 了结记录已滚出窗口：静默摘除该陈旧残留（不重复计次、不广播），
        // 本次请求按出窗口径落入规则 5/6 作新世代到场。
        int position = 0;
        var it = awaiters.iterator();
        while (it.hasNext()) {
            Waiter w = it.next();
            position++;
            if (w.sessionId() == cmd.sessionId() && w.requestId() == cmd.requestId()) {
                if (w.notified()) {
                    it.remove();
                    break;
                }
                return BarrierAwaitResult.queued(position, generation, parties);
            }
        }

        // 规则 5：队列深度限额（先于入队动作检查）。
        if (awaiters.size() >= cfg.maxQueueDepthPerKey()) {
            return BarrierAwaitResult.rejected(Outcome.REJECT_QUEUE_FULL, parties);
        }

        // 规则 6：入队到场；当回合拢判定。
        // threadId 位以 0 占位（await 无归属线程概念，等待者身份 = (会话, 请求)）。
        arrivals.add(arrival);
        awaiters.addLast(new Waiter(cmd.sessionId(), cmd.requestId(), LockType.BARRIER,
                1, 0, now, 0));
        if (arrivals.size() == parties) {
            // 最后到场者不等待通知：从队列摘除自身。
            awaiters.removeIf(w -> w.sessionId() == cmd.sessionId()
                    && w.requestId() == cmd.requestId());
            if (cmd.carriesAction()) {
                actionPending = new ActionPending(cmd.sessionId(), cmd.requestId(), generation);
                return BarrierAwaitResult.queuedExecutor(generation, parties);
            }
            complete(BarrierFinal.TRIPPED, 0L, now, headReplyTimeoutMs, notify);
            return BarrierAwaitResult.tripped(generation - 1, parties, true);
        }
        return BarrierAwaitResult.queued(awaiters.size(), generation, parties);
    }

    /**
     * 动作了结回报（执行者完成 barrierAction 后提交）。判定顺序——
     * <ol>
     *   <li><b>待决命中</b>：世代号即动作挂账世代——回报会话与挂账者一致
     *       时合拢生效（定型 TRIPPED、回卷世代、广播其余在队者）回
     *       {@link Outcome#GRANTED}；不一致回
     *       {@link Outcome#REJECT_BARRIER_ACTION}（非指定执行者）；</li>
     *   <li><b>完结命中</b>：世代号属最近了结记录——TRIPPED 且回报者为
     *       记录执行者 → 幂等 {@link Outcome#GRANTED}（重复回报）；
     *       BROKEN 且回报者为挂账执行者 → {@link Outcome#BARRIER_BROKEN}
     *       （动作执行期间世代已破，本方以破障终结）；其余回
     *       {@link Outcome#REJECT_BARRIER_ACTION}；</li>
     *   <li><b>其余</b>（世代号属当前世代但无挂账、或已滚出窗口）→
     *       {@link Outcome#REJECT_BARRIER_ACTION}，条目状态零扰动。</li>
     * </ol>
     *
     * @param cmd                了结命令（会话 + 世代号）
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 已通知等待者的响应超时（毫秒）
     * @param notify             通知收集列表，合拢生效时由调用方在条目锁外触发
     * @return 了结结果
     */
    public synchronized BarrierActionDoneResult actionDone(BarrierActionDoneCommand cmd,
            long now, long headReplyTimeoutMs, List<Waiter> notify) {
        long g = cmd.generation();
        long session = cmd.sessionId();
        ActionPending pending = actionPending;
        if (pending != null && pending.generation() == g) {
            if (pending.sessionId() != session) {
                return BarrierActionDoneResult.rejected();
            }
            complete(BarrierFinal.TRIPPED, session, now, headReplyTimeoutMs, notify);
            return BarrierActionDoneResult.ok(g);
        }
        Completed done = completed;
        if (done != null && done.generation() == g) {
            if (done.result() == BarrierFinal.TRIPPED && done.executorSession() == session) {
                return BarrierActionDoneResult.replayed(g);
            }
            if (done.result() == BarrierFinal.BROKEN && done.executorSession() == session) {
                return BarrierActionDoneResult.broken(g);
            }
            return BarrierActionDoneResult.rejected();
        }
        return BarrierActionDoneResult.rejected();
    }

    /**
     * 离场（超时/中断/显式破障）。判定顺序——
     * <ol>
     *   <li><b>纯破障主张</b>（{@code awaitRequestId == 0}）：当前世代有
     *       到场记录或动作挂账即破该世代；无则幂等无操作；</li>
     *   <li><b>已了结命中</b>：等待项属最近完结世代 → 摘除在队残留，
     *       幂等无操作；</li>
     *   <li><b>当前世代命中</b>（在队、账簿或挂账）→ 离场即破障：破当前
     *       世代（本方在队项摘除、其余在队者以 {@link Outcome#BARRIER_BROKEN}
     *       广播了结）；</li>
     *   <li><b>无对应记录</b> → 幂等无操作（OK）。</li>
     * </ol>
     *
     * @param cmd                离场命令
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 已通知等待者的响应超时（毫秒）
     * @param notify             通知收集列表，破障广播时由调用方在条目锁外触发
     * @return 离场结果（恒 {@link Outcome#GRANTED}，除非会话/家族类拒绝
     *         ——由引擎门面在条目外裁决）
     */
    public synchronized BarrierLeaveResult leave(BarrierLeaveCommand cmd, long now,
            long headReplyTimeoutMs, List<Waiter> notify) {
        long session = cmd.sessionId();
        long rid = cmd.awaitRequestId();
        if (rid == 0) {
            // 纯破障主张（breakBarrier）：破当前世代（若存在到场记录或挂账）。
            if (!arrivals.isEmpty() || actionPending != null) {
                breakCurrent(session, now, headReplyTimeoutMs, notify);
                return BarrierLeaveResult.broke();
            }
            return BarrierLeaveResult.ok();
        }
        Arrival arrival = new Arrival(session, rid);
        Completed done = completed;
        if (done != null && done.arrivals().contains(arrival)) {
            awaiters.removeIf(w -> w.sessionId() == session && w.requestId() == rid);
            return BarrierLeaveResult.ok();
        }
        if (arrivals.contains(arrival)) {
            breakCurrent(session, now, headReplyTimeoutMs, notify);
            return BarrierLeaveResult.broke();
        }
        return BarrierLeaveResult.ok();
    }

    /**
     * 会话摘除：当前世代有该会话的到场记录（含执行者挂账）→ 离场即破障，
     * 全体在队者以破障了结；该会话在旧完结世代的在队残留静默摘除（了结
     * 已定型，无需也不可再广播）；从未到场的会话与本条目无世代牵连，
     * MUST NOT 破障。
     *
     * @param sessionId          要清理的会话
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 已通知等待者的响应超时（毫秒）
     * @param notify             通知收集列表，破障广播时由调用方在条目锁外触发
     */
    @Override
    public synchronized void removeSession(long sessionId, long now,
            long headReplyTimeoutMs, List<Waiter> notify) {
        boolean currentTouch = actionPending != null && actionPending.sessionId() == sessionId;
        for (Arrival a : arrivals) {
            if (a.sessionId() == sessionId) {
                currentTouch = true;
                break;
            }
        }
        if (currentTouch) {
            breakCurrent(sessionId, now, headReplyTimeoutMs, notify, true);
            return;
        }
        awaiters.removeIf(w -> w.sessionId() == sessionId);
        // 无当前世代牵连的死亡：其身份若残留在最近完结世代的了结记录中，
        // 同步剔除（存续记录 MUST NOT 携带已消亡会话的内部 sid）。
        Completed c = completed;
        if (c != null) {
            boolean holds = c.executorSession() == sessionId;
            for (Arrival a : c.arrivals()) {
                if (a.sessionId() == sessionId) {
                    holds = true;
                    break;
                }
            }
            if (holds) {
                Set<Arrival> scrubbed = new LinkedHashSet<>(c.arrivals());
                scrubbed.removeIf(a -> a.sessionId() == sessionId);
                completed = new Completed(c.generation(), c.result(), scrubbed,
                        c.executorSession() == sessionId ? 0L : c.executorSession());
            }
        }
    }

    /**
     * 不可达：屏障无租约、永不入到期堆（引擎 {@code expireDue} 无此条目的
     * 堆记录）。以断言级无操作实现，防御调用路径意外触达。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 已通知等待者的响应超时（毫秒）
     * @param notify             通知收集列表
     * @throws IllegalStateException 恒抛出（屏障条目无到期语义）
     */
    @Override
    public synchronized void forceExpire(long now, long headReplyTimeoutMs, List<Waiter> notify) {
        throw new IllegalStateException("barrier entry has no lease to expire");
    }

    /**
     * 已通知等待者的响应超时清扫：仅队首参与（与锁/Latch 同机制）——
     * 合拢或破障广播后队首放弃重发时出队；未通知的当前世代到场者恒在
     * 队首之前不入本分支（其离场由 {@link #leave}/{@link #removeSession}
     * 裁决，MUST NOT 经清扫静默消失）。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 已通知等待者的响应超时（毫秒）
     * @param notify             通知收集列表（清扫不产生新通知）
     * @return 是否移除了超时队首
     */
    @Override
    public synchronized boolean sweepNotifiedHead(long now, long headReplyTimeoutMs,
            List<Waiter> notify) {
        Waiter head = awaiters.peekFirst();
        if (head == null || !head.notified()) {
            return false;
        }
        if (head.notifyDeadlineMs() > now) {
            return false;
        }
        awaiters.pollFirst();
        return true;
    }

    @Override
    public KeyFamily family() {
        return KeyFamily.BARRIER;
    }

    @Override
    public String key() {
        return key;
    }

    /** 恒 0：屏障无租约（契约读数占位，到期堆永不登记本条目）。 */
    @Override
    public long leaseToken() {
        return 0;
    }

    /** 恒 0：屏障无到期时刻（{@link #leaseToken()} 同理）。 */
    @Override
    public long leaseExpiresAtMs() {
        return 0;
    }

    /**
     * 恒 {@code false}：屏障条目不随世代完结或参与者散尽回收（循环语义的
     * 定型与世代单调性依赖条目存续，见类注释）。
     *
     * @return 恒 {@code false}
     */
    @Override
    public boolean isEmpty() {
        return false;
    }

    /**
     * 当前世代在队等待者读数（统计观察面；与锁家族统一为"本 key 当前
     * 排队等待项数"口径）。须在持有条目锁时调用。
     *
     * @return 在队等待者数
     */
    @Override
    public synchronized int waiterCount() {
        return awaiters.size();
    }

    /**
     * 定型许可数（快照序列化侧与应答回显读取）。须在持有条目锁时调用。
     *
     * @return 定型许可数
     */
    public synchronized long parties() {
        return parties;
    }

    /**
     * 当前世代号（快照序列化侧读取）。须在持有条目锁时调用。
     *
     * @return 当前世代号
     */
    public synchronized long generation() {
        return generation;
    }

    /**
     * 当前世代到场数（快照序列化侧与观察面读取）。须在持有条目锁时调用。
     *
     * @return 当前世代到场数
     */
    public synchronized int arrived() {
        return arrivals.size();
    }

    /**
     * 当前世代到场账簿快照（插入序；快照序列化侧读取）。须在持有条目锁时调用。
     *
     * @return 到场账簿不可变快照
     */
    public synchronized List<Arrival> currentArrivals() {
        return List.copyOf(arrivals);
    }

    /**
     * 动作挂账会话（0=无挂账）。须在持有条目锁时调用。
     *
     * @return 挂账执行者会话；无挂账为 0
     */
    public synchronized long actionSession() {
        return actionPending == null ? 0L : actionPending.sessionId();
    }

    /**
     * 动作挂账请求 id（{@link #actionSession()} 为正时有效）。须在持有条目锁时调用。
     *
     * @return 挂账请求 id；无挂账为 0
     */
    public synchronized long actionRequest() {
        return actionPending == null ? 0L : actionPending.requestId();
    }

    /**
     * 最近完结世代的了结记录世代号（0=尚无完结记录）。须在持有条目锁时调用。
     *
     * @return 完结世代号；尚无记录为 0
     */
    public synchronized long completedGeneration() {
        return completed == null ? 0L : completed.generation();
    }

    /**
     * 最近完结世代的了结形态（{@code null}=尚无完结记录）。须在持有条目锁时调用。
     *
     * @return 了结形态；尚无记录为 {@code null}
     */
    public synchronized BarrierFinal completedResult() {
        return completed == null ? null : completed.result();
    }

    /**
     * 最近完结世代的到场账簿快照（插入序）。须在持有条目锁时调用。
     *
     * @return 完结世代到场账簿不可变快照；尚无记录为空表
     */
    public synchronized List<Arrival> completedArrivals() {
        Completed c = completed;
        return c == null ? List.of() : List.copyOf(c.arrivals());
    }

    /**
     * 最近完结世代的执行者会话（0=无）。须在持有条目锁时调用。
     *
     * @return 执行者会话；无记录或无执行者为 0
     */
    public synchronized long completedExecutorSession() {
        return completed == null ? 0L : completed.executorSession();
    }

    /**
     * 复制态全量快照（条目锁内一次拷贝；影子表镜像与摘要判据数据源）。
     *
     * @return 不可变的 {@link ReplicatedState} 导出
     */
    public synchronized ReplicatedState replicatedState() {
        Completed c = completed;
        return new ReplicatedState(parties, generation, List.copyOf(arrivals),
                actionSession(), actionRequest(),
                c == null ? 0L : c.generation(), c == null ? null : c.result(),
                c == null ? List.of() : List.copyOf(c.arrivals()),
                c == null ? 0L : c.executorSession());
    }

    /**
     * 明细只读快照：条目锁内拷贝定型许可数、当前世代号与到场数、动作挂账
     * 标记、最近完结世代的了结形态与在队队列（按 FIFO 序，等待项 threadId
     * 恒 0、permits 恒 1——与入队形态一致）。屏障无租约与持有者，租约
     * 三元组与持有表恒零值/空表。纯读，MUST NOT 改变任何状态。
     *
     * @param now 采样时刻（毫秒，引擎时钟）
     * @return 本条目自洽的不可变快照
     */
    public synchronized CoreInspection.KeySnapshot snapshot(long now) {
        List<CoreInspection.WaiterSnapshot> waiterSnaps = new ArrayList<>(awaiters.size());
        for (Waiter w : awaiters) {
            waiterSnaps.add(new CoreInspection.WaiterSnapshot(
                    w.sessionId(), w.requestId(), w.threadId(), w.permits(),
                    w.enqueuedAtMs(), Math.max(0, now - w.enqueuedAtMs()), w.notified()));
        }
        return new CoreInspection.KeySnapshot(key, KeyFamily.BARRIER, false,
                0, 0, 0, 0,
                List.of(), List.copyOf(waiterSnaps),
                0, 0, 0, 0, List.of(),
                null, 0, 0, 0,
                parties, generation, arrivals.size(), actionPending != null,
                completed == null ? null : completed.result());
    }

    /**
     * 世代完结（合拢或破障）的统一落点：归档 {@code completed}（含执行者
     * 会话）、世代号恰 +1、到场账簿清空、动作挂账清除，并对当前世代在队
     * 的未通知等待者逐个标记"已通知、待重发"收集进 {@code notify}（与
     * Latch 归零广播同机制：广播不摘队，等待者经各自重发命中规则 2 出队，
     * 遗弃者经 {@link #sweepNotifiedHead} 清扫）。
     *
     * <p>调用方义务：MUST 在条目锁内调用；完结后 {@code completed} 即被
     * 替换——上一完结世代的未重发残留就此滚出窗口（显式竞态声明的载体）。
     *
     * @param result             了结形态
     * @param executorSession    执行者会话（0=无动作挂账）
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 已通知等待者的响应超时（毫秒）
     * @param notify             通知收集列表
     */
    private void complete(BarrierFinal result, long executorSession, long now,
            long headReplyTimeoutMs, List<Waiter> notify) {
        complete(result, executorSession, now, headReplyTimeoutMs, notify, 0L);
    }

    /**
     * 带身份剔除的完结变体：{@code purgeSession} 非零时，该会话的到场记录
     * 与执行者身份从归档中剔除——死亡会话的引擎内部 sid 不应残留在任何
     * 存续记录里（集群镜像按 sidMap 折算逻辑 id，残留会泄漏随机内部 sid；
     * 其重发路径在会话校验处即被拒，剔除无观测差异）。
     *
     * @param result             了结形态
     * @param executorSession    执行者会话（0=无）
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 已通知等待者响应超时（毫秒）
     * @param notify             通知收集列表
     * @param purgeSession       需剔除的死亡会话（0=不剔除）
     */
    private void complete(BarrierFinal result, long executorSession, long now,
            long headReplyTimeoutMs, List<Waiter> notify, long purgeSession) {
        long finishedGeneration = generation;
        Set<Arrival> finished = arrivals;
        if (purgeSession != 0) {
            Set<Arrival> scrubbed = new LinkedHashSet<>(finished);
            scrubbed.removeIf(a -> a.sessionId() == purgeSession);
            finished = scrubbed;
            if (executorSession == purgeSession) {
                executorSession = 0L;
            }
        }
        completed = new Completed(finishedGeneration, result, finished, executorSession);
        actionPending = null;
        generation = finishedGeneration + 1;
        arrivals = new LinkedHashSet<>();
        // 标记并广播当前在队的未通知者（即刚完结世代的等待者；已通知的
        // 上一世代残留保持原状，等待重发或清扫）。
        List<Waiter> updated = new ArrayList<>(awaiters.size());
        for (Waiter w : awaiters) {
            if (w.notified()) {
                updated.add(w);
                continue;
            }
            Waiter marked = w.withDeadline(now + headReplyTimeoutMs);
            updated.add(marked);
            notify.add(marked);
        }
        // 以带截止时刻的实例替换队列（保持不可变等待项约定）。
        awaiters.clear();
        awaiters.addAll(updated);
    }

    /**
     * 破当前世代（离场即破障的统一实现）：以 {@link BarrierFinal#BROKEN}
     * 完结，执行者挂账（若有）记入会话身份供事后回报判定；离场发起方
     * （若在队）的等待项随破障摘除——其本方 await 已在客户端终结。
     *
     * @param departingSession   离场发起会话（死亡摘除时即死者会话）
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 已通知等待者的响应超时（毫秒）
     * @param notify             通知收集列表
     */
    private void breakCurrent(long departingSession, long now, long headReplyTimeoutMs,
            List<Waiter> notify) {
        breakCurrent(departingSession, now, headReplyTimeoutMs, notify, false);
    }

    /**
     * 破障变体：{@code purgeDeparting} 为真（死亡路径）时同步剔除死者身份。
     *
     * @param departingSession   离场发起会话
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 已通知等待者响应超时（毫秒）
     * @param notify             通知收集列表
     * @param purgeDeparting     是否从归档中剔除发起会话的到场与执行者身份
     */
    private void breakCurrent(long departingSession, long now, long headReplyTimeoutMs,
            List<Waiter> notify, boolean purgeDeparting) {
        long executor = actionPending != null ? actionPending.sessionId() : 0L;
        awaiters.removeIf(w -> w.sessionId() == departingSession
                && !w.notified());
        complete(BarrierFinal.BROKEN, executor, now, headReplyTimeoutMs, notify,
                purgeDeparting ? departingSession : 0L);
    }
}
