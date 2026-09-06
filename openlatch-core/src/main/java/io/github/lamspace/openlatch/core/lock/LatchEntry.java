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
import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.command.LatchAwaitCommand;
import io.github.lamspace.openlatch.core.result.LatchAwaitResult;
import io.github.lamspace.openlatch.core.result.LatchCountDownResult;
import io.github.lamspace.openlatch.core.result.Outcome;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 单 key 的一次性倒计数屏障状态机（Phase 3 详设 §2.4 / P3-05）。
 *
 * <p><b>状态要素</b>：
 * <ul>
 *   <li>{@code total}（初始计数，首个带非零断言的请求定型后不变）与
 *       {@code count}（当前剩余，下限 0）；</li>
 *   <li>等待队列 {@code awaiters}（FIFO，元素复用 {@link Waiter} 形态，
 *       {@code lockType} 恒 {@link LockType#LATCH}）；</li>
 *   <li>屏障一经定型即存续至节点重启（一次性语义不随参与者散尽而失效）：
 *       参与者身份仍被记录用于观测与调试，但 MUST NOT 触发条目回收——
 *       回收归零屏障会使晚到者误重建同名屏障，破坏一次性护栏；未归零
 *       屏障的遗弃由 key 命名约定（每轮新 key）管理。</li>
 * </ul>
 *
 * <p><b>与租约机制的关系</b>：{@link #leaseToken()}/{@link #leaseExpiresAtMs()}
 * 恒 0，条目永不入到期堆，{@link #forceExpire} 不可达（引擎侧无登记路径）；
 * 等待者断连随 {@link #removeSession} 摘除，不参与看门狗。
 *
 * <p><b>并发模型</b>：与 {@link LockEntry} 相同——全部迁移在
 * {@code synchronized(this)} 内完成，通知经 {@code notify} 参数收集、
 * 调用者锁外触发；生命周期方法实现 {@link KeyEntry} 契约。
 *
 * <p><b>一次性语义</b>：归零后 {@code await} 立即通过、{@code countDown}
 * 无操作；不支持重置——新屏障用新 key（参与者散尽即条目回收，回收后
 * 带值请求新建屏障、纯加入被拒，见 design D5）。
 */
public final class LatchEntry implements KeyEntry {

    /** 屏障键。 */
    private final String key;
    /** 初始计数（定型后不变）。 */
    private final long total;
    /** 当前剩余计数（下限 0）。 */
    private long count;
    /** 等待队列（FIFO，归零后各等待者经重发命中离队）。 */
    private final ArrayDeque<Waiter> awaiters = new ArrayDeque<>();
    /** 参与会话集（定型/扣减/等待者登记，观测用；不驱动回收，见类注释）。 */
    private final Set<Long> participants = new HashSet<>();

    /**
     * 构造屏障条目（定型通道：首个携带非零 {@code total} 的 await/countDown）。
     *
     * @param key   屏障键
     * @param total 初始计数（调用方保证 {@code > 0}，引擎建条目预检把关）
     */
    public LatchEntry(String key, long total) {
        this.key = key;
        this.total = total;
        this.count = total;
    }

    /**
     * 快照重建工厂（详设 §7.1 推广）：以计数快照直接装配，不经迁移规则；
     * 等待者与参与者集恒空（均为 Leader 本地态，不入复制状态）。
     *
     * @param key   屏障键
     * @param total 定型初始计数
     * @param count 当前剩余计数
     * @return 快照初态的条目
     */
    public static LatchEntry restored(String key, long total, long count) {
        LatchEntry e = new LatchEntry(key, total);
        e.count = count;
        return e;
    }

    /**
     * 等待屏障：判定顺序（首个命中者即为结果）——
     * <ol>
     *   <li><b>总量断言</b>：非零 {@code total} 与定型值不符 →
     *       {@link Outcome#REJECT_LATCH_TOTAL}；</li>
     *   <li><b>已归零</b>：{@code count == 0} → 摘除该请求在队的等待项
     *       （如有）→ {@link Outcome#GRANTED}（立即通过）；</li>
     *   <li><b>幂等去重</b>：同 {@code (sessionId, requestId)} 已在队 →
     *       返回当前位次（通知丢失的重发兜底）；</li>
     *   <li><b>队列已满</b> → {@link Outcome#REJECT_QUEUE_FULL}；</li>
     *   <li><b>入队挂起</b> → {@link Outcome#QUEUED}。</li>
     * </ol>
     *
     * <p>与锁获取不同：屏障无"立即式拒绝"分支（await 只有挂起与通过两态），
     * 也无重入概念；参与者集随任意通道登记。
     *
     * @param cmd    等待命令（{@code total} 断言已在引擎侧完成建条目判定）
     * @param now    当前时刻（毫秒）
     * @param cfg    限额配置（队列深度上限）
     * @return 等待结果
     */
    public synchronized LatchAwaitResult await(LatchAwaitCommand cmd, long now, CoreConfig cfg) {
        // 规则 1：总量断言（0 不主张）。
        if (cmd.total() != 0 && cmd.total() != total) {
            return new LatchAwaitResult(Outcome.REJECT_LATCH_TOTAL, 0);
        }
        participants.add(cmd.sessionId());

        // 规则 2：已归零立即通过（顺带摘除同请求的在队等待项）。
        if (count == 0) {
            awaiters.removeIf(w -> w.sessionId() == cmd.sessionId() && w.requestId() == cmd.requestId());
            return new LatchAwaitResult(Outcome.GRANTED, 0);
        }

        // 规则 3：幂等去重。
        int position = 0;
        for (Waiter w : awaiters) {
            position++;
            if (w.sessionId() == cmd.sessionId() && w.requestId() == cmd.requestId()) {
                return new LatchAwaitResult(Outcome.QUEUED, position);
            }
        }

        // 规则 4：队列深度限额。
        if (awaiters.size() >= cfg.maxQueueDepthPerKey()) {
            return new LatchAwaitResult(Outcome.REJECT_QUEUE_FULL, 0);
        }

        // 规则 5：入队挂起。
        // threadId 位以 0 占位（await 无归属线程概念，等待者身份 = (会话, 请求)）。
        awaiters.addLast(new Waiter(cmd.sessionId(), cmd.requestId(), LockType.LATCH,
                1, 0, now, 0));
        return new LatchAwaitResult(Outcome.QUEUED, awaiters.size());
    }

    /**
     * 倒计数：判定顺序——已归零（或纯断言 {@code n == 0}）→ 无操作返回当前
     * 剩余；否则 {@code count = max(0, count − n)}，本次扣减使计数<em>首次</em>
     * 落到 0 时，对全部在队等待者逐个标记"已通知、待重发"并收集进
     * {@code notify}（CDL 的唤醒语义本就是全体放行；等待者在各自重发命中
     * 规则 2 时离队，故广播不摘队、条目不因广播即刻可回收）。
     *
     * <p>判定顺序：非零 {@code total} 断言与定型值不符 →
     * {@link Outcome#REJECT_LATCH_TOTAL}（零扰动，先于参与者登记之外的
     * 一切迁移）；已归零 → 无操作返回剩余 0；否则扣减并在首次落零时广播。
     *
     * @param n                  扣减量（{@code >= 0}，0 为纯初始化/断言）
     * @param total              定型断言（0 不主张）
     * @param sessionId          发起会话（登记参与者）
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 通知响应超时（毫秒）
     * @param notify             通知收集列表，由调用方在条目锁外触发
     * @return 结果：GRANTED 携带剩余计数，或总量断言拒绝
     */
    public synchronized LatchCountDownResult countDown(long n, long total, long sessionId, long now,
            long headReplyTimeoutMs, List<Waiter> notify) {
        // 总量断言（与 await 规则 1 同口径；0 为不主张）。
        if (total != 0 && total != this.total) {
            return new LatchCountDownResult(Outcome.REJECT_LATCH_TOTAL, 0);
        }
        participants.add(sessionId);
        if (count == 0) {
            return new LatchCountDownResult(Outcome.GRANTED, 0);
        }
        long before = count;
        count = Math.max(0, count - n);
        if (before > 0 && count == 0) {
            List<Waiter> updated = new ArrayList<>(awaiters.size());
            for (Waiter w : awaiters) {
                Waiter marked = w.withDeadline(now + headReplyTimeoutMs);
                updated.add(marked);
                notify.add(marked);
            }
            // 以带截止时刻的实例替换队列（保持不可变等待项约定）。
            awaiters.clear();
            awaiters.addAll(updated);
        }
        return new LatchCountDownResult(Outcome.GRANTED, count);
    }

    /**
     * 会话摘除：移除其等待项与参与者身份；条目是否可回收由 {@link #isEmpty()}
     * 判定（无租约可清，本方法不触碰计数）。
     *
     * @param sessionId          要清理的会话
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒，屏障不适用队首
     *                           推进，保留形参以对齐 {@link KeyEntry} 契约）
     * @param notify             通知收集列表（屏障仅在归零时广播，此处恒空）
     */
    @Override
    public synchronized void removeSession(long sessionId, long now,
            long headReplyTimeoutMs, List<Waiter> notify) {
        awaiters.removeIf(w -> w.sessionId() == sessionId);
        participants.remove(sessionId);
    }

    /**
     * 不可达：屏障无租约、永不入到期堆（引擎 {@code expireDue} 无此条目的
     * 堆记录）。以断言级无操作实现，防御调用路径意外触达。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表
     * @throws IllegalStateException 恒抛出（屏障条目无到期语义）
     */
    @Override
    public synchronized void forceExpire(long now, long headReplyTimeoutMs, List<Waiter> notify) {
        throw new IllegalStateException("latch entry has no lease to expire");
    }

    /**
     * 已通知等待者的响应超时清扫：仅队首参与（与锁同机制）——归零广播后
     * 队首放弃重发时出队，其余等待者经新到 await 的规则 2 仍可直接通过。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表（清扫不产生新通知）
     * @return 是否移除了超时队首
     */
    @Override
    public synchronized boolean sweepNotifiedHead(long now, long headReplyTimeoutMs, List<Waiter> notify) {
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
        return KeyFamily.LATCH;
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
     * 恒 {@code false}：屏障条目不随操作收尾回收——未归零屏障必须存活等待
     * 扣减，已归零屏障以条目存续承载一次性护栏（见类注释与 design D5 修订）。
     *
     * @return 恒 {@code false}
     */
    @Override
    public boolean isEmpty() {
        return false;
    }

    /**
     * 定型初始计数（快照序列化侧读取）。须在持有条目锁时调用。
     *
     * @return 初始计数
     */
    public synchronized long total() {
        return total;
    }

    /**
     * 当前剩余计数（快照序列化侧与 countDown 应答读取）。须在持有条目锁时调用。
     *
     * @return 剩余计数
     */
    public synchronized long count() {
        return count;
    }
}
