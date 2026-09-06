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
import io.github.lamspace.openlatch.core.command.AcquireCommand;
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.command.RenewCommand;
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.ReleaseResult;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import io.github.lamspace.openlatch.core.result.RenewResult;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

/**
 * 单 key 的许可门闸状态机（Phase 3 详设 §2.3 / P3-03）：N 个许可的共享
 * 资源闸口，许可获取受租约保护（客户端崩溃不泄漏许可）。
 *
 * <p><b>状态要素</b>：
 * <ul>
 *   <li>许可池：{@code permitsTotal}（首次请求定型，条目存续期不变）、
 *       {@code permitsAvailable}（当前可用数）；</li>
 *   <li>持有表：{@code holders}（归属 → 该归属持有许可数，重入按次累加）；</li>
 *   <li>共享租约三元组：与锁读者群同口径——整条目共享单一凭证，任何
 *       持有者（含重入与后来加入者）的授予/续租都刷新这段共享租约；
 *       强制到期归还<b>全部</b>持有者的许可（"与锁一致"即指此）；</li>
 *   <li>等待队列：{@code waiters}，FIFO，每项携带其请求许可数。</li>
 * </ul>
 *
 * <p><b>并发模型</b>：与 {@link LockEntry} 相同——全部状态迁移在
 * {@code synchronized(this)} 内完成，通知事件经 {@code notify} 参数收集、
 * 由调用者在条目锁外触发；生命周期方法实现 {@link KeyEntry} 契约。
 *
 * <p><b>公平性</b>：严格 FIFO 仅队首可获——许可归还后只有队首的请求
 * 被检查，队首不满足时任何后续请求（含所需许可更小的）都不得越位，
 * 杜绝大请求饥饿（详设 §2.3）。同归属重入是唯一例外：它不改变队列
 * 位次，只在许可足量时直接累加持有。
 */
public final class SemaphoreEntry implements KeyEntry {

    /** 锁键。 */
    private final String key;
    /** 许可总量，首次请求定型，之后不变。 */
    private final int permitsTotal;
    /** 当前可用许可数。 */
    private int permitsAvailable;
    /** 归属 → 持有许可数（计数恒 > 0，归零移除）。 */
    private final Map<Owner, Integer> holders = new HashMap<>();
    /** 等待队列（每项携带请求许可数）。 */
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();

    /** 当前共享租约凭证，无持有者时为 0。 */
    private long leaseToken;
    /** 当前共享租约时长（毫秒）。 */
    private long leaseMs;
    /** 当前共享租约到期时刻（毫秒）。 */
    private long leaseExpiresAtMs;

    /**
     * 构造许可门闸条目（建条目路径，总量由首次请求定型）。
     *
     * @param key          锁键
     * @param permitsTotal 许可总量（调用方保证 &gt; 0，core 门面在建条目预检把关）
     */
    public SemaphoreEntry(String key, int permitsTotal) {
        this.key = key;
        this.permitsTotal = permitsTotal;
        this.permitsAvailable = permitsTotal;
    }

    /**
     * 快照重建工厂（详设 §7.1 推广，供 {@code CoreEngine.restoreFrom} 加载
     * Semaphore 条目）：以传入的池与持有快照直接装配初态，不经状态迁移
     * 规则；等待队列恒空（集群等待队列不进复制状态，design D9）。
     *
     * @param key              锁键
     * @param permitsTotal     许可总量
     * @param permitsAvailable 可用许可数
     * @param holders          归属 → 持有许可数
     * @param leaseToken       共享租约凭证（无持有者时 0）
     * @param leaseMs          生效租期（毫秒）
     * @param leaseExpiresAtMs 到期时刻（毫秒）
     * @return 持有快照初态的条目
     */
    public static SemaphoreEntry restored(String key, int permitsTotal, int permitsAvailable,
            Map<Owner, Integer> holders, long leaseToken, long leaseMs, long leaseExpiresAtMs) {
        SemaphoreEntry e = new SemaphoreEntry(key, permitsTotal);
        e.permitsAvailable = permitsAvailable;
        e.holders.putAll(holders);
        e.leaseToken = leaseToken;
        e.leaseMs = leaseMs;
        e.leaseExpiresAtMs = leaseExpiresAtMs;
        return e;
    }

    /**
     * 许可获取：按下列规则顺序授予、排队或拒绝（首个命中者即为结果，
     * 对齐 {@link LockEntry#acquire} 的规则集风格）：
     * <ol>
     *   <li><b>总量主张检查</b>：请求携带非零 {@code permitsTotal} 断言且与
     *       定型值不符 → {@link Outcome#REJECT_SEMAPHORE_TOTAL}（零扰动）；</li>
     *   <li><b>重入</b>：同归属已持有且许可足量 → 持有累加、共享凭证、
     *       租约按本次请求值整段刷新 → 授予（重入不受队首约束，但仍受
     *       池内足量约束——与 {@code java.util.concurrent.Semaphore} 的
     *       {@code acquire(n)} 消耗池语义一致）；</li>
     *   <li><b>空队快路径</b>：队列空且许可足量 → 授予（无持有者时签发
     *       新凭证；有持有者时复用共享凭证并刷新租约）；</li>
     *   <li><b>队首重发命中</b>：队首与本次请求同 {@code (sessionId, requestId)}
     *       且许可足量 → 出队授予；队首但许可不足 → 保持排队（位次 1）；</li>
     *   <li><b>不可排队</b>：{@code queueIfBusy} 为假 → {@link Outcome#DENIED}；</li>
     *   <li><b>幂等去重 / 队列已满 / 入队</b>：与 {@link LockEntry#acquire}
     *       同规则，等待项携带请求许可数。</li>
     * </ol>
     *
     * <p>非队首的足量请求 MUST NOT 被授予（严格 FIFO 防饥饿）；一切授予
     * 路径的租约以 {@code effectiveLeaseMs} 整段刷新（design D2 口径）。
     * 会话有效性与条目存活校验由 {@code CoreEngine} 在本方法之外完成。
     *
     * @param cmd                获取命令（{@code permits ≥ 1}，已由门面归一）
     * @param now                当前时刻（毫秒）
     * @param leaseTokenSupplier 新租约凭证供应器，仅首次持有消费
     * @param effectiveLeaseMs   已夹取的实际租约时长（毫秒）
     * @param cfg                限额配置（队列深度上限）
     * @return 获取结果
     */
    public synchronized AcquireResult acquire(AcquireCommand cmd, long now,
            LongSupplier leaseTokenSupplier, long effectiveLeaseMs, CoreConfig cfg) {
        int permits = cmd.permits();
        Owner owner = new Owner(cmd.sessionId(), cmd.threadId());

        // 规则 1：总量断言（0 为不主张；非零必须与定型值一致）。
        if (cmd.permitsTotal() != 0 && cmd.permitsTotal() != permitsTotal) {
            return new AcquireResult(Outcome.REJECT_SEMAPHORE_TOTAL, 0, 0, 0);
        }

        boolean canTake = permitsAvailable >= permits;

        // 规则 2：重入（已持有 + 足量）——不受队首约束，共享凭证，租约整段刷新。
        if (canTake && holders.containsKey(owner)) {
            holders.merge(owner, permits, Integer::sum);
            permitsAvailable -= permits;
            leaseMs = effectiveLeaseMs;
            leaseExpiresAtMs = now + effectiveLeaseMs;
            return new AcquireResult(Outcome.GRANTED, leaseToken, effectiveLeaseMs, 0);
        }

        // 规则 3：空队快路径。
        if (canTake && waiters.isEmpty()) {
            if (holders.isEmpty()) {
                long token = leaseTokenSupplier.getAsLong();
                holders.put(owner, permits);
                permitsAvailable -= permits;
                leaseToken = token;
                leaseMs = effectiveLeaseMs;
                leaseExpiresAtMs = now + effectiveLeaseMs;
                return new AcquireResult(Outcome.GRANTED, token, effectiveLeaseMs, 0);
            }
            // 有持有者：复用共享凭证（避免新 token 使既有持有者的释放失效）。
            holders.merge(owner, permits, Integer::sum);
            permitsAvailable -= permits;
            leaseMs = effectiveLeaseMs;
            leaseExpiresAtMs = now + effectiveLeaseMs;
            return new AcquireResult(Outcome.GRANTED, leaseToken, effectiveLeaseMs, 0);
        }

        // 规则 4：队首重发命中（AWAIT_NOTIFY → 重发的幂等落地路径）。
        Waiter head = waiters.peekFirst();
        if (head != null && head.sessionId() == cmd.sessionId() && head.requestId() == cmd.requestId()) {
            if (canTake) {
                waiters.pollFirst();
                long token = leaseToken;
                if (holders.isEmpty()) {
                    token = leaseTokenSupplier.getAsLong();
                    leaseToken = token;
                }
                holders.merge(owner, permits, Integer::sum);
                permitsAvailable -= permits;
                leaseMs = effectiveLeaseMs;
                leaseExpiresAtMs = now + effectiveLeaseMs;
                return new AcquireResult(Outcome.GRANTED, token, effectiveLeaseMs, 0);
            }
            // 队首但池内不足：保持排队并续约其通知窗口（重发抵达即存活证明，
            // 与集群 WaitQueue.enqueue 的续约规则对称）。
            Waiter rearmed = head.withDeadline(now + cfg.headReplyTimeoutMs());
            waiters.pollFirst();
            waiters.addFirst(rearmed);
            return new AcquireResult(Outcome.QUEUED, 0, 0, 1);
        }

        // 规则 5：不可排队 → DENIED（池不足或队列非空的越位禁止同落此支）。
        if (!cmd.queueIfBusy()) {
            return new AcquireResult(Outcome.DENIED, 0, 0, 0);
        }

        // 规则 6：幂等去重。
        int position = 0;
        for (Waiter w : waiters) {
            position++;
            if (w.sessionId() == cmd.sessionId() && w.requestId() == cmd.requestId()) {
                return new AcquireResult(Outcome.QUEUED, 0, 0, position);
            }
        }

        // 规则 7：队列深度限额。
        if (waiters.size() >= cfg.maxQueueDepthPerKey()) {
            return new AcquireResult(Outcome.REJECT_QUEUE_FULL, 0, 0, 0);
        }

        // 规则 8：入队（携带请求许可数）。
        waiters.addLast(new Waiter(cmd.sessionId(), cmd.requestId(), LockType.SEMAPHORE,
                cmd.permits(), cmd.threadId(), now, 0));
        return new AcquireResult(Outcome.QUEUED, 0, 0, waiters.size());
    }

    /**
     * 归还许可：判定顺序——无任何持有 → {@code NOT_HELD}；凭证不匹配 →
     * {@code INVALID_TOKEN}；归属未持有 → {@code NOT_HELD}；归还数超过
     * 该归属持有数 → {@code OVER_RELEASE}（池零扰动，详设 §2.3 对称扣减
     * 约定）；否则持有计数扣减、许可即时归还池中，持有归零时移除归属，
     * 全体持有清空时撤销共享租约。每次成功归还后按队首规则尝试推进
     * （许可部分归还即可解锁队首，与锁"全体释放才推进"不同）。
     *
     * @param cmd                释放命令（{@code permits ≥ 1}，已由门面归一）
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表，由调用方在条目锁外触发
     * @return 释放结果（{@code fullyReleased} 表示全体持有清空）
     */
    public synchronized ReleaseResult release(ReleaseCommand cmd, long now,
            long headReplyTimeoutMs, List<Waiter> notify) {
        if (holders.isEmpty()) {
            return new ReleaseResult(ReleaseStatus.NOT_HELD, false);
        }
        if (leaseToken != cmd.leaseToken()) {
            return new ReleaseResult(ReleaseStatus.INVALID_TOKEN, false);
        }
        Owner owner = new Owner(cmd.sessionId(), cmd.threadId());
        Integer held = holders.get(owner);
        if (held == null) {
            return new ReleaseResult(ReleaseStatus.NOT_HELD, false);
        }
        if (cmd.permits() > held) {
            return new ReleaseResult(ReleaseStatus.OVER_RELEASE, false);
        }
        int left = held - cmd.permits();
        permitsAvailable += cmd.permits();
        if (left == 0) {
            holders.remove(owner);
            if (holders.isEmpty()) {
                clearLease();
            }
        } else {
            holders.put(owner, left);
        }
        notifyHeadIfPossible(now, headReplyTimeoutMs, notify);
        return new ReleaseResult(ReleaseStatus.OK, holders.isEmpty(), cmd.permits());
    }

    /**
     * 续租共享租约：判定顺序与 {@link LockEntry#renew} 一致（凭证 0 →
     * {@code NOT_HELD}；不匹配 → {@code INVALID_TOKEN}；匹配则整段刷新）。
     * 任一持有者续租即刷新全体共享租约（与锁读者群口径一致）。
     *
     * @param cmd              续租命令
     * @param now              当前时刻（毫秒）
     * @param effectiveLeaseMs 已夹取的实际租约时长（毫秒）
     * @return 续租结果
     */
    public synchronized RenewResult renew(RenewCommand cmd, long now, long effectiveLeaseMs) {
        if (leaseToken == 0) {
            return new RenewResult(ReleaseStatus.NOT_HELD, 0);
        }
        if (leaseToken != cmd.leaseToken()) {
            return new RenewResult(ReleaseStatus.INVALID_TOKEN, 0);
        }
        leaseMs = effectiveLeaseMs;
        leaseExpiresAtMs = now + effectiveLeaseMs;
        return new RenewResult(ReleaseStatus.OK, leaseExpiresAtMs);
    }

    /**
     * 租约到期强制回收：归还<b>全部</b>持有者的许可（共享租约的连带效果，
     * 与"与锁一致"的整 key 单一租约语义同源）、清空持有表、撤销租约，
     * 并按队首规则尝试推进。等待队列不受影响。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表，由调用方在条目锁外触发
     */
    @Override
    public synchronized void forceExpire(long now, long headReplyTimeoutMs, List<Waiter> notify) {
        permitsAvailable = permitsTotal;
        holders.clear();
        clearLease();
        notifyHeadIfPossible(now, headReplyTimeoutMs, notify);
    }

    /**
     * 会话清理：归还该会话全部持有的许可、摘除其全部等待项，全体持有
     * 清空时撤销共享租约，最后按队首规则尝试推进。
     *
     * @param sessionId          要清理的会话
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表，由调用方在条目锁外触发
     */
    @Override
    public synchronized void removeSession(long sessionId, long now,
            long headReplyTimeoutMs, List<Waiter> notify) {
        var it = holders.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Owner, Integer> en = it.next();
            if (en.getKey().sessionId() == sessionId) {
                permitsAvailable += en.getValue();
                it.remove();
            }
        }
        waiters.removeIf(w -> w.sessionId() == sessionId);
        if (holders.isEmpty()) {
            clearLease();
        }
        notifyHeadIfPossible(now, headReplyTimeoutMs, notify);
    }

    @Override
    public synchronized boolean sweepNotifiedHead(long now, long headReplyTimeoutMs, List<Waiter> notify) {
        Waiter head = waiters.peekFirst();
        if (head == null || !head.notified()) {
            return false;
        }
        if (head.notifyDeadlineMs() > now) {
            return false;
        }
        waiters.pollFirst();
        notifyHeadIfPossible(now, headReplyTimeoutMs, notify);
        return true;
    }

    @Override
    public KeyFamily family() {
        return KeyFamily.SEMAPHORE;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public long leaseToken() {
        return leaseToken;
    }

    @Override
    public long leaseExpiresAtMs() {
        return leaseExpiresAtMs;
    }

    @Override
    public boolean isEmpty() {
        return holders.isEmpty() && waiters.isEmpty();
    }

    /**
     * 许可总量（快照序列化侧读取）。须在持有条目锁时调用。
     *
     * @return 许可总量
     */
    public synchronized int permitsTotal() {
        return permitsTotal;
    }

    /**
     * 当前可用许可数（快照序列化侧读取）。须在持有条目锁时调用。
     *
     * @return 可用许可数
     */
    public synchronized int permitsAvailable() {
        return permitsAvailable;
    }

    /**
     * 归属持有表快照（键值拷贝，快照序列化侧读取）。须在持有条目锁时调用。
     *
     * @return 持有表不可变拷贝
     */
    public synchronized Map<Owner, Integer> holdersSnapshot() {
        return Map.copyOf(holders);
    }

    /**
     * 当前共享租约时长（毫秒，快照序列化侧读取）。须在持有条目锁时调用。
     *
     * @return 生效租期（毫秒）
     */
    public synchronized long leaseMs() {
        return leaseMs;
    }

    /**
     * 等待队列长度读数（统计观察面，Phase 3 T2）。须在持有条目锁时调用。
     *
     * @return 当前排队等待项数
     */
    @Override
    public synchronized int waiterCount() {
        return waiters.size();
    }

    /**
     * 队首推进检查：等待队列非空、队首未被通知过、且可用许可满足队首
     * 请求数时，标记队首"已通知、待重发"并收集通知。队首不满足时不检查
     * 后续条目（严格 FIFO，防大请求饥饿的机制本体）。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表
     */
    private void notifyHeadIfPossible(long now, long headReplyTimeoutMs, List<Waiter> notify) {
        if (waiters.isEmpty()) {
            return;
        }
        Waiter head = waiters.peekFirst();
        if (head.notified()) {
            return;
        }
        if (permitsAvailable < head.permits()) {
            return;
        }
        Waiter updated = head.withDeadline(now + headReplyTimeoutMs);
        waiters.pollFirst();
        waiters.addFirst(updated);
        notify.add(updated);
    }

    /**
     * 撤销共享租约三元组（凭证、时长、到期时刻归零）。仅在全体持有清空
     * 或强制到期时调用。
     */
    private void clearLease() {
        leaseToken = 0;
        leaseExpiresAtMs = 0;
        leaseMs = 0;
    }
}
