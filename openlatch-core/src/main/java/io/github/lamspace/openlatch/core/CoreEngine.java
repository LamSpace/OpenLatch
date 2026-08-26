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

package io.github.lamspace.openlatch.core;

import io.github.lamspace.openlatch.core.command.AcquireCommand;
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.command.RenewCommand;
import io.github.lamspace.openlatch.core.lease.LeaseManager;
import io.github.lamspace.openlatch.core.lock.LockEntry;
import io.github.lamspace.openlatch.core.lock.LockTable;
import io.github.lamspace.openlatch.core.lock.Waiter;
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.ReleaseResult;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import io.github.lamspace.openlatch.core.result.RenewResult;
import io.github.lamspace.openlatch.core.session.SessionRegistry;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 锁语义核心门面，是本引擎对外的唯一契约面：全部锁操作（获取、释放、续租）、
 * 会话生命周期与租约到期回收都经此类完成。纯 Java、零外部运行依赖、无网络；
 * 时间可经 {@link Clock} 注入（测试用手工时钟），事件经 {@link CoreEventListener}
 * 向外报告。
 *
 * <p><b>线程模型</b>：所有公共方法均可被多线程并发调用且相互安全。生产环境下的
 * 调用方为多个 Netty IO 线程（业务请求）与单个租约扫描线程（{@link #expireDue}、
 * {@link #sweepNotifiedHeads}）。安全性基于两级机制：跨 key 状态由并发容器
 * （{@link LockTable}、{@link SessionRegistry}、{@link LeaseManager} 各自内部自同步）
 * 承载；单 key 状态迁移在对应 {@link LockEntry} 的条目锁内完成，任一调用路径
 * 最多持有一个条目锁，不存在跨条目持锁，故无锁顺序死锁风险。
 *
 * <p><b>事件回调</b>：{@link CoreEventListener#notifyHead} 一律在条目锁之外触发
 * （通知列表先在锁内收集，出锁后统一回调），回调实现不得假设任何持锁上下文。
 *
 * <p><b>惰性到期契约</b>：租约到期不会立即释放锁——到期时刻仅记录在
 * {@link LeaseManager} 到期堆中，须由调用方周期性调用 {@link #expireDue}
 * 才真正回收。两次扫描之间已过期的锁仍被视为持有。
 */
public final class CoreEngine {

    /** 限额与租约配置（不可变）。 */
    private final CoreConfig config;
    /** 时间源，所有到期/超时判断均以此为准。 */
    private final Clock clock;
    /** 事件出口，接收队首通知事件（条目锁外触发）。 */
    private final CoreEventListener listener;
    /** key → 锁条目映射与条目生命周期。 */
    private final LockTable lockTable = new LockTable();
    /** 租约到期堆，供 {@link #expireDue} 扫描。 */
    private final LeaseManager leaseManager = new LeaseManager();
    /** 会话登记表，会话校验与断连清理的权威。 */
    private final SessionRegistry sessions = new SessionRegistry();
    /** 租约凭证发号器，授予新持有时自增取值。 */
    private final AtomicLong leaseTokenCounter = new AtomicLong(1);

    /**
     * 构造核心引擎。
     *
     * @param config   限额与租约配置
     * @param clock    时间源，测试可用手工时钟推进租约
     * @param listener 事件出口，接收队首通知事件
     */
    public CoreEngine(CoreConfig config, Clock clock, CoreEventListener listener) {
        this.config = Objects.requireNonNull(config);
        this.clock = Objects.requireNonNull(clock);
        this.listener = Objects.requireNonNull(listener);
    }

    /**
     * 登记新会话（连接握手成功时由上层调用一次）。
     *
     * @return 新登记会话的 sessionId，为正随机数，重复概率可忽略
     */
    public long sessionOpened() {
        long id = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        sessions.register(id);
        return id;
    }

    /**
     * 关闭会话：释放该会话的全部持锁（写侧与读侧）、摘除其全部等待项，
     * 并对因此可前进的队首触发通知。幂等：重复调用或关闭未登记会话
     * 均无副作用（首次调用后登记表已移除该会话）。
     *
     * <p>这是断连清理的唯一入口，与 {@link #acquire} 中的会话校验原子互斥：
     * 要么获取请求先登记成功、关闭时一并清理，要么关闭先生效、获取被拒。
     *
     * @param sessionId 要关闭的会话
     */
    public void sessionClosed(long sessionId) {
        Set<String> keys = sessions.remove(sessionId);
        if (keys == null) {
            return;
        }
        long now = clock.nowMs();
        for (String key : keys) {
            LockEntry e = lockTable.get(key);
            if (e == null) {
                continue;
            }
            List<Waiter> notify = new ArrayList<>();
            synchronized (e) {
                e.removeSession(sessionId, now, config.headReplyTimeoutMs(), notify);
                if (e.isEmpty()) {
                    lockTable.remove(key, e);
                }
            }
            fireNotify(notify, key);
        }
    }

    /**
     * 获取锁：校验会话与 key 合法性后，授予、排队或拒绝。
     *
     * <p><b>校验顺序</b>（首个不满足者即为返回值）：
     * <ol>
     *   <li>会话已登记，否则 {@link Outcome#REJECT_SESSION}（预检）；</li>
     *   <li>key 非空，否则 {@link Outcome#REJECT_KEY_EMPTY}；</li>
     *   <li>key 的 UTF-8 字节长度不超过 {@code maxKeyLength}，否则
     *       {@link Outcome#REJECT_KEY_TOO_LONG}；</li>
     *   <li>条目锁内再次权威校验会话仍存活（与 {@link #sessionClosed} 原子互斥），
     *       失败仍回 {@link Outcome#REJECT_SESSION}。</li>
     * </ol>
     *
     * <p><b>租约</b>：请求租约为 0 时取默认租约，并一律夹取到
     * {@code [minLeaseMs, maxLeaseMs]} 之间。授予新持有时签发新租约凭证；
     * 重入获取（写侧或读侧）不换新凭证：持有计数加一、同一凭证、
     * 租约整段刷新；读锁加入已有读者时复用现有凭证。
     *
     * <p><b>排队语义</b>：锁被占用且 {@code queueIfBusy} 为真时入队，
     * 返回 {@link Outcome#QUEUED} 与 1 起的队列位次；同一
     * {@code (sessionId, requestId)} 重复请求幂等去重——不二次入队，
     * 返回当前位次。队列满返回 {@link Outcome#REJECT_QUEUE_FULL}；
     * 锁被占用且 {@code queueIfBusy} 为假（立即式）返回 {@link Outcome#DENIED}。
     *
     * <p><b>条目生命周期</b>：条目按需创建；操作完成后若无持有者且无等待者，
     * 立即从锁表移除，避免空条目滞留。
     *
     * @param cmd 获取锁命令，会话必须已登记（否则返回 {@code REJECT_SESSION}）
     * @return 获取结果：{@link Outcome#GRANTED} 时携带租约凭证与实际租约，
     *         {@link Outcome#QUEUED} 时携带队列位次，其余为拒绝原因
     */
    public AcquireResult acquire(AcquireCommand cmd) {
        long now = clock.nowMs();
        if (!sessions.contains(cmd.sessionId())) {
            return new AcquireResult(Outcome.REJECT_SESSION, 0, 0, 0);
        }
        String key = cmd.key();
        if (key == null || key.isEmpty()) {
            return new AcquireResult(Outcome.REJECT_KEY_EMPTY, 0, 0, 0);
        }
        if (key.getBytes(StandardCharsets.UTF_8).length > config.maxKeyLength()) {
            return new AcquireResult(Outcome.REJECT_KEY_TOO_LONG, 0, 0, 0);
        }

        boolean reentrant = cmd.lockType() != LockType.SIMPLE;
        long effectiveLeaseMs = clampLease(cmd.requestedLeaseMs());

        while (true) {
            LockEntry e = lockTable.computeIfAbsent(key, k -> new LockEntry(k, reentrant));
            synchronized (e) {
                if (lockTable.get(key) != e) {
                    continue; // 条目在等待期间被移除，重试（design.md D4）
                }
                // 权威会话校验 + 原子登记，与 sessionClosed 的 remove 原子互斥。
                if (!sessions.touchIfPresent(cmd.sessionId(), key)) {
                    if (e.isEmpty()) {
                        lockTable.remove(key, e);
                    }
                    return new AcquireResult(Outcome.REJECT_SESSION, 0, 0, 0);
                }
                AcquireResult result = e.acquire(cmd, now, leaseTokenCounter::getAndIncrement, effectiveLeaseMs, config);
                if (result.outcome() == Outcome.GRANTED) {
                    leaseManager.offer(key, result.leaseToken(), now + result.grantedLeaseMs());
                }
                if (e.isEmpty()) {
                    lockTable.remove(key, e);
                }
                return result;
            }
        }
    }

    /**
     * 释放锁：写侧或读侧持有计数减一，归零时锁完全释放并对新队首触发通知。
     *
     * <p><b>判定顺序</b>（首个不满足者即为返回值）：
     * <ol>
     *   <li>会话已登记，否则 {@link ReleaseStatus#REJECT_SESSION}；</li>
     *   <li>该 key 有条目且存在持有者，否则 {@link ReleaseStatus#NOT_HELD}；</li>
     *   <li>凭证与当前租约匹配，否则 {@link ReleaseStatus#INVALID_TOKEN}
     *       （如租约已到期被回收后重放旧凭证）；</li>
     *   <li>该 {@code (sessionId, threadId)} 确为持有者，否则仍回
     *       {@link ReleaseStatus#NOT_HELD}（防御性归属校验，正常路径下
     *       凭证匹配即归属匹配，此分支理论不可达）。</li>
     * </ol>
     *
     * <p>可重入锁需逐层释放：每次调用只减一层计数，
     * {@code fullyReleased} 仅在计数归零（锁完全释放）时为 {@code true}。
     *
     * @param cmd 释放锁命令，携带获取时签发的租约凭证
     * @return 释放结果：状态与是否完全释放
     */
    public ReleaseResult release(ReleaseCommand cmd) {
        long now = clock.nowMs();
        if (!sessions.contains(cmd.sessionId())) {
            return new ReleaseResult(ReleaseStatus.REJECT_SESSION, false);
        }
        LockEntry e = lockTable.get(cmd.key());
        if (e == null) {
            return new ReleaseResult(ReleaseStatus.NOT_HELD, false);
        }
        List<Waiter> notify = new ArrayList<>();
        ReleaseResult result;
        synchronized (e) {
            result = e.release(cmd, now, config.headReplyTimeoutMs(), notify);
            if (e.isEmpty()) {
                lockTable.remove(cmd.key(), e);
            }
        }
        fireNotify(notify, cmd.key());
        return result;
    }

    /**
     * 续租：延长当前租约的到期时刻。
     *
     * <p><b>判定顺序</b>：会话未登记回 {@link ReleaseStatus#REJECT_SESSION}；
     * 该 key 无条目回 {@link ReleaseStatus#NOT_HELD}；条目内凭证为 0（无持有者）
     * 回 {@link ReleaseStatus#NOT_HELD}；凭证不匹配回
     * {@link ReleaseStatus#INVALID_TOKEN}。
     *
     * <p>校验通过后，期望租约同样经 0 取默认与上下限夹取，以新值刷新到期时刻，
     * 并向到期堆登记新记录。旧堆记录不作删除，由 {@link #expireDue} 的陈旧
     * 校验跳过。
     *
     * @param cmd 续租命令，携带获取时签发的租约凭证与期望租约时长（0 表示默认）
     * @return 续租结果：{@link ReleaseStatus#OK} 时携带新的到期时刻
     */
    public RenewResult renew(RenewCommand cmd) {
        long now = clock.nowMs();
        if (!sessions.contains(cmd.sessionId())) {
            return new RenewResult(ReleaseStatus.REJECT_SESSION, 0);
        }
        LockEntry e = lockTable.get(cmd.key());
        if (e == null) {
            return new RenewResult(ReleaseStatus.NOT_HELD, 0);
        }
        synchronized (e) {
            RenewResult result = e.renew(cmd, now, clampLease(cmd.requestedLeaseMs()));
            if (result.status() == ReleaseStatus.OK) {
                leaseManager.offer(cmd.key(), e.leaseToken(), result.newExpiresAtMs());
            }
            return result;
        }
    }

    /**
     * 到期扫描：强制释放所有已过期租约并对被释放 key 触发队首通知。
     * 这是惰性到期契约的执行端——租约到期本身不触发任何动作，须由调用方
     * （生产环境为租约扫描线程）周期性调用本方法回收。
     *
     * <p><b>陈旧校验</b>：到期堆只入不删（释放/续租均不删堆记录），
     * 故取出的堆记录可能已陈旧。仅当堆记录的凭证与到期时刻均与条目
     * 当前值一致时才执行强制释放；续租或重新授予后的旧记录因凭证或
     * 时刻已变而被安全跳过，不会误杀新租约。
     *
     * <p>强制释放清除该 key 的全部持有者（写侧与读侧）与租约，语义见
     * {@link LockEntry#forceExpire}。
     *
     * @return 本次因到期释放的锁数量
     */
    public int expireDue() {
        long now = clock.nowMs();
        int count = 0;
        for (LeaseManager.HeapEntry he : leaseManager.drainExpired(now)) {
            LockEntry e = lockTable.get(he.key());
            if (e == null) {
                continue;
            }
            List<Waiter> notify = new ArrayList<>();
            synchronized (e) {
                // 陈旧校验：堆记录的凭证与到期时刻均与条目当前值一致才视为有效。
                if (e.leaseToken() == he.leaseToken() && e.leaseExpiresAtMs() == he.expiresAtMs()) {
                    e.forceExpire(now, config.headReplyTimeoutMs(), notify);
                    count++;
                    if (e.isEmpty()) {
                        lockTable.remove(he.key(), e);
                    }
                }
            }
            fireNotify(notify, he.key());
        }
        return count;
    }

    /**
     * 队首响应超时清扫：移除"已通知但在 {@code headReplyTimeoutMs} 内
     * 未重发获取请求"的队首等待者，并对新队首补发通知。
     *
     * <p>这是 {@code AWAIT_NOTIFY} 推送丢失（连接断开、写出失败等）的兜底：
     * 队首被通知后进入"待重发"状态并记录响应截止时刻；截止前重发则正常
     * 授予，超时未重发则视为放弃，出队并让下一个等待者获得通知机会。
     *
     * @return 本次因超时移除的队首数量
     */
    public int sweepNotifiedHeads() {
        long now = clock.nowMs();
        int count = 0;
        for (LockEntry e : lockTable.values()) {
            List<Waiter> notify = new ArrayList<>();
            synchronized (e) {
                if (e.sweepNotifiedHead(now, config.headReplyTimeoutMs(), notify)) {
                    count++;
                    if (e.isEmpty()) {
                        lockTable.remove(e.key(), e);
                    }
                }
            }
            fireNotify(notify, e.key());
        }
        return count;
    }

    /**
     * 租约夹取：请求租约为 0 时替换为默认租约，再夹取到
     * {@code [minLeaseMs, maxLeaseMs]} 之间。授予与续租共用此规则，
     * 保证实际生效租约永远在配置限额内。
     *
     * @param requested 请求的租约时长（毫秒），{@code 0} 表示使用默认租约
     * @return 实际生效的租约时长（毫秒）
     */
    private long clampLease(long requested) {
        long v = requested == 0 ? config.defaultLeaseMs() : requested;
        return Math.max(config.minLeaseMs(), Math.min(config.maxLeaseMs(), v));
    }

    /**
     * 在条目锁之外统一触发收集到的队首通知事件。通知列表由条目内操作
     * （释放、到期、清扫等）在持锁期间填充，本方法负责出锁后逐个回调，
     * 避免回调实现中的任何行为反向影响条目锁的持有。
     *
     * @param notify 待通知的队首等待者列表
     * @param key    锁键，随事件一并报告
     */
    private void fireNotify(List<Waiter> notify, String key) {
        for (Waiter w : notify) {
            listener.notifyHead(w.sessionId(), w.requestId(), key);
        }
    }
}
