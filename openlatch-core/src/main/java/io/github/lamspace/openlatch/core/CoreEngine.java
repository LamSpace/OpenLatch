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
 * 锁语义核心门面。纯 Java、零外部运行依赖、无网络。时间可经 {@link Clock} 注入，
 * 事件经 {@link CoreEventListener} 向外报告（条目锁外触发）。
 */
public final class CoreEngine {

    private final CoreConfig config;
    private final Clock clock;
    private final CoreEventListener listener;
    private final LockTable lockTable = new LockTable();
    private final LeaseManager leaseManager = new LeaseManager();
    private final SessionRegistry sessions = new SessionRegistry();
    private final AtomicLong leaseTokenCounter = new AtomicLong(1);

    public CoreEngine(CoreConfig config, Clock clock, CoreEventListener listener) {
        this.config = Objects.requireNonNull(config);
        this.clock = Objects.requireNonNull(clock);
        this.listener = Objects.requireNonNull(listener);
    }

    /** 新会话登记，返回 sessionId。 */
    public long sessionOpened() {
        long id = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        sessions.register(id);
        return id;
    }

    /** 会话关闭：释放其全部持锁、摘除其全部等待项。幂等。 */
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

    /** 到期扫描：释放所有已过期租约并对被释放 key 触发队首通知。返回本次释放数量。 */
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

    /** 队首响应超时清扫：移除"已通知但超时未重发"的队首，并对新队首补通知。 */
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

    private long clampLease(long requested) {
        long v = requested == 0 ? config.defaultLeaseMs() : requested;
        return Math.max(config.minLeaseMs(), Math.min(config.maxLeaseMs(), v));
    }

    private void fireNotify(List<Waiter> notify, String key) {
        for (Waiter w : notify) {
            listener.notifyHead(w.sessionId(), w.requestId(), key);
        }
    }
}
