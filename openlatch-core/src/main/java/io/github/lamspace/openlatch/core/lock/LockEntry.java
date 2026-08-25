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
 * 单 key 状态：持有者（写侧 + 读侧）、计数、租约、FIFO 等待队列。
 *
 * <p>并发模型（设计说明书 §4.9）：条目内所有状态迁移都在 {@code synchronized(this)}
 * 内完成，任何调用路径最多持有一个条目锁。{@link #acquire}/{@link #release} 等
 * 方法自带同步；外层调用者（CoreEngine）再以 {@code synchronized(entry)} 包裹以
 * 原子完成"成员回查 + 状态迁移 + 条目移除"。通知事件经 {@code notify} 参数收集，
 * 由调用者在条目锁外统一触发。
 *
 * <p>锁类型约定：同一 key 应使用一致的锁类型（与 Redisson 约定一致）。
 * {@code reentrant} 在建条目时由首次请求的锁类型确定（{@code SIMPLE} 为 false，其余为 true）。
 */
public final class LockEntry {

    private final String key;
    private final boolean reentrant;

    /** 写侧/互斥侧持有者（REENTRANT / SIMPLE / WRITE 使用）。 */
    private Owner writer;
    private int writeCount;

    /** 读锁持有者 → 各自重入计数。 */
    private final Map<Owner, Integer> readers = new HashMap<>();

    /** 当前生效租约（无持有者时无效，token 为 0）。 */
    private long leaseToken;
    private long leaseExpiresAtMs;
    private long leaseMs;

    /** FIFO 等待队列。 */
    private final ArrayDeque<Waiter> waiters = new ArrayDeque<>();

    /**
     * 构造锁条目。
     *
     * @param key       锁键
     * @param reentrant 是否可重入（首次请求锁类型为 {@code SIMPLE} 时取 false，其余取 true）
     */
    public LockEntry(String key, boolean reentrant) {
        this.key = key;
        this.reentrant = reentrant;
    }

    /**
     * 状态迁移：按锁类型与队列规则授予、排队或拒绝（设计说明书 §4.3 规则集）。
     *
     * @param cmd                获取锁命令
     * @param now                当前时刻（毫秒）
     * @param leaseTokenSupplier 新租约凭证供应器，仅授予新持有时消费
     * @param effectiveLeaseMs   已夹取的实际租约时长（毫秒）
     * @param cfg                限额配置（队列深度上限）
     * @return 获取结果：授予（携带凭证与租约）、排队（携带位次）或拒绝
     */
    public synchronized AcquireResult acquire(AcquireCommand cmd, long now,
            LongSupplier leaseTokenSupplier, long effectiveLeaseMs, CoreConfig cfg) {
        Owner owner = new Owner(cmd.sessionId(), cmd.threadId());
        boolean isRead = cmd.lockType() == LockType.READ;

        // 规则 4：写侧重入（可重入类型）—— 计数 +1，租约整段刷新，同 token。
        if (!isRead && writer != null && writer.equals(owner) && reentrant) {
            writeCount++;
            leaseExpiresAtMs = now + leaseMs;
            return new AcquireResult(Outcome.GRANTED, leaseToken, leaseMs, 0);
        }
        // 读锁重入 —— readers 计数 +1，租约整段刷新，同 token。
        if (isRead) {
            Integer count = readers.get(owner);
            if (count != null) {
                readers.put(owner, count + 1);
                leaseExpiresAtMs = now + leaseMs;
                return new AcquireResult(Outcome.GRANTED, leaseToken, leaseMs, 0);
            }
        }

        // 快路径：无冲突持有者且队列空 → 直接授予（规则 3 / 规则 5）。
        boolean fastPath = isRead
                ? (writer == null && waiters.isEmpty())
                : (writer == null && readers.isEmpty() && waiters.isEmpty());
        if (fastPath) {
            if (isRead && !readers.isEmpty()) {
                // 加入已有读者：复用现有凭证、刷新租约，避免新 token 使旧读者释放失效。
                readers.put(owner, 1);
                leaseMs = effectiveLeaseMs;
                leaseExpiresAtMs = now + effectiveLeaseMs;
                return new AcquireResult(Outcome.GRANTED, leaseToken, effectiveLeaseMs, 0);
            }
            long token = leaseTokenSupplier.getAsLong();
            grant(owner, isRead, token, effectiveLeaseMs, now);
            return new AcquireResult(Outcome.GRANTED, token, effectiveLeaseMs, 0);
        }

        // 规则 7：队首重发命中（AWAIT_NOTIFY → 重发 ACQUIRE 的幂等落地路径）。
        Waiter head = waiters.peekFirst();
        if (head != null && head.sessionId() == cmd.sessionId() && head.requestId() == cmd.requestId()) {
            if (compatibleWithHold(head.lockType())) {
                waiters.pollFirst();
                long token = leaseTokenSupplier.getAsLong();
                grant(owner, head.lockType() == LockType.READ, token, effectiveLeaseMs, now);
                return new AcquireResult(Outcome.GRANTED, token, effectiveLeaseMs, 0);
            }
            // 防御：队首但当前不兼容（理论上不可达，规则 3 保证队首被通知后无人可越位），保持排队。
            return new AcquireResult(Outcome.QUEUED, 0, 0, 1);
        }

        // 规则 6：不可排队 → DENIED。
        if (!cmd.queueIfBusy()) {
            return new AcquireResult(Outcome.DENIED, 0, 0, 0);
        }

        // 幂等去重：同 (sessionId, requestId) 已在队 → 返回当前位次，不二次入队（§4.8）。
        int position = 0;
        for (Waiter w : waiters) {
            position++;
            if (w.sessionId() == cmd.sessionId() && w.requestId() == cmd.requestId()) {
                return new AcquireResult(Outcome.QUEUED, 0, 0, position);
            }
        }

        if (waiters.size() >= cfg.maxQueueDepthPerKey()) {
            return new AcquireResult(Outcome.REJECT_QUEUE_FULL, 0, 0, 0);
        }

        waiters.addLast(new Waiter(cmd.sessionId(), cmd.requestId(), cmd.lockType(), cmd.threadId(), now, 0));
        return new AcquireResult(Outcome.QUEUED, 0, 0, waiters.size());
    }

    /**
     * 释放持有：写侧或读侧计数减一，归零时清除租约并推进队首。
     *
     * @param cmd                释放锁命令，凭证须与当前租约匹配
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表，由调用方在条目锁外触发
     * @return 释放结果：状态与是否完全释放
     */
    public synchronized ReleaseResult release(ReleaseCommand cmd, long now,
            long headReplyTimeoutMs, List<Waiter> notify) {
        Owner owner = new Owner(cmd.sessionId(), cmd.threadId());
        if (writer == null && readers.isEmpty()) {
            return new ReleaseResult(ReleaseStatus.NOT_HELD, false);
        }
        if (leaseToken != cmd.leaseToken()) {
            return new ReleaseResult(ReleaseStatus.INVALID_TOKEN, false);
        }

        if (writer != null && writer.equals(owner)) {
            writeCount--;
            if (writeCount == 0) {
                writer = null;
                clearLease();
                notifyHeadIfPossible(now, headReplyTimeoutMs, notify);
                return new ReleaseResult(ReleaseStatus.OK, true);
            }
            return new ReleaseResult(ReleaseStatus.OK, false);
        }

        Integer count = readers.get(owner);
        if (count != null) {
            int next = count - 1;
            if (next == 0) {
                readers.remove(owner);
                if (readers.isEmpty()) {
                    clearLease();
                    notifyHeadIfPossible(now, headReplyTimeoutMs, notify);
                    return new ReleaseResult(ReleaseStatus.OK, true);
                }
                return new ReleaseResult(ReleaseStatus.OK, false);
            }
            readers.put(owner, next);
            return new ReleaseResult(ReleaseStatus.OK, false);
        }

        // token 匹配即归属匹配（防御性保留的归属校验失败）。
        return new ReleaseResult(ReleaseStatus.NOT_HELD, false);
    }

    /**
     * 续租：凭证匹配时以新租约刷新到期时刻。
     *
     * @param cmd              续租命令，凭证须与当前租约匹配
     * @param now              当前时刻（毫秒）
     * @param effectiveLeaseMs 已夹取的实际租约时长（毫秒）
     * @return 续租结果：{@link ReleaseStatus#OK} 时携带新到期时刻
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
     * 租约到期强制释放全部持有者并通知队首。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表，由调用方在条目锁外触发
     */
    public synchronized void forceExpire(long now, long headReplyTimeoutMs, List<Waiter> notify) {
        writer = null;
        writeCount = 0;
        readers.clear();
        clearLease();
        notifyHeadIfPossible(now, headReplyTimeoutMs, notify);
    }

    /**
     * 队首响应超时清扫：移除超时的已通知队首，并对新队首补通知。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表，由调用方在条目锁外触发
     * @return 是否移除了超时队首
     */
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

    /**
     * 会话清理：释放该会话的全部持有（写侧 + 读侧）、摘除其等待项，并做队首前进检查。
     *
     * @param sessionId          要清理的会话
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表，由调用方在条目锁外触发
     */
    public synchronized void removeSession(long sessionId, long now,
            long headReplyTimeoutMs, List<Waiter> notify) {
        if (writer != null && writer.sessionId() == sessionId) {
            writer = null;
            writeCount = 0;
        }
        readers.keySet().removeIf(o -> o.sessionId() == sessionId);
        waiters.removeIf(w -> w.sessionId() == sessionId);
        if (writer == null && readers.isEmpty()) {
            clearLease();
        }
        notifyHeadIfPossible(now, headReplyTimeoutMs, notify);
    }

    private void grant(Owner owner, boolean isRead, long token, long effectiveLeaseMs, long now) {
        if (isRead) {
            readers.put(owner, 1);
        } else {
            writer = owner;
            writeCount = 1;
        }
        leaseToken = token;
        leaseMs = effectiveLeaseMs;
        leaseExpiresAtMs = now + effectiveLeaseMs;
    }

    private void clearLease() {
        leaseToken = 0;
        leaseExpiresAtMs = 0;
        leaseMs = 0;
    }

    /** 锁无冲突持有者且队列非空时，标记并通知队首（仅队首，不批量唤醒）。 */
    private void notifyHeadIfPossible(long now, long headReplyTimeoutMs, List<Waiter> notify) {
        if (writer != null || !readers.isEmpty() || waiters.isEmpty()) {
            return;
        }
        Waiter head = waiters.peekFirst();
        if (head.notified()) {
            return;
        }
        Waiter updated = head.withDeadline(now + headReplyTimeoutMs);
        waiters.pollFirst();
        waiters.addFirst(updated);
        notify.add(updated);
    }

    private boolean compatibleWithHold(LockType lockType) {
        if (lockType == LockType.READ) {
            return writer == null;
        }
        return writer == null && readers.isEmpty();
    }

    /**
     * 锁键。须在持有条目锁时调用（CoreEngine 保证）。
     *
     * @return 锁键
     */
    public String key() {
        return key;
    }

    /**
     * 当前租约凭证，无持有者时为 0。须在持有条目锁时调用（CoreEngine 保证）。
     *
     * @return 租约凭证
     */
    public long leaseToken() {
        return leaseToken;
    }

    /**
     * 当前租约到期时刻。须在持有条目锁时调用（CoreEngine 保证）。
     *
     * @return 到期时刻（毫秒）
     */
    public long leaseExpiresAtMs() {
        return leaseExpiresAtMs;
    }

    /**
     * 是否无持有者且无等待者。须在持有条目锁时调用（CoreEngine 保证）。
     *
     * @return 条目为空返回 true
     */
    public boolean isEmpty() {
        return writer == null && readers.isEmpty() && waiters.isEmpty();
    }
}
