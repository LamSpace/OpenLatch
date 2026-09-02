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
 * 单 key 的锁状态机：持有者（写侧 + 读侧）、重入计数、租约、FIFO 等待队列。
 *
 * <p><b>状态要素</b>：
 * <ul>
 *   <li>写侧：{@code writer} + {@code writeCount}（互斥持有与重入层数）；</li>
 *   <li>读侧：{@code readers}（各读者及其重入计数）；</li>
 *   <li>租约三元组：凭证 {@code leaseToken}、时长 {@code leaseMs}、
 *       到期时刻 {@code leaseExpiresAtMs}。整 key 共享单一租约——读锁的多个
 *       读者也共用一个凭证，加入已有读者时复用凭证，避免新 token 使
 *       旧读者的释放失效；</li>
 *   <li>等待队列：{@code waiters}，FIFO；队首可能处于"已通知、待重发"状态。</li>
 * </ul>
 *
 * <p><b>并发模型</b>（设计说明书 §4.9）：条目内所有状态迁移都在
 * {@code synchronized(this)} 内完成，任何调用路径最多持有一个条目锁。
 * {@link #acquire}/{@link #release} 等方法自带同步；外层调用者
 * （{@code CoreEngine}）再以 {@code synchronized(entry)} 包裹以原子完成
 * "成员回查 + 状态迁移 + 条目移除"。通知事件经 {@code notify} 参数收集，
 * 由调用者在条目锁外统一触发。
 *
 * <p><b>锁类型约定</b>：同一 key 应使用一致的锁类型（与 Redisson 约定一致）。
 * {@code reentrant} 在建条目时由首次请求的锁类型确定（{@code SIMPLE} 为 false，
 * 其余为 true），建条目后不再变化。
 */
public final class LockEntry {

    /** 锁键。 */
    private final String key;
    /** 是否可重入，建条目时由首次请求的锁类型确定，之后不变。 */
    private final boolean reentrant;

    /** 写侧/互斥侧持有者（REENTRANT / SIMPLE / WRITE 使用）。 */
    private Owner writer;
    /** 写侧重入层数，归零表示写侧无持有。 */
    private int writeCount;

    /** 读锁持有者 → 各自重入计数。 */
    private final Map<Owner, Integer> readers = new HashMap<>();

    /** 当前租约凭证，无持有者时为 0。 */
    private long leaseToken;
    /** 当前租约到期时刻（毫秒）。 */
    private long leaseExpiresAtMs;
    /** 当前租约时长（毫秒），续租/重入刷新时随之更新。 */
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
     * 快照重建工厂（详设 §7.1，仅供 {@code CoreEngine.restoreFrom} 在加载快照时
     * 构造"已持有"状态的条目）：以传入的持有与租约快照直接装配条目初态，
     * 不经任何状态迁移规则——凭证、到期时刻、持有计数按快照原值落地。
     *
     * <p><b>不变量</b>：等待队列恒为空（集群引擎无本地等待项，单机恢复不存在
     * 等待语义）；写侧持有由 {@code writer}/{@code writeCount} 表达，读侧持有
     * 由 {@code readers} 表表达，二者互斥（{@code writer == null} 或
     * {@code readers.isEmpty()}）。本工厂不校验该互斥性——校验责任在输入值
     * 对象构造处（{@code CoreStateRestore}），以调用方契约+构造校验双保险。
     *
     * @param key           锁键
     * @param reentrant     是否可重入（与 {@code LockType != SIMPLE} 同判定）
     * @param writer        写侧持有者；读锁条目为 {@code null}
     * @param writeCount    写侧重入层数（{@code writer == null} 时忽略）
     * @param readers       读者 → 重入计数（写类条目传入空表）
     * @param leaseToken    当前租约凭证
     * @param leaseMs       实际生效租期（毫秒）
     * @param leaseExpiresAtMs 当前到期时刻（毫秒）
     * @return 持有快照初态的条目（等待队列空）
     */
    public static LockEntry restored(String key, boolean reentrant, Owner writer, int writeCount,
            Map<Owner, Integer> readers, long leaseToken, long leaseMs, long leaseExpiresAtMs) {
        LockEntry e = new LockEntry(key, reentrant);
        e.writer = writer;
        e.writeCount = writer == null ? 0 : writeCount;
        e.readers.putAll(readers);
        e.leaseToken = leaseToken;
        e.leaseMs = leaseMs;
        e.leaseExpiresAtMs = leaseExpiresAtMs;
        return e;
    }

    /**
     * 状态迁移：按下列规则顺序授予、排队或拒绝（设计说明书 §4.3 规则集，
     * 首个命中者即为结果）：
     * <ol>
     *   <li><b>写侧重入</b>：同归属已持有写侧且条目可重入——重入计数加一，
     *       同一凭证，租约按本次请求值整段刷新（{@code now + effectiveLeaseMs}，
     *       重入者由此获得调整租约的通道）→ 授予；</li>
     *   <li><b>读侧重入</b>：同归属已在读者表——该读者计数加一，同一凭证，
     *       租约按本次请求值整段刷新（全体读者共享新到期时刻）→ 授予；</li>
     *   <li><b>快路径</b>：无冲突持有者且队列空——读请求且已有其他读者时
     *       加入读者表并复用现有凭证（避免新凭证使旧读者的释放失效），
     *       否则签发新凭证 → 授予；</li>
     *   <li><b>队首重发命中</b>：队首等待者与本次请求同属
     *       {@code (sessionId, requestId)} 且与当前持有兼容——出队并授予
     *       （这是 {@code AWAIT_NOTIFY} 后重发的幂等落地路径）；
     *       理论不可达的不兼容兜底为保持排队；</li>
     *   <li><b>不可排队</b>：{@code queueIfBusy} 为假（立即式）→ 拒绝
     *       （{@code DENIED}）；</li>
     *   <li><b>幂等去重</b>：同 {@code (sessionId, requestId)} 已在队——
     *       不二次入队，返回当前位次（1 起）→ 排队；</li>
     *   <li><b>队列已满</b>：等待数达到 {@code maxQueueDepthPerKey} →
     *       拒绝（{@code REJECT_QUEUE_FULL}）；</li>
     *   <li><b>入队</b>：追加至队尾 → 排队，位次为入队后队列长度。</li>
     * </ol>
     *
     * <p>一切授予路径（新持有、重入、加入已有读者）的租约均以
     * {@code effectiveLeaseMs} 整段刷新，口径统一（design D2：请求值 0 取
     * 默认、非 0 钳制到 [min,max]）；重入与加入已有读者不消费凭证供应器。
     * 会话有效性与条目存活校验由 {@code CoreEngine} 在本方法之外完成。
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

        // 规则 4：写侧重入（可重入类型）—— 计数 +1，租约按本次请求值整段刷新，同 token。
        // 口径与读侧重入、加入已有读者群一致：effectiveLeaseMs 已由 CoreEngine 按
        // 请求值钳制（0 取默认），重入者由此获得延长/缩短租约的通道（design D2）。
        if (!isRead && writer != null && writer.equals(owner) && reentrant) {
            writeCount++;
            leaseMs = effectiveLeaseMs;
            leaseExpiresAtMs = now + effectiveLeaseMs;
            return new AcquireResult(Outcome.GRANTED, leaseToken, effectiveLeaseMs, 0);
        }
        // 读锁重入 —— readers 计数 +1，租约按本次请求值整段刷新（全体读者共享），同 token。
        if (isRead) {
            Integer count = readers.get(owner);
            if (count != null) {
                readers.put(owner, count + 1);
                leaseMs = effectiveLeaseMs;
                leaseExpiresAtMs = now + effectiveLeaseMs;
                return new AcquireResult(Outcome.GRANTED, leaseToken, effectiveLeaseMs, 0);
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
     * <p><b>判定顺序</b>（首个命中者即为结果）：
     * <ol>
     *   <li>无任何持有者 → {@code NOT_HELD}；</li>
     *   <li>凭证与当前租约不匹配 → {@code INVALID_TOKEN}；</li>
     *   <li>写侧持有者匹配：计数减一，归零时清除写侧与租约、对队首触发
     *       通知收集 → {@code OK}（{@code fullyReleased} 为归零与否）；</li>
     *   <li>读侧持有者匹配：该读者计数减一并从读者表移除（归零时），
     *       最后一个读者移除时清除租约、对队首触发通知收集 → {@code OK}；</li>
     *   <li>凭证匹配但归属均不匹配 → {@code NOT_HELD}（防御性保留，
     *       正常路径下凭证匹配即归属匹配，理论不可达）。</li>
     * </ol>
     *
     * <p>重入锁逐层释放：单次调用只减一层计数。通知仅收集到
     * {@code notify} 列表，由调用方在条目锁外统一触发。
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
     * 续租：凭证匹配时以新租约时长刷新到期时刻。
     *
     * <p><b>判定顺序</b>：凭证为 0（无持有者）→ {@code NOT_HELD}；
     * 凭证不匹配 → {@code INVALID_TOKEN}；否则以 {@code effectiveLeaseMs}
     * 更新租约时长并令到期时刻为 {@code now + effectiveLeaseMs} → {@code OK}。
     * 不更换凭证，续租前后持有者身份不变。到期堆中旧记录的清理由
     * {@code CoreEngine.expireDue} 的陈旧校验负责，本方法不触及到期堆。
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
     * 租约到期强制释放：清除全部持有者（写侧与读侧）与租约，并对队首
     * 触发通知收集。由 {@code CoreEngine.expireDue} 在陈旧校验通过后调用——
     * 即到期堆记录的凭证与到期时刻仍与条目当前值一致，确认期间无续租
     * 或重新授予。等待队列不受影响：锁释放后等待者照常竞争获取。
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
     * 队首响应超时清扫：队首处于"已通知、待重发"状态且响应截止时刻
     * 已过（{@code notifyDeadlineMs <= now}）时，将其出队（视为放弃），
     * 并对新队首补发通知收集。队首未通知或未超时则不做任何变更。
     * 由 {@code CoreEngine.sweepNotifiedHeads} 周期调用，是通知丢失的兜底。
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
     * 会话清理：移除该会话在本条目的全部痕迹——写侧持有（若是持有者）、
     * 读侧持有（从读者表移除）、等待队列中的全部等待项；若清理后无任何
     * 持有者则清除租约；最后对队首做前进检查（被移除者恰为已通知队首时，
     * 新队首获得通知机会）。由 {@code CoreEngine.sessionClosed} 在断连
     * 清理时逐 key 调用。
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

    /**
     * 授予新持有：按读/写落入读者表或写侧（计数置 1），并以新凭证
     * 建立整段租约（时长 {@code effectiveLeaseMs}，到期
     * {@code now + effectiveLeaseMs}）。仅快路径与队首重发命中时调用，
     * 重入与加入已有读者不经由此方法（它们复用凭证）。
     *
     * @param owner            被授予的归属
     * @param isRead           true 为读锁授予，false 为写侧授予
     * @param token            新签发的租约凭证
     * @param effectiveLeaseMs 已夹取的实际租约时长（毫秒）
     * @param now              当前时刻（毫秒）
     */
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

    /**
     * 清除租约三元组（凭证、时长、到期时刻全部归零），表示当前无有效租约。
     * 仅在持有者计数全部归零或强制到期时调用。
     */
    private void clearLease() {
        leaseToken = 0;
        leaseExpiresAtMs = 0;
        leaseMs = 0;
    }

    /**
     * 队首通知收集：锁无冲突持有者（写侧与读侧均空）且队列非空时，
     * 将队首标记为"已通知、待重发"（写入响应截止时刻
     * {@code now + headReplyTimeoutMs}，以新实例替换队首保持不可变），
     * 并加入通知列表。仅通知队首一个，不批量唤醒。已处于待重发状态的
     * 队首不重复通知。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表，由调用方在条目锁外触发
     */
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

    /**
     * 判定请求的锁类型与当前持有是否兼容，用于队首重发命中时的落地检查：
     * 读请求只需写侧无人持有（可与其他读者共存）；写类请求须写侧与读侧
     * 均无人持有。
     *
     * @param lockType 请求的锁类型
     * @return 与当前持有兼容返回 true
     */
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
