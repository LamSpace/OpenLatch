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

package io.github.lamspace.openlatch.client.internal;

import io.github.lamspace.openlatch.client.LockLostException;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LeaseRenewRequest;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;

/**
 * 看门狗：持锁期间的自动续租调度（详设 §6.6）。
 *
 * <p><b>续租周期</b>：{@code grantedLeaseMs / 3}（概要设计 §6.3）。续租请求
 * 自身的超时取 {@code min(请求超时, 周期)}。
 *
 * <p><b>失败判定</b>：
 * <ul>
 *   <li>响应 {@code INVALID_TOKEN}/{@code NOT_HELD}/{@code SESSION_EXPIRED}
 *       → <b>锁已失效</b>：立即停止续租并触发锁丢失回调；</li>
 *   <li>请求超时或其它非成功状态 → 记录连续失败次数，下一周期重试；
 *       <b>连续 2 次</b> → 判定失效并触发回调；</li>
 *   <li>成功 → 刷新本地到期时间并重置失败计数。</li>
 * </ul>
 *
 * <p><b>断连正交化（design.md D5）</b>：连接非 ACTIVE 时跳过本次续租发送
 * 且不计失败次数——断连场景的失锁裁决由 {@code lostAt} 机制独占，
 * 看门狗只裁决"连接正常但服务端不认账"。
 *
 * <p><b>并发</b>：单条目的续租周期严格串行（响应处理中才调度下一周期），
 * 条目上的失败计数因此无竞争；每次执行前以持锁簿记复核条目仍然在册，
 * 已解锁/已失锁的条目自然终止。
 */
public final class Watchdog {

    /** 触发失锁判定的连续超时次数阈值。 */
    private static final int MAX_CONSECUTIVE_TIMEOUTS = 2;
    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(Watchdog.class);

    /** 共享定时器：续租周期任务挂于此。 */
    private final HashedWheelTimer timer;
    /** 多路复用器：续租请求经此出站。 */
    private final RequestMultiplexer multiplexer;
    /** 持锁簿记：执行前复核条目在册状态。 */
    private final HeldLockRegistry registry;
    /** 每请求超时（毫秒）。 */
    private final long requestTimeoutMs;
    /** 连接可用性判断：断连跳过续租（D5）。 */
    private final BooleanSupplier connectionActive;
    /** 失锁回调：停止续租、移除登记后由客户端触发监听器。 */
    private final BiConsumer<HeldLockRegistry.HeldEntry, LockLostException> onLockLost;

    /**
     * 创建看门狗。
     *
     * @param timer            共享定时器
     * @param multiplexer      多路复用器
     * @param registry         持锁簿记
     * @param requestTimeoutMs 每请求超时（毫秒）
     * @param connectionActive 连接可用性判断
     * @param onLockLost       失锁回调
     */
    public Watchdog(HashedWheelTimer timer, RequestMultiplexer multiplexer,
            HeldLockRegistry registry, long requestTimeoutMs, BooleanSupplier connectionActive,
            BiConsumer<HeldLockRegistry.HeldEntry, LockLostException> onLockLost) {
        this.timer = timer;
        this.multiplexer = multiplexer;
        this.registry = registry;
        this.requestTimeoutMs = requestTimeoutMs;
        this.connectionActive = connectionActive;
        this.onLockLost = onLockLost;
    }

    /**
     * 为持锁条目启动周期续租：首个周期在 {@code grantedLeaseMs / 3} 后执行。
     *
     * @param entry 持锁条目
     */
    public void start(HeldLockRegistry.HeldEntry entry) {
        schedule(entry, periodMs(entry));
    }

    /**
     * 停止条目的续租（完全释放或失锁后调用）。幂等。
     *
     * @param entry 持锁条目
     */
    public void stop(HeldLockRegistry.HeldEntry entry) {
        io.netty.util.Timeout task = entry.watchdogTask();
        if (task != null) {
            task.cancel();
            entry.setWatchdogTask(null);
        }
    }

    /**
     * 调度下一次续租。
     *
     * @param entry   持锁条目
     * @param delayMs 延时（毫秒）
     */
    private void schedule(HeldLockRegistry.HeldEntry entry, long delayMs) {
        entry.setWatchdogTask(timer.newTimeout(t -> tick(entry), delayMs, TimeUnit.MILLISECONDS));
    }

    /**
     * 续租周期执行：条目已不在册 → 终止；断连 → 跳过不计数（D5）；
     * 否则发送续租请求并在响应处理中决定下一周期。
     *
     * @param entry 持锁条目
     */
    private void tick(HeldLockRegistry.HeldEntry entry) {
        if (!stillHeld(entry)) {
            return;
        }
        if (!connectionActive.getAsBoolean()) {
            schedule(entry, periodMs(entry));
            return;
        }
        Envelope renew = Envelope.newBuilder()
                .setType(MessageType.LEASE_RENEW)
                .setLeaseRenewRequest(LeaseRenewRequest.newBuilder()
                        .setKey(entry.key())
                        .setLeaseToken(entry.leaseToken())
                        .setLeaseMs(entry.grantedLeaseMs()))
                .build();
        long renewTimeoutMs = Math.min(requestTimeoutMs, periodMs(entry));
        multiplexer.send(renew.toBuilder(), renewTimeoutMs)
                .whenComplete((resp, err) -> onResult(entry, resp, err));
    }

    /**
     * 续租结果处理：成功刷新并重置；明确失效错误即时失锁；
     * 超时/其它错误计数，连续 {@value #MAX_CONSECUTIVE_TIMEOUTS} 次失锁。
     *
     * @param entry 持锁条目
     * @param resp  续租响应；失败为 {@code null}
     * @param err   收发异常；成功为 {@code null}
     */
    private void onResult(HeldLockRegistry.HeldEntry entry, Envelope resp, Throwable err) {
        if (!stillHeld(entry)) {
            return;
        }
        if (err != null) {
            if (entry.recordRenewTimeout() >= MAX_CONSECUTIVE_TIMEOUTS) {
                lockLost(entry, new LockLostException(
                        "renew of '" + entry.key() + "' timed out "
                                + MAX_CONSECUTIVE_TIMEOUTS + " times in a row"));
            } else {
                schedule(entry, periodMs(entry));
            }
            return;
        }
        StatusCode status = resp.getLeaseRenewResponse().getStatus();
        if (status == StatusCode.OK) {
            entry.resetRenewTimeouts();
            entry.markRenewed(System.currentTimeMillis());
            schedule(entry, periodMs(entry));
            return;
        }
        if (status == StatusCode.INVALID_TOKEN || status == StatusCode.NOT_HELD
                || status == StatusCode.SESSION_EXPIRED) {
            lockLost(entry, new LockLostException(status,
                    "renew of '" + entry.key() + "' rejected: " + status));
            return;
        }
        // OVERLOADED / INTERNAL_ERROR 等：按瞬时失败计数重试。
        log.debug("renew of '{}' got transient status {}", entry.key(), status);
        if (entry.recordRenewTimeout() >= MAX_CONSECUTIVE_TIMEOUTS) {
            lockLost(entry, new LockLostException(status,
                    "renew of '" + entry.key() + "' failed consecutively: " + status));
        } else {
            schedule(entry, periodMs(entry));
        }
    }

    /**
     * 失锁处理：停止续租、移除簿记、回调客户端触发监听。
     *
     * @param entry 持锁条目
     * @param cause 失锁原因
     */
    private void lockLost(HeldLockRegistry.HeldEntry entry, LockLostException cause) {
        stop(entry);
        registry.remove(entry.key(), entry.threadId());
        onLockLost.accept(entry, cause);
    }

    /**
     * 条目是否仍在簿记在册（未解锁、未失锁）。
     *
     * @param entry 持锁条目
     * @return 在册返回 {@code true}
     */
    private boolean stillHeld(HeldLockRegistry.HeldEntry entry) {
        return registry.get(entry.key(), entry.threadId()) == entry;
    }

    /**
     * 续租周期：实际生效租约的三分之一，至少 1ms。
     *
     * @param entry 持锁条目
     * @return 周期（毫秒）
     */
    private static long periodMs(HeldLockRegistry.HeldEntry entry) {
        return Math.max(1, entry.grantedLeaseMs() / 3);
    }
}
