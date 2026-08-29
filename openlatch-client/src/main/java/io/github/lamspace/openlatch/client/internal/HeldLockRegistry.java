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

import io.github.lamspace.openlatch.client.LockType;
import io.netty.util.Timeout;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 本地持锁簿记（详设 §6.1/§6.3）。
 *
 * <p><b>核心约束（design.md D4）：只记归属、不记重入计数。</b>
 * 重入计数的唯一事实源在服务端；本表以 {@code (key, threadId)} 为键记录
 * "哪个线程持有什么锁、凭据与到期信息"。同线程重入获取时服务端返回同凭据
 * 的授予，本表登记命中既有条目后不做任何变更；每次解锁都发送释放请求，
 * 以服务端响应的 {@code fullyReleased} 决定是否移除登记。
 *
 * <p>读锁多读者场景下同一 {@code key} 可存在多个条目（每读者线程一个），
 * 各条目共享服务端条目的同一租约凭据。
 */
public final class HeldLockRegistry {

    /** 持锁条目表：(key, threadId) → 条目。 */
    private final ConcurrentMap<RegistryKey, HeldEntry> entries = new ConcurrentHashMap<>();

    /**
     * 创建空簿记。
     */
    public HeldLockRegistry() {
        // 无状态初始化：条目表随登记惰性填充
    }

    /**
     * 持锁条目的查找键。
     *
     * @param key      锁键
     * @param threadId 持有线程标识
     */
    private record RegistryKey(String key, long threadId) {
    }

    /**
     * 单个持锁条目：归属与凭据的本地快照。
     *
     * <p>可变字段（{@code lastRenewAtMs}）仅由看门狗续租成功路径更新，
     * 读取方（失锁时刻计算）容忍其弱一致刷新。
     */
    public static final class HeldEntry {
        /** 锁键。 */
        private final String key;
        /** 持有线程标识。 */
        private final long threadId;
        /** 锁类型。 */
        private final LockType lockType;
        /** 服务端签发的租约凭据。 */
        private final long leaseToken;
        /** 实际生效租约（毫秒）。 */
        private final long grantedLeaseMs;
        /** 获取时的会话 id：重连后用于识别陈旧条目（旧会话的锁必然失效）。 */
        private final long sessionId;
        /** 上次成功续租（或授予）的本地时刻（epoch 毫秒）：失锁时刻计算的基准。 */
        private volatile long lastRenewAtMs;
        /** 看门狗续租任务句柄；注销续租时取消。 */
        private volatile Timeout watchdogTask;
        /** 断连失锁时刻（{@code lostAt}）定时任务句柄；重连裁决时取消。 */
        private volatile Timeout lostAtTask;
        /** 续租请求连续超时次数（成功或明确失效错误时重置/终止）。 */
        private volatile int consecutiveRenewTimeouts;

        /**
         * 创建持锁条目。
         *
         * @param key            锁键
         * @param threadId       持有线程标识
         * @param lockType       锁类型
         * @param leaseToken     租约凭据
         * @param grantedLeaseMs 实际生效租约（毫秒）
         * @param sessionId      获取时的会话 id
         * @param nowMs          当前时刻（epoch 毫秒）
         */
        HeldEntry(String key, long threadId, LockType lockType, long leaseToken,
                long grantedLeaseMs, long sessionId, long nowMs) {
            this.key = key;
            this.threadId = threadId;
            this.lockType = lockType;
            this.leaseToken = leaseToken;
            this.grantedLeaseMs = grantedLeaseMs;
            this.sessionId = sessionId;
            this.lastRenewAtMs = nowMs;
        }

        /**
         * 锁键。
         *
         * @return 锁键
         */
        public String key() {
            return key;
        }

        /**
         * 持有线程标识。
         *
         * @return 线程标识
         */
        public long threadId() {
            return threadId;
        }

        /**
         * 锁类型。
         *
         * @return 锁类型
         */
        public LockType lockType() {
            return lockType;
        }

        /**
         * 租约凭据。
         *
         * @return 凭据
         */
        public long leaseToken() {
            return leaseToken;
        }

        /**
         * 实际生效租约（毫秒）。
         *
         * @return 租约时长
         */
        public long grantedLeaseMs() {
            return grantedLeaseMs;
        }

        /**
         * 获取时的会话 id。
         *
         * @return sessionId
         */
        public long sessionId() {
            return sessionId;
        }

        /**
         * 上次成功续租（或授予）的本地时刻。
         *
         * @return epoch 毫秒
         */
        public long lastRenewAtMs() {
            return lastRenewAtMs;
        }

        /**
         * 续租成功后刷新本地时刻。仅由看门狗成功路径调用。
         *
         * @param nowMs 续租成功的本地时刻（epoch 毫秒）
         */
        public void markRenewed(long nowMs) {
            this.lastRenewAtMs = nowMs;
        }

        /**
         * 失锁时刻：上次成功续租 + 实际生效租约（详设 §6.2）。
         *
         * @return 失锁时刻（epoch 毫秒）
         */
        public long lostAtMs() {
            return lastRenewAtMs + grantedLeaseMs;
        }

        /**
         * 设置看门狗续租任务句柄。仅由看门狗调用。
         *
         * @param task 任务句柄
         */
        public void setWatchdogTask(Timeout task) {
            this.watchdogTask = task;
        }

        /**
         * 设置失锁时刻定时任务句柄。仅由断连/重连裁决路径调用。
         *
         * @param task 任务句柄
         */
        public void setLostAtTask(Timeout task) {
            this.lostAtTask = task;
        }

        /**
         * 失锁时刻定时任务句柄。
         *
         * @return 任务句柄；未登记失锁时刻为 {@code null}
         */
        public Timeout lostAtTask() {
            return lostAtTask;
        }

        /**
         * 看门狗续租任务句柄。
         *
         * @return 任务句柄；未启动续租为 {@code null}
         */
        public Timeout watchdogTask() {
            return watchdogTask;
        }

        /**
         * 续租请求超时一次：递增连续超时计数并返回新值。
         *
         * @return 递增后的连续超时次数
         */
        public int recordRenewTimeout() {
            return ++consecutiveRenewTimeouts;
        }

        /**
         * 续租成功：重置连续超时计数。
         */
        public void resetRenewTimeouts() {
            consecutiveRenewTimeouts = 0;
        }

        /**
         * 当前连续续租超时次数。
         *
         * @return 连续超时次数
         */
        public int consecutiveRenewTimeouts() {
            return consecutiveRenewTimeouts;
        }
    }

    /**
     * 登记持锁：同 {@code (key, threadId)} 已存在时（重入）返回既有条目且不覆盖，
     * 避免双账本漂移（详设 §6.3）。
     *
     * @param key            锁键
     * @param threadId       持有线程标识
     * @param lockType       锁类型
     * @param leaseToken     租约凭据
     * @param grantedLeaseMs 实际生效租约（毫秒）
     * @param sessionId      获取时的会话 id
     * @param nowMs          当前时刻（epoch 毫秒）
     * @return 登记生效的条目（重入时为既有条目）
     */
    public HeldEntry register(String key, long threadId, LockType lockType, long leaseToken,
            long grantedLeaseMs, long sessionId, long nowMs) {
        RegistryKey registryKey = new RegistryKey(key, threadId);
        HeldEntry entry = new HeldEntry(key, threadId, lockType, leaseToken,
                grantedLeaseMs, sessionId, nowMs);
        HeldEntry existing = entries.putIfAbsent(registryKey, entry);
        return existing != null ? existing : entry;
    }

    /**
     * 按 {@code (key, threadId)} 查询条目。
     *
     * @param key      锁键
     * @param threadId 线程标识
     * @return 条目；未持有返回 {@code null}
     */
    public HeldEntry get(String key, long threadId) {
        return entries.get(new RegistryKey(key, threadId));
    }

    /**
     * 移除条目。
     *
     * @param key      锁键
     * @param threadId 线程标识
     * @return 被移除的条目；不存在返回 {@code null}
     */
    public HeldEntry remove(String key, long threadId) {
        return entries.remove(new RegistryKey(key, threadId));
    }

    /**
     * 全部条目的不可变快照：断连失锁登记、重连失锁裁决、关停释放遍历用。
     *
     * @return 条目集合
     */
    public Collection<HeldEntry> entries() {
        return Collections.unmodifiableCollection(entries.values());
    }

    /**
     * 该锁键是否仍有任一线程的本地持有条目。锁完全释放（计数归零）后
     * 判否，供客户端丢弃该键的附属登记（如锁丢失监听器，详设 §6.3、
     * 变更 phase1-audit-remediation design D4）。
     *
     * @param key 锁键
     * @return 仍存在任一持有条目返回 {@code true}
     */
    public boolean hasAnyFor(String key) {
        for (HeldEntry entry : entries.values()) {
            if (entry.key().equals(key)) {
                return true;
            }
        }
        return false;
    }
}
