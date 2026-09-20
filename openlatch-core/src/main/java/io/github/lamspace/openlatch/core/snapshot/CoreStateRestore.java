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

package io.github.lamspace.openlatch.core.snapshot;

import io.github.lamspace.openlatch.core.LockType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 快照状态重建的输入值对象：复制状态全集的
 * core 原生形态——锁条目（含持有者计数与租约三元组）、原子变量条目
 * （含值、版本戳与去重槽）与会话登记集合。
 *
 * <p><b>定位</b>：本类型是 {@code CoreEngine.restoreFrom} 的唯一合法输入，
 * 刻意使用 core 原生类型（不引用 proto/序列化/网络类型），维持 core
 * "纯 Java、零外部依赖"的模块隔离；
 * 外部存储格式（如 Raft 快照的 {@code SnapshotState}）到本类型的翻译由
 * 调用方完成。
 *
 * <p><b>不可变性</b>：构造时逐项校验并做防御性复制，构造后条目列表、会话
 * 列表与各条目持有者列表均不可变；多线程间传递无需额外同步（深不可变）。
 *
 * <p><b>语义约定</b>：条目按声明序表达"首次授予的先后序"（跨副本比对要求
 * 顺序保持）；{@code sessions} 为登记时刻的逻辑集合——每个持有者的
 * {@code sessionId} MUST 同时出现在会话集合中（构造校验）。会话 id 与线程 id
 * 的取值域由调用方决定（集群路径为引擎内部 sid），本类型不解释其含义。
 *
 * @param entries         锁条目列表（声明序 = 首次授予序）
 * @param sessions        已登记会话 id 列表（含无持有者的空会话）
 * @param nextLeaseToken  发号水位：重建后引擎的下一租约凭证（MUST 大于全部
 *                        继承条目的凭证；保证跨副本对同一尾部日志发出相同凭证）
 */
public record CoreStateRestore(List<Entry> entries, List<Long> sessions, long nextLeaseToken) {

    /**
     * 单个持有权属的重入计数（数据载体）。
     *
     * @param sessionId 持有会话 id
     * @param threadId  持有线程 id
     * @param count     重入持有计数（{@code >= 1}）
     */
    public record Holder(long sessionId, long threadId, int count) {

        /**
         * 构造并校验持有计数。
         *
         * @throws IllegalArgumentException {@code count < 1}
         */
        public Holder {
            if (count < 1) {
                throw new IllegalArgumentException("holder count must be >= 1: " + count);
            }
        }
    }

    /**
     * 单 key 的复制态条目（数据载体）：模式、租约三元组与持有者列表。
     *
     * @param key         锁键（非空）
     * @param lockType    锁类型（决定写侧/读侧重建与可重入性）
     * @param leaseToken  当前租约凭证（{@code >= 1}）
     * @param leaseMs     实际生效租期（{@code >= 1}）
     * @param expiresAtMs 当前到期时刻（{@code >= 1}）
     * @param holders     持有者列表（锁/Semaphore 非空；Latch/ATOMIC 恒空。
     *                    Semaphore 条目的 {@code count} 语义为持有许可数）
     * @param permitsTotal Semaphore 条目的许可总量（非 Semaphore 恒 0）
     * @param latchTotal  Latch 条目的定型初始计数（非 Latch 恒 0）
     * @param latchCount  Latch 条目的当前剩余计数（非 Latch 恒 0）
     * @param atomic      ATOMIC 条目的状态组（形态合法值、版本戳与去重槽；
     *                    非 ATOMIC 恒 {@code null}）
     * @param barrier     BARRIER 条目的状态组（parties/世代/到场账簿/挂账/
     *                    了结记录；非 BARRIER 恒 {@code null}）
     */
    public record Entry(String key, LockType lockType, long leaseToken, long leaseMs,
                        long expiresAtMs, List<Holder> holders,
                        int permitsTotal, long latchTotal, long latchCount,
                        AtomicState atomic, BarrierState barrier) {

        /**
         * 锁家族便捷构造：许可与屏障字段取缺省 0，原子状态组为 {@code null}。
         *
         * @param key         锁键
         * @param lockType    锁类型
         * @param leaseToken  当前租约凭证
         * @param leaseMs     实际生效租期
         * @param expiresAtMs 当前到期时刻
         * @param holders     持有者列表
         */
        public Entry(String key, LockType lockType, long leaseToken, long leaseMs,
                long expiresAtMs, List<Holder> holders) {
            this(key, lockType, leaseToken, leaseMs, expiresAtMs, holders, 0, 0, 0, null, null);
        }

        /**
         * 无原子状态组的九参构造（Latch/Semaphore/锁通道的既有调用形态）。
         *
         * @param key          锁键
         * @param lockType     锁类型
         * @param leaseToken   当前租约凭证
         * @param leaseMs      实际生效租期
         * @param expiresAtMs  当前到期时刻
         * @param holders      持有者列表
         * @param permitsTotal Semaphore 许可总量
         * @param latchTotal   Latch 定型初始计数
         * @param latchCount   Latch 当前剩余计数
         */
        public Entry(String key, LockType lockType, long leaseToken, long leaseMs,
                long expiresAtMs, List<Holder> holders, int permitsTotal,
                long latchTotal, long latchCount) {
            this(key, lockType, leaseToken, leaseMs, expiresAtMs, holders,
                    permitsTotal, latchTotal, latchCount, null, null);
        }

        /**
         * 构造并校验条目形态自洽性（按家族分支）：锁条目在租约三元组非正、
         * 持有者列表为空、写类条目多持有者、{@code SIMPLE} 多层持有时均拒绝；Semaphore 条目额外要求总量不小于持有和且持有者
         * 非空；Latch 条目无租约与持有者（三元组与 holders 允许 0/空），
         * 计数须在 {@code [0, total]} 内且 {@code total >= 1}；ATOMIC 条目
         * 无租约与持有者、MUST 携带原子状态组且初值/当前值/槽应答属形态
         * 值域（integer 截断域、boolean 限 {0,1}）；BARRIER 条目无租约与
         * 持有者、MUST 携带屏障状态组且 parties/generation 为正、了结编码
         * 在值域内。
         *
         * @throws IllegalArgumentException 家族自洽性违例
         */
        public Entry {
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("entry key must be non-empty");
            }
            if (lockType == null) {
                throw new IllegalArgumentException("entry lockType must be non-null");
            }
            holders = List.copyOf(holders);
            if (lockType == LockType.BARRIER) {
                if (barrier == null) {
                    throw new IllegalArgumentException(
                            "barrier entry requires state group: key=" + key);
                }
                if (!holders.isEmpty() || leaseToken != 0 || leaseMs != 0 || expiresAtMs != 0) {
                    throw new IllegalArgumentException(
                            "barrier entry carries lease or holders: key=" + key);
                }
            } else if (barrier != null) {
                throw new IllegalArgumentException(
                        "non-barrier entry carries barrier state: key=" + key);
            }
            boolean atomicKind = lockType == LockType.ATOMIC_LONG
                    || lockType == LockType.ATOMIC_INTEGER
                    || lockType == LockType.ATOMIC_BOOLEAN;
            if (atomicKind) {
                if (atomic == null) {
                    throw new IllegalArgumentException("atomic entry requires state group: key=" + key);
                }
                if (!holders.isEmpty() || leaseToken != 0 || leaseMs != 0 || expiresAtMs != 0) {
                    throw new IllegalArgumentException(
                            "atomic entry must have no holders or lease: key=" + key);
                }
                if (permitsTotal != 0 || latchTotal != 0 || latchCount != 0) {
                    throw new IllegalArgumentException(
                            "atomic entry must not carry other-family counters: key=" + key);
                }
                if (!inKindDomain(lockType, atomic.initial()) || !inKindDomain(lockType, atomic.value())
                        || !inKindDomain(lockType, atomic.slotValue())) {
                    throw new IllegalArgumentException("atomic value out of kind domain: key=" + key);
                }
            } else if (lockType == LockType.LATCH) {
                if (latchTotal < 1 || latchCount < 0 || latchCount > latchTotal) {
                    throw new IllegalArgumentException(
                            "bad latch counters: key=" + key + " total=" + latchTotal
                                    + " count=" + latchCount);
                }
                if (!holders.isEmpty()) {
                    throw new IllegalArgumentException("latch entry must have no holders: key=" + key);
                }
            } else if (lockType == LockType.BARRIER) {
                // 屏障自洽性已在家族首检完成；此处只拦他族字段携带。
                if (atomic != null || permitsTotal != 0 || latchTotal != 0 || latchCount != 0) {
                    throw new IllegalArgumentException(
                            "barrier entry must not carry other-family state: key=" + key);
                }
            } else {
                if (atomic != null) {
                    throw new IllegalArgumentException(
                            "non-atomic entry must not carry atomic state: key=" + key);
                }
                if (leaseToken < 1 || leaseMs < 1 || expiresAtMs < 1) {
                    throw new IllegalArgumentException(
                            "lease triple must be positive: key=" + key);
                }
                if (holders.isEmpty()) {
                    throw new IllegalArgumentException("entry must have holders: key=" + key);
                }
                if (lockType == LockType.SEMAPHORE) {
                    int held = 0;
                    for (Holder h : holders) {
                        held += h.count();
                    }
                    if (permitsTotal < 1 || held > permitsTotal) {
                        throw new IllegalArgumentException(
                                "bad semaphore counters: key=" + key + " total=" + permitsTotal
                                        + " held=" + held);
                    }
                } else {
                    if (permitsTotal != 0 || latchTotal != 0 || latchCount != 0) {
                        throw new IllegalArgumentException(
                                "lock entry must not carry family counters: key=" + key);
                    }
                }
            }
            if (lockType != LockType.READ && lockType != LockType.LATCH
                    && lockType != LockType.SEMAPHORE && lockType != LockType.BARRIER
                    && !atomicKind && holders.size() != 1) {
                throw new IllegalArgumentException(
                        "write-side entry must have exactly one holder: key=" + key);
            }
            if (lockType == LockType.SIMPLE && holders.get(0).count() != 1) {
                throw new IllegalArgumentException(
                        "SIMPLE entry cannot be reentrant-held: key=" + key);
            }
        }
    }

    /**
     * ATOMIC 条目的快照状态组（数据载体）：定型初值、当前值、版本戳与
     * 去重槽（最近被处理写操作的会话/序号与应答四元组；空槽以
     * {@code slotSession = 0} 表达）。值与槽应答的形态值域不变量由
     * {@link Entry} 依条目 {@code lockType} 校验。
     *
     * @param initial      定型初值（无主张创建为 0）
     * @param value        当前值
     * @param version      版本戳（{@code >= 0}）
     * @param slotSession  去重槽会话（0=空槽）
     * @param slotOpSeq    去重槽序号（空槽恒 0；非空槽 {@code >= 1}）
     * @param slotApplied  槽应答 applied
     * @param slotOldValue 槽应答 oldValue
     * @param slotValue    槽应答 value
     * @param slotVersion  槽应答 version
     */
    public record AtomicState(long initial, long value, long version, long slotSession,
                              long slotOpSeq, boolean slotApplied, long slotOldValue,
                              long slotValue, long slotVersion) {

        /**
         * 构造并校验版本戳与槽形态自洽性。
         *
         * @throws IllegalArgumentException 版本为负、空/非空槽与序号矛盾
         */
        public AtomicState {
            if (version < 0) {
                throw new IllegalArgumentException("atomic version must be >= 0: " + version);
            }
            if (slotSession == 0 && slotOpSeq != 0) {
                throw new IllegalArgumentException("empty dedup slot must carry seq 0");
            }
            if (slotSession != 0 && slotOpSeq < 1) {
                throw new IllegalArgumentException("occupied dedup slot must carry seq >= 1");
            }
        }
    }

    /**
     * 循环屏障条目的复制态状态组（快照/重建直写通道）：
     * parties 定型、当前世代号与到场账簿、动作挂账 (会话, 请求)、
     * 最近完结世代的了结记录（形态 0=无、1=TRIPPED、2=BROKEN）。
     *
     * @param parties             定型许可数（{@code >= 1}）
     * @param generation          当前世代号（{@code >= 1}）
     * @param arrivals            当前世代到场账簿（引擎内部会话 id，插入序）
     * @param actionSession       动作挂账会话（0=无挂账）
     * @param actionRequest       动作挂账请求 id
     * @param completedGeneration 最近完结世代号（0=无完结记录）
     * @param completedResult     完结形态编码（0=无、1=TRIPPED、2=BROKEN）
     * @param completedArrivals   完结世代到场账簿（插入序；已消亡会话的
     *                            条目由装配侧剔除，见重建工厂注释）
     * @param completedExecutor   完结世代执行者会话（0=无）
     */
    public record BarrierState(long parties, long generation,
                               java.util.List<io.github.lamspace.openlatch.core.lock.BarrierEntry.Arrival> arrivals,
                               long actionSession, long actionRequest,
                               long completedGeneration, int completedResult,
                               java.util.List<io.github.lamspace.openlatch.core.lock.BarrierEntry.Arrival> completedArrivals,
                               long completedExecutor) {

        /**
         * 构造并校验世代自洽性：parties/generation 正、了结编码值域、
         * 无完结记录时账簿必空。
         *
         * @throws IllegalArgumentException 自洽性违例
         */
        public BarrierState {
            if (parties < 1) {
                throw new IllegalArgumentException("barrier parties must be >= 1: " + parties);
            }
            if (generation < 1) {
                throw new IllegalArgumentException("barrier generation must be >= 1: " + generation);
            }
            if (completedResult < 0 || completedResult > 2) {
                throw new IllegalArgumentException("unknown completed result code: " + completedResult);
            }
            arrivals = java.util.List.copyOf(arrivals);
            completedArrivals = java.util.List.copyOf(completedArrivals);
            if (completedGeneration == 0 && !completedArrivals.isEmpty()) {
                throw new IllegalArgumentException("completed arrivals without completed generation");
            }
        }
    }

    /**
     * 值是否属指定原子形态的值域：long 恒真；integer 限 int32；boolean 限 {0,1}。
     *
     * @param kind 原子形态判别
     * @param v    待检值
     * @return 属域为 {@code true}
     */
    private static boolean inKindDomain(LockType kind, long v) {
        return switch (kind) {
            case ATOMIC_LONG -> true;
            case ATOMIC_INTEGER -> (int) v == v;
            default -> v == 0 || v == 1;
        };
    }

    /**
     * 构造重建输入：防御性复制并做整体一致性校验。
     *
     * @throws IllegalArgumentException 条目 key 重复、持有者引用未登记会话、
     *         会话列表含 {@code null}、水位不大于任何继承凭证
     */
    public CoreStateRestore {
        entries = List.copyOf(entries);
        sessions = List.copyOf(sessions);
        Set<Long> sessionSet = new HashSet<>();
        for (Long sid : sessions) {
            if (sid == null || !sessionSet.add(sid)) {
                throw new IllegalArgumentException("bad session list element: " + sid);
            }
        }
        Set<String> keys = new HashSet<>();
        for (Entry e : entries) {
            if (!keys.add(e.key())) {
                throw new IllegalArgumentException("duplicate entry key: " + e.key());
            }
        }
        for (Entry e : entries) {
            for (Holder h : e.holders()) {
                if (!sessionSet.contains(h.sessionId())) {
                    throw new IllegalArgumentException(
                            "holder session not registered: " + h.sessionId());
                }
            }
            if (nextLeaseToken <= e.leaseToken()) {
                throw new IllegalArgumentException(
                        "next_lease_token must exceed all entry tokens: key=" + e.key());
            }
        }
        if (nextLeaseToken < 1) {
            throw new IllegalArgumentException("next_lease_token must be >= 1: " + nextLeaseToken);
        }
    }

    /**
     * 快照内最大租约凭证（发号器跳界的判定输入）。
     *
     * @return 全部条目凭证的最大值；空快照为 {@code 0}
     */
    public long maxLeaseToken() {
        long max = 0;
        for (Entry e : entries) {
            max = Math.max(max, e.leaseToken());
        }
        return max;
    }

    /**
     * 空状态（空快照恢复的合法输入：无锁条目、无会话、发号器从零起点）。
     *
     * @return 空的重建输入
     */
    public static CoreStateRestore empty() {
        return new CoreStateRestore(List.of(), List.of(), 1L);
    }
}
