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
 * 快照状态重建的输入值对象（详设 §7.1，S4/design D1）：复制状态全集的
 * core 原生形态——锁条目（含持有者计数与租约三元组）与会话登记集合。
 *
 * <p><b>定位</b>：本类型是 {@code CoreEngine.restoreFrom} 的唯一合法输入，
 * 刻意使用 core 原生类型（不引用 proto/序列化/网络类型），维持 core
 * "纯 Java、零外部依赖"的模块隔离（core-lock-engine spec"无网络与零依赖"）；
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
     * @param holders     持有者列表（非空；{@code READ} 允许多持有者）
     */
    public record Entry(String key, LockType lockType, long leaseToken, long leaseMs,
                        long expiresAtMs, List<Holder> holders) {

        /**
         * 构造并校验条目形态自洽性。
         *
         * @throws IllegalArgumentException 租约字段非正、持有者列表为空、
         *         写类条目持有者多于一个、{@code SIMPLE} 条目计数不为 1
         *         （非可重入类型不可能有多层持有）
         */
        public Entry {
            if (key == null || key.isEmpty()) {
                throw new IllegalArgumentException("entry key must be non-empty");
            }
            if (lockType == null) {
                throw new IllegalArgumentException("entry lockType must be non-null");
            }
            if (leaseToken < 1 || leaseMs < 1 || expiresAtMs < 1) {
                throw new IllegalArgumentException(
                        "lease triple must be positive: key=" + key);
            }
            holders = List.copyOf(holders);
            if (holders.isEmpty()) {
                throw new IllegalArgumentException("entry must have holders: key=" + key);
            }
            if (lockType != LockType.READ && holders.size() != 1) {
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
