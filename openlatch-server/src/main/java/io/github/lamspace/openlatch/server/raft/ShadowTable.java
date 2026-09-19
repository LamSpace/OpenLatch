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

package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.raft.SnapshotBarrierArrival;
import io.github.lamspace.openlatch.protocol.raft.SnapshotHolder;
import io.github.lamspace.openlatch.protocol.raft.SnapshotLock;
import io.github.lamspace.openlatch.protocol.raft.SnapshotState;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 影子状态表（复制边界的逻辑镜像）：状态机应用路径同步维护的
 * "已复制锁状态"视图，以逻辑会话 id 为归属标识（各副本引擎的内部 sid 不外露）。
 *
 * <p><b>职责</b>：①跨副本一致性摘要的载体（{@link #digest()}，一致性断言与
 * 快照比对共用）；②快照序列化结构（{@link #toProto()}/{@link #load}，
 * 内容 = 锁条目 + 会话注册表，<b>不含</b>等待队列与本地配置）；
 * ③Leader 侧预检查与到期扫描的无锁读索引（{@link #isHeld}/{@link #isHeldBy}/{@link #heldEntries()}，
 * 供 {@code ReplicationGateway} 与到期驱动消费）；④管理观察的明细投影
 * （{@link #adminEntry}/{@link #adminEntries()}，逐应用点整体
 * 重发布、逻辑会话口径、LATCH 亦在列，供 {@code ADMIN_*} 消息跨线程弱一致读）。
 *
 * <p><b>与引擎的双写核算</b>：每次条目应用同时驱动
 * {@link io.github.lamspace.openlatch.core.CoreEngine} 与本表，
 * 二者由同一串行应用线程按同一日志序更新（digest 与引擎实际状态偏离即
 * 意味着复制语义或时间语义出 bug，见随机序列属性测试）。
 *
 * <p><b>线程模型</b>：结构性写操作（本类全部 {@code public} 变更方法）只在
 * 状态机应用线程（{@link LockStateMachineCore#applyEntry} 的 apply 锁内）调用；
 * 读侧 digest/toProto 同线程。无锁索引 {@code heldIndex} 是结构变更的
 * 最终一致投影（{@link ConcurrentHashMap}），跨线程读仅用于预检查/扫描这类
 * "结果可旧不可错"的路径：读到过期状态最多导致一次多余的日志提交或一个
 * 扫描周期的延后，正确性裁决恒在应用路径。
 *
 * <p><b>顺序契约</b>：锁条目按 key 首次授予的插入序保持（{@link LinkedHashMap}），
 * 会话按登记序保持（{@link LinkedHashSet}），使 {@link #digest()} 跨副本可比；
 * 该顺序由日志全序保证，任何改变插入时机即改变 digest（视为缺陷而非兼容性）。
 */
public final class ShadowTable {

    /**
     * 持有条目身份：逻辑会话 + 线程（归属粒度，对齐 core 的 (sessionId, threadId)）。
     *
     * @param sessionId 逻辑会话 id
     * @param threadId  持有线程 id
     */
    public record Holder(long sessionId, long threadId) { }

    /**
     * 无锁索引的投影记录：当前租约凭证、到期时刻、持有者集快照与条目的
     * 协议锁类型数值（重入预检消费；holders 快照仅在授予/装载时刷新，
     * 摘除可短暂滞后，判定容错语义见 {@link #isHeldBy}；{@code lockType}
     * 随条目定型不变，按家族聚合的指标读数来源）。
     *
     * @param leaseToken  当前租约凭证
     * @param expiresAtMs 到期时刻（毫秒时间戳）
     * @param holders     持有者集快照（授予时点的不可变拷贝）
     * @param lockType    协议 {@code LockType} 数值（条目定型值）
     */
    public record HeldRef(long leaseToken, long expiresAtMs, Set<Holder> holders, int lockType) { }

    /**
     * 循环屏障到场身份的逻辑会话投影（(逻辑会话, 请求) 二元组）。
     * 引擎内部 sid 不出节点，镜像与 digest 一律以逻辑会话 id 表达。
     *
     * @param sessionId 逻辑会话 id
     * @param requestId 到场请求 id
     */
    public record ArrivalRef(long sessionId, long requestId) { }

    /**
     * 循环屏障复制态镜像输入（应用点自引擎导出的不可变快照）。
     *
     * @param parties             定型许可数
     * @param generation          当前世代号
     * @param arrivals            当前世代到场账簿（逻辑 id，插入序）
     * @param actionSession       动作挂账逻辑会话（0=无）
     * @param actionRequest       动作挂账请求 id
     * @param completedGeneration 最近完结世代号（0=无）
     * @param completedResult     完结世代了结形态：0=无、1=TRIPPED、2=BROKEN
     * @param completedArrivals   完结世代到场账簿（逻辑 id，插入序）
     * @param completedExecutor   完结世代执行者逻辑会话（0=无）
     */
    public record BarrierMirrorData(long parties, long generation, List<ArrivalRef> arrivals,
                                    long actionSession, long actionRequest,
                                    long completedGeneration, int completedResult,
                                    List<ArrivalRef> completedArrivals, long completedExecutor) { }

    /**
     * 管理观察的条目全量投影：
     * 应用线程在每次结构变更后自 {@link SLock} 同步发布的不可变明细视图，
     * 以逻辑会话 id 为归属标识（跨节点可对齐；引擎内部 sid 不外露）。
     * 与 {@link HeldRef} 的"授予时点 holders 快照"不同，本视图逐应用点整体
     * 重发布，跨线程读取允许落后一至数个应用（弱一致镜像口径）。
     * LATCH 条目同样入本投影（{@link #heldIndex} 因其无持有语义而排除）。
     *
     * @param lockType         协议 {@code LockType} 数值（条目定型值）
     * @param leaseToken       当前租约凭证（Latch 恒 0）
     * @param expiresAtMs      到期时刻（Latch/无持有为 0）
     * @param leaseMs          实际生效租期（Latch 为 0）
     * @param holders          归属 → 持有计数（重入层数/持有许可数；Latch 恒空）
     * @param permitsTotal     Semaphore 许可总量（其余家族 0）
     * @param permitsAvailable Semaphore 当前可用许可（其余家族 0）
     * @param latchTotal       Latch 定型初始计数（其余家族 0）
     * @param latchCount       Latch 当前剩余计数（其余家族 0）
     * @param latchParticipants Latch 参与逻辑会话集（其余家族空）
     * @param atomicInitial     ATOMIC 定型初值（其余家族 0）
     * @param atomicValue       ATOMIC 当前值（其余家族 0）
     * @param atomicVersion     ATOMIC 版本戳（其余家族 0）
     * @param barrierParties    BARRIER 定型许可数（其余家族 0）
     * @param barrierGeneration BARRIER 当前世代号（其余家族 0）
     * @param barrierArrived    BARRIER 当前世代到场数（其余家族 0）
     * @param barrierActionPending BARRIER 是否动作待决（其余家族 false）
     * @param barrierCompletedResult BARRIER 最近完结世代的了结形态
     *                               （0=无、1=TRIPPED、2=BROKEN；其余家族 0）
     */
    public record AdminEntryView(int lockType, long leaseToken, long expiresAtMs, long leaseMs,
                                 Map<Holder, Integer> holders, int permitsTotal, int permitsAvailable,
                                 long latchTotal, long latchCount, Set<Long> latchParticipants,
                                 long atomicInitial, long atomicValue, long atomicVersion,
                                 long barrierParties, long barrierGeneration, int barrierArrived,
                                 boolean barrierActionPending, int barrierCompletedResult) { }

    /** 单 key 的复制态：模式、凭证、到期、租期与持有者计数（插入序=首次持有序）。 */
    private static final class SLock {
        /** 协议锁类型数值（{@code LockType} 枚举序）。 */
        private final int lockType;
        /** 当前租约凭证。 */
        private long leaseToken;
        /** 到期时刻（毫秒时间戳）。 */
        private long expiresAtMs;
        /** 实际生效租期（毫秒）。 */
        private long leaseMs;
        /** 持有者 → 计数（可重入逐层），插入序=首次持有序。 */
        private final LinkedHashMap<Holder, Integer> holders = new LinkedHashMap<>();
        /** SEMAPHORE 条目：许可总量（其余家族 0）。 */
        private int permitsTotal;
        /** SEMAPHORE 条目：当前可用许可数（其余家族 0）。 */
        private int permitsAvailable;
        /** LATCH 条目：定型初始计数（其余家族 0）。 */
        private long latchTotal;
        /** LATCH 条目：当前剩余计数（其余家族 0）。 */
        private long latchCount;
        /** LATCH 条目：参与会话集（逻辑 sid，插入序；其余家族空）。 */
        private final LinkedHashSet<Long> latchParticipants = new LinkedHashSet<>();
        /** ATOMIC 条目：定型初值（其余家族 0）。 */
        private long atomicInitial;
        /** ATOMIC 条目：当前值（其余家族 0）。 */
        private long atomicValue;
        /** ATOMIC 条目：版本戳（其余家族 0）。 */
        private long atomicVersion;
        /** ATOMIC 条目：去重槽会话（逻辑 sid，0=空槽；其余家族 0）。 */
        private long atomicSlotSession;
        /** ATOMIC 条目：去重槽 op_seq（空槽恒 0；其余家族 0）。 */
        private long atomicSlotOpSeq;
        /** ATOMIC 条目：槽应答 applied（其余家族 false）。 */
        private boolean atomicSlotApplied;
        /** ATOMIC 条目：槽应答 oldValue（其余家族 0）。 */
        private long atomicSlotOldValue;
        /** ATOMIC 条目：槽应答 value（其余家族 0）。 */
        private long atomicSlotValue;
        /** ATOMIC 条目：槽应答 version（其余家族 0）。 */
        private long atomicSlotVersion;
        /** BARRIER 定型许可数（其余家族 0）。 */
        private long barrierParties;
        /** BARRIER 当前世代号（其余家族 0）。 */
        private long barrierGeneration;
        /** BARRIER 当前世代到场账簿（逻辑 id，插入序；其余家族空）。 */
        private List<ArrivalRef> barrierArrivals = List.of();
        /** BARRIER 动作挂账逻辑会话（0=无）。 */
        private long barrierActionSession;
        /** BARRIER 动作挂账请求 id。 */
        private long barrierActionRequest;
        /** BARRIER 最近完结世代号（0=无）。 */
        private long barrierCompletedGeneration;
        /** BARRIER 完结形态：0=无、1=TRIPPED、2=BROKEN。 */
        private int barrierCompletedResult;
        /** BARRIER 完结世代到场账簿（逻辑 id，插入序）。 */
        private List<ArrivalRef> barrierCompletedArrivals = List.of();
        /** BARRIER 完结世代执行者逻辑会话（0=无）。 */
        private long barrierCompletedExecutor;

        /**
         * 构造复制态条目。
         *
         * @param lockType    协议锁类型数值
         * @param leaseToken  当前租约凭证
         * @param expiresAtMs 到期时刻
         * @param leaseMs     实际生效租期
         */
        private SLock(int lockType, long leaseToken, long expiresAtMs, long leaseMs) {
            this.lockType = lockType;
            this.leaseToken = leaseToken;
            this.expiresAtMs = expiresAtMs;
            this.leaseMs = leaseMs;
        }
    }

    /**
     * 构造空影子表（锁表与会话集为空，digest 即"全新内核"基准摘要）。
     */
    public ShadowTable() {
    }

    /** key → 复制态条目，仅在应用线程访问（见类级线程模型）。 */
    private final LinkedHashMap<String, SLock> locks = new LinkedHashMap<>();
    /** 已登记逻辑会话（digest 用插入序），仅在应用线程访问。 */
    private final LinkedHashSet<Long> sessions = new LinkedHashSet<>();
    /** 无锁投影：key → (token, expiresAt)，供 Leader 预检查与到期扫描跨线程读。 */
    private final ConcurrentHashMap<String, HeldRef> heldIndex = new ConcurrentHashMap<>();
    /** 会话无锁投影（预检查/失联批量清理的跨线程读）。 */
    private final java.util.Set<Long> sessionIndex = java.util.concurrent.ConcurrentHashMap.newKeySet();
    /**
     * 管理观察明细投影：key → 不可变全字段视图，应用线程在
     * 每个结构变更点后整体重发布（发布点与本表 {@link #locks} 的变更同一
     * 应用时刻，读侧弱一致"可旧不可错"）；仅 ADMIN 消费，不参与 digest。
     */
    private final ConcurrentHashMap<String, AdminEntryView> adminView = new ConcurrentHashMap<>();

    /**
     * 登记逻辑会话（SESSION_OPEN 应用点）。幂等：重复登记为无操作，
     * 与引擎侧 {@code sidMap} 的判存联合保证回放幂等。
     *
     * @param sessionId 逻辑会话 id
     */
    public void addSession(long sessionId) {
        sessions.add(sessionId);
        sessionIndex.add(sessionId);
    }

    /**
     * 摘除逻辑会话（SESSION_CLOSE 应用点，不处理其持锁——持锁清理由
     * {@link #dropSessionHolders} 单独驱动，保持与引擎动作的一一对应）。
     *
     * @param sessionId 逻辑会话 id
     */
    public void removeSession(long sessionId) {
        sessions.remove(sessionId);
        sessionIndex.remove(sessionId);
    }

    /**
     * 逻辑会话是否已登记。
     *
     * @param sessionId 逻辑会话 id
     * @return 已登记为 {@code true}
     */
    public boolean hasSession(long sessionId) {
        return sessionIndex.contains(sessionId);
    }

    /**
     * 授予登记（GRANTED 应用点）：条目不存在则创建；存在则按重入语义刷新
     * 凭证/到期/租期（引擎重入不换凭证且整段刷新租约，本表镜像同一规则），
     * 并对 {@code (sid, threadId)} 持有计数 +1。
     *
     * @param sessionId  逻辑会话 id
     * @param threadId   持有线程 id
     * @param key        锁键
     * @param lockType   协议锁类型数值（{@code LockType} 枚举序）
     * @param token      当前租约凭证
     * @param leaseMs    实际生效租期
     * @param expiresAt  到期时刻（条目时刻 + 租期）
     */
    public void grant(long sessionId, long threadId, String key, int lockType,
                      long token, long leaseMs, long expiresAt) {
        grantDelta(sessionId, threadId, key, lockType, token, leaseMs, expiresAt, 1, 0);
    }

    /**
     * 授予登记（计数增量形态）：Semaphore 一次授予归还多许可，
     * 影子表按引擎实际持有增量镜像（{@code holderDelta = permits}），锁家族
     * 恒 1——保持与引擎 {@code holders} 计数严格对称，digest 与释放回退
     * 不因许可语义漂移。
     *
     * @param sessionId   逻辑会话 id
     * @param threadId    持有线程 id
     * @param key         锁键
     * @param lockType    协议锁类型数值
     * @param token       当前租约凭证
     * @param leaseMs     实际生效租期
     * @param expiresAt   到期时刻
     * @param holderDelta  本次授予新增持有计数（{@code >= 1}；Semaphore 为许可数）
     * @param permitsTotal Semaphore 建条目的许可总量（其余家族 0）
     */
    public void grantDelta(long sessionId, long threadId, String key, int lockType,
                           long token, long leaseMs, long expiresAt, int holderDelta,
                           int permitsTotal) {
        SLock l = locks.get(key);
        if (l == null) {
            l = new SLock(lockType, token, expiresAt, leaseMs);
            l.permitsTotal = permitsTotal;
            l.permitsAvailable = permitsTotal;
            locks.put(key, l);
        } else {
            l.leaseToken = token;
            l.expiresAtMs = expiresAt;
            l.leaseMs = leaseMs;
        }
        if (lockType == LockType.LOCK_TYPE_SEMAPHORE_VALUE) {
            l.permitsAvailable -= holderDelta;
        }
        l.holders.merge(new Holder(sessionId, threadId), holderDelta, Integer::sum);
        heldIndex.put(key, new HeldRef(token, expiresAt, Set.copyOf(l.holders.keySet()), lockType));
        adminView.put(key, viewOf(l));
    }

    /**
     * 自应用线程独占的 {@link SLock} 整体拷贝出不可变管理视图
     * （仅在 {@link #locks} 变更点后调用，读侧经 {@code adminView} 并发容器）。
     *
     * @param l 变更后的条目
     * @return 管理观察视图
     */
    private static AdminEntryView viewOf(SLock l) {
        return new AdminEntryView(l.lockType, l.leaseToken, l.expiresAtMs, l.leaseMs,
                Map.copyOf(l.holders), l.permitsTotal, l.permitsAvailable,
                l.latchTotal, l.latchCount, Set.copyOf(l.latchParticipants),
                l.atomicInitial, l.atomicValue, l.atomicVersion,
                l.barrierParties, l.barrierGeneration, l.barrierArrivals.size(),
                l.barrierActionSession != 0, l.barrierCompletedResult);
    }

    /**
     * 释放登记（RELEASE OK 应用点）：对应持有计数 -1，归零摘除持有者；
     * 条目无持有者时移除 key（与引擎的条目生命周期同步）。
     *
     * @param sessionId 逻辑会话 id
     * @param threadId  持有线程 id
     * @param key       锁键
     */
    public void release(long sessionId, long threadId, String key) {
        SLock l = locks.get(key);
        if (l == null) {
            return;
        }
        Holder h = new Holder(sessionId, threadId);
        Integer count = l.holders.get(h);
        if (count != null) {
            if (count <= 1) {
                l.holders.remove(h);
            } else {
                l.holders.put(h, count - 1);
            }
        }
        if (l.holders.isEmpty()) {
            locks.remove(key);
            heldIndex.remove(key);
            adminView.remove(key);
        } else {
            adminView.put(key, viewOf(l));
        }
    }

    /**
     * 释放登记（计数增量形态）：归还 {@code releaseDelta} 个
     * 持有计数，归零摘除归属；条目无持有者时移除 key。
     *
     * @param sessionId     逻辑会话 id
     * @param threadId      持有线程 id
     * @param key           锁键
     * @param releaseDelta  引擎实际归还计数（{@code >= 0}，0 为无操作）
     */
    public void release(long sessionId, long threadId, String key, int releaseDelta) {
        if (releaseDelta <= 0) {
            return;
        }
        SLock l = locks.get(key);
        if (l == null) {
            return;
        }
        Holder h = new Holder(sessionId, threadId);
        Integer count = l.holders.get(h);
        if (count != null) {
            if (l.lockType == LockType.LOCK_TYPE_SEMAPHORE_VALUE) {
                l.permitsAvailable += releaseDelta;
            }
            if (count <= releaseDelta) {
                l.holders.remove(h);
            } else {
                l.holders.put(h, count - releaseDelta);
            }
        }
        if (l.holders.isEmpty()) {
            locks.remove(key);
            heldIndex.remove(key);
            adminView.remove(key);
        } else {
            adminView.put(key, viewOf(l));
        }
    }

    /**
     * 续租登记（RENEW OK 应用点）：刷新到期时刻（凭证不变），同步无锁索引。
     *
     * @param key          锁键
     * @param newExpiresAt 新到期时刻（条目时刻 + 续租租期）
     * @param newLeaseMs   实际生效租期
     */
    public void renew(String key, long newExpiresAt, long newLeaseMs) {
        SLock l = locks.get(key);
        if (l == null) {
            return;
        }
        l.expiresAtMs = newExpiresAt;
        l.leaseMs = newLeaseMs;
        heldIndex.computeIfPresent(key,
                (k, ref) -> new HeldRef(ref.leaseToken(), newExpiresAt, ref.holders(), ref.lockType()));
        adminView.put(key, viewOf(l));
    }

    /**
     * 到期清扫（LEASE_EXPIRE_ENTRY 应用点）：移除全部到期时刻不晚于
     * {@code entryTimeMs} 的条目——与引擎 {@code expireDue()} 在条目时刻下的
     * 释放集合一致（两侧的状态迁移事件流相同），无需逐条目 token 判定。
     *
     * @param entryTimeMs 条目携带时刻（"以该时刻为现在"求到期集）
     * @return 被移除的 key 列表（插入序；供 Leader 侧唤醒队首消费）
     */
    public List<String> expireUpTo(long entryTimeMs) {
        List<String> freed = new ArrayList<>();
        for (Map.Entry<String, SLock> en : locks.entrySet()) {
            if (en.getValue().lockType == LockType.LOCK_TYPE_LATCH_VALUE
                    || en.getValue().lockType == LockType.LOCK_TYPE_BARRIER_VALUE
                    || isAtomicType(en.getValue().lockType)) {
                // 无租约家族（Latch/ATOMIC/BARRIER）到期时刻恒 0——非"已到期"
                // 信号，永不由到期清扫回收（一次性护栏、常驻值与循环屏障世代
                // 存续语义各自承载；判例：原子变更对影子表无租约家族到期误扫的修复）。
                continue;
            }
            if (en.getValue().expiresAtMs <= entryTimeMs) {
                freed.add(en.getKey());
            }
        }
        for (String key : freed) {
            locks.remove(key);
            heldIndex.remove(key);
            adminView.remove(key);
        }
        return freed;
    }

    /**
     * 会话关闭清理（SESSION_CLOSE 应用点）：摘除该逻辑会话在全部条目上的
     * 持有（对齐引擎 {@code sessionClosed} 的"释放该会话全部持锁"语义），
     * 空条目随移除。
     *
     * @param sessionId 逻辑会话 id
     * @return 因该会话而完全空出的 key 列表（插入序；供 Leader 侧唤醒队首消费）
     */
    public List<String> dropSessionHolders(long sessionId) {
        List<String> removedKeys = new ArrayList<>();
        for (Map.Entry<String, SLock> en : locks.entrySet()) {
            SLock l = en.getValue();
            if (l.lockType == LockType.LOCK_TYPE_LATCH_VALUE) {
                // 屏障条目存续不随参与者散尽而回收（一次性护栏）：
                // 仅摘除参与身份，条目留在表内。
                if (l.latchParticipants.remove(sessionId)) {
                    adminView.put(en.getKey(), viewOf(l));
                }
                continue;
            }
            if (isAtomicType(l.lockType)) {
                // 原子条目存续与一切会话无关：值不绑定归属，
                // 会话关闭不得扰动镜像（含空 holders 的"可回收"误判）。
                continue;
            }
            if (l.lockType == LockType.LOCK_TYPE_BARRIER_VALUE) {
                // 循环屏障条目常驻不回收：会话死亡的世代破障由引擎经
                // SESSION_CLOSE 应用点裁决，破障结果经 barrierMirror
                // 整体刷新——本表不在此处逐会话摘除账簿项（账簿随
                // 世代完结整体归档/清空，非按会话增量维护）。
                continue;
            }
            for (Map.Entry<Holder, Integer> hh : l.holders.entrySet()) {
                if (hh.getKey().sessionId() == sessionId
                        && l.lockType == LockType.LOCK_TYPE_SEMAPHORE_VALUE) {
                    l.permitsAvailable += hh.getValue();
                }
            }
            boolean dropped = l.holders.keySet().removeIf(h -> h.sessionId() == sessionId);
            if (l.holders.isEmpty()) {
                removedKeys.add(en.getKey());
            } else if (dropped) {
                adminView.put(en.getKey(), viewOf(l));
            }
        }
        for (String key : removedKeys) {
            locks.remove(key);
            heldIndex.remove(key);
            adminView.remove(key);
        }
        return removedKeys;
    }

    /**
     * key 当前是否被持有（无锁投影，跨线程安全；预检查快速失败通道专用，
     * 结果允许滞后于 apply，正确性裁决在应用路径）。
     *
     * @param key 锁键
     * @return 持有中为 {@code true}
     */
    public boolean isHeld(String key) {
        return heldIndex.containsKey(key);
    }

    /**
     * 当前持有条目的无锁投影（Leader 到期扫描输入：遍历其中
     * {@code expiresAtMs <= now} 者即为待提交到期条目）。
     *
     * @return key → 投影记录 的弱一致视图（不复制，只读用途）
     */
    public Map<String, HeldRef> heldEntries() {
        return heldIndex;
    }

    /**
     * 指定 key 的当前投影记录（到期回放守卫输入）。
     *
     * @param key 锁键
     * @return 投影记录，未持有为 {@code null}
     */
    public HeldRef heldRef(String key) {
        return heldIndex.get(key);
    }

    /**
     * 指定归属当前是否持有该 key（重入预检通道）：读取
     * {@code heldIndex} 快照的持有者集——快照仅在授予/装载时刷新，
     * 持有者移除后可短暂滞后（"结果可旧不可错"：误判重入也只是多一次
     * 提案，授予与否恒由应用路径引擎裁决；同归属先后授予经
     * 应答-请求程序序 + 并发容器可见性保证不滞后）。
     *
     * @param sessionId 逻辑会话 id
     * @param threadId  持有线程 id
     * @param key       锁键
     * @return 快照显示该归属在持有为 {@code true}
     */
    public boolean isHeldBy(long sessionId, long threadId, String key) {
        HeldRef ref = heldIndex.get(key);
        return ref != null && ref.holders().contains(new Holder(sessionId, threadId));
    }

    /**
     * 当前复制状态的全量 proto 形态（快照与 digest 的统一序列化入口）。
     *
     * @return 按声明序构建的 {@link SnapshotState}
     */
    public SnapshotState toProto() {
        SnapshotState.Builder b = SnapshotState.newBuilder();
        for (Map.Entry<String, SLock> en : locks.entrySet()) {
            SLock l = en.getValue();
            SnapshotLock.Builder lb = SnapshotLock.newBuilder()
                    .setKey(en.getKey())
                    .setLockTypeValue(l.lockType)
                    .setLeaseToken(l.leaseToken)
                    .setExpiresAtMs(l.expiresAtMs)
                    .setLeaseMs(l.leaseMs);
            for (Map.Entry<Holder, Integer> h : l.holders.entrySet()) {
                lb.addHolders(SnapshotHolder.newBuilder()
                        .setSessionId(h.getKey().sessionId())
                        .setThreadId(h.getKey().threadId())
                        .setCount(h.getValue()));
            }
            // v3 家族字段：仅相关家族写入（锁条目保持既有序列化字节形）。
            if (l.lockType == LockType.LOCK_TYPE_SEMAPHORE_VALUE) {
                lb.setPermitsTotal(l.permitsTotal);
            } else if (l.lockType == LockType.LOCK_TYPE_LATCH_VALUE) {
                lb.setLatchTotal(l.latchTotal).setLatchCount(l.latchCount);
            } else if (l.lockType == LockType.LOCK_TYPE_BARRIER_VALUE) {
                // v5 家族字段：仅循环屏障条目写入（列表序即账簿插入序，序列化确定）。
                lb.setBarrierParties(l.barrierParties).setBarrierGeneration(l.barrierGeneration)
                        .setBarrierActionSession(l.barrierActionSession)
                        .setBarrierActionRequest(l.barrierActionRequest)
                        .setBarrierCompletedGeneration(l.barrierCompletedGeneration)
                        .setBarrierCompletedResult(l.barrierCompletedResult)
                        .setBarrierCompletedExecutor(l.barrierCompletedExecutor);
                for (ArrivalRef a : l.barrierArrivals) {
                    lb.addBarrierCurrentArrivals(SnapshotBarrierArrival.newBuilder()
                            .setSessionId(a.sessionId()).setRequestId(a.requestId()));
                }
                for (ArrivalRef a : l.barrierCompletedArrivals) {
                    lb.addBarrierCompletedArrivals(SnapshotBarrierArrival.newBuilder()
                            .setSessionId(a.sessionId()).setRequestId(a.requestId()));
                }
            } else if (isAtomicType(l.lockType)) {
                // v4 家族字段：仅原子条目写入（其余家族序列化字节零扰动）。
                lb.setAtomicInitial(l.atomicInitial).setAtomicValue(l.atomicValue)
                        .setAtomicVersion(l.atomicVersion)
                        .setAtomicSlotSession(l.atomicSlotSession)
                        .setAtomicSlotOpSeq(l.atomicSlotOpSeq)
                        .setAtomicSlotApplied(l.atomicSlotApplied)
                        .setAtomicSlotOldValue(l.atomicSlotOldValue)
                        .setAtomicSlotValue(l.atomicSlotValue)
                        .setAtomicSlotVersion(l.atomicSlotVersion);
            }
            b.addLocks(lb);
        }
        for (long s : sessions) {
            b.addSessions(s);
        }
        return b.build();
    }

    /**
     * 从 proto 全量恢复（替换当前内容；快照加载使用）。
     *
     * @param st 快照状态
     */
    public void load(SnapshotState st) {
        locks.clear();
        sessions.clear();
        sessionIndex.clear();
        heldIndex.clear();
        adminView.clear();
        for (SnapshotLock l : st.getLocksList()) {
            SLock sl = new SLock(l.getLockTypeValue(), l.getLeaseToken(),
                    l.getExpiresAtMs(), l.getLeaseMs());
            for (SnapshotHolder h : l.getHoldersList()) {
                sl.holders.put(new Holder(h.getSessionId(), h.getThreadId()), h.getCount());
            }
            if (l.getLockTypeValue() == LockType.LOCK_TYPE_SEMAPHORE_VALUE) {
                sl.permitsTotal = l.getPermitsTotal();
                int held = 0;
                for (Integer c : sl.holders.values()) {
                    held += c;
                }
                sl.permitsAvailable = sl.permitsTotal - held;
                locks.put(l.getKey(), sl);
                heldIndex.put(l.getKey(), new HeldRef(l.getLeaseToken(), l.getExpiresAtMs(),
                        Set.copyOf(sl.holders.keySet()), l.getLockTypeValue()));
            } else if (isAtomicType(l.getLockTypeValue())) {
                sl.atomicInitial = l.getAtomicInitial();
                sl.atomicValue = l.getAtomicValue();
                sl.atomicVersion = l.getAtomicVersion();
                sl.atomicSlotSession = l.getAtomicSlotSession();
                sl.atomicSlotOpSeq = l.getAtomicSlotOpSeq();
                sl.atomicSlotApplied = l.getAtomicSlotApplied();
                sl.atomicSlotOldValue = l.getAtomicSlotOldValue();
                sl.atomicSlotValue = l.getAtomicSlotValue();
                sl.atomicSlotVersion = l.getAtomicSlotVersion();
                // 常驻条目：不入 heldIndex（无持有语义），仅入表与观察视图。
                locks.put(l.getKey(), sl);
            } else if (l.getLockTypeValue() == LockType.LOCK_TYPE_BARRIER_VALUE) {
                sl.barrierParties = l.getBarrierParties();
                sl.barrierGeneration = l.getBarrierGeneration();
                sl.barrierArrivals = l.getBarrierCurrentArrivalsList().stream()
                        .map(a -> new ArrivalRef(a.getSessionId(), a.getRequestId())).toList();
                sl.barrierActionSession = l.getBarrierActionSession();
                sl.barrierActionRequest = l.getBarrierActionRequest();
                sl.barrierCompletedGeneration = l.getBarrierCompletedGeneration();
                sl.barrierCompletedResult = l.getBarrierCompletedResult();
                sl.barrierCompletedArrivals = l.getBarrierCompletedArrivalsList().stream()
                        .map(a -> new ArrivalRef(a.getSessionId(), a.getRequestId())).toList();
                sl.barrierCompletedExecutor = l.getBarrierCompletedExecutor();
                // 常驻条目：不入 heldIndex（无持有语义），仅入表与观察视图。
                locks.put(l.getKey(), sl);
            } else if (l.getLockTypeValue() == LockType.LOCK_TYPE_LATCH_VALUE) {
                sl.latchTotal = l.getLatchTotal();
                sl.latchCount = l.getLatchCount();
                // 装载的屏障无参与会话（参与者不入快照）：首个 countDown
                // 恢复参与关系；期间条目不得被 dropSessionHolders 误删——
                // 空参与者集仅在"曾有参与者且全散"时才是回收信号，装载态
                // 条目由后续日志重放补回参与者，语义与未截断副本收敛一致。
                locks.put(l.getKey(), sl);
            } else {
                locks.put(l.getKey(), sl);
                heldIndex.put(l.getKey(), new HeldRef(l.getLeaseToken(), l.getExpiresAtMs(),
                        Set.copyOf(sl.holders.keySet()), l.getLockTypeValue()));
            }
            adminView.put(l.getKey(), viewOf(sl));
        }
        sessions.addAll(st.getSessionsList());
        sessionIndex.addAll(st.getSessionsList());
    }

    /**
     * 全量摘要（SHA-256 hex）：跨副本一致性比对基准（"锁不丢"断言与
     * 快照比对的公共判据）。
     *
     * @return 64 位十六进制摘要
     */
    public String digest() {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(toProto().toByteArray()));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 不可用", e);
        }
    }

    /**
     * 屏障倒计数应用点镜像：条目不存在则按 {@code total}
     * 创建（纯初始化的复制落点），存在则刷新剩余计数并登记参与会话。
     * 参与者散尽的回收经 {@link #dropSessionHolders} 同路径完成。
     *
     * @param key       屏障键
     * @param sessionId 逻辑会话 id（参与者）
     * @param total     定型初始计数（创建时使用）
     * @param remaining 生效后的剩余计数
     */
    public void latchApplied(String key, long sessionId, long total, long remaining) {
        SLock l = locks.get(key);
        if (l == null) {
            // GRANTED 且条目缺席 = 引擎刚创建（countDown/初始化的复制落点），
            // 按 total 镜像建条目；total 为 0 的病态序列（引擎无创建依据）零扰动。
            if (total <= 0) {
                return;
            }
            l = new SLock(LockType.LOCK_TYPE_LATCH_VALUE, 0, 0, 0);
            l.latchTotal = total;
            locks.put(key, l);
        }
        if (l.lockType != LockType.LOCK_TYPE_LATCH_VALUE) {
            // 家族冲突（引擎已回 REJECT_TYPE_MISMATCH）：影子零扰动。
            return;
        }
        l.latchCount = remaining;
        l.latchParticipants.add(sessionId);
        adminView.put(key, viewOf(l));
    }

    /**
     * 循环屏障复制态镜像（三命令应用点与 SESSION_CLOSE 破障传播的落点）：
     * 条目不存在则按导出态创建，存在则整体刷新（世代回卷、账簿归档与
     * 破障传播都表现为导出态的自然变化，本方法不做增量推导）。家族冲突
     * （引擎已回 {@code REJECT_TYPE_MISMATCH} 的拒绝路径）零扰动。
     *
     * @param key 屏障键
     * @param d   引擎复制态导出（逻辑会话 id 投影由调用方完成）
     */
    public void barrierMirror(String key, BarrierMirrorData d) {
        SLock l = locks.get(key);
        if (l == null) {
            l = new SLock(LockType.LOCK_TYPE_BARRIER_VALUE, 0, 0, 0);
            locks.put(key, l);
        }
        if (l.lockType != LockType.LOCK_TYPE_BARRIER_VALUE) {
            return; // 家族冲突：镜像零扰动
        }
        l.barrierParties = d.parties();
        l.barrierGeneration = d.generation();
        l.barrierArrivals = List.copyOf(d.arrivals());
        l.barrierActionSession = d.actionSession();
        l.barrierActionRequest = d.actionRequest();
        l.barrierCompletedGeneration = d.completedGeneration();
        l.barrierCompletedResult = d.completedResult();
        l.barrierCompletedArrivals = List.copyOf(d.completedArrivals());
        l.barrierCompletedExecutor = d.completedExecutor();
        adminView.put(key, viewOf(l));
    }

    /**
     * key 是否为循环屏障条目（含装载态）。
     *
     * @param key 屏障键
     * @return 存在 BARRIER 条目为 {@code true}
     */
    public boolean isBarrier(String key) {
        SLock l = locks.get(key);
        return l != null && l.lockType == LockType.LOCK_TYPE_BARRIER_VALUE;
    }

    /**
     * 协议 {@code LockType} 数值是否原子形态（7/8/9，含装载态）。
     *
     * @param lockTypeValue 协议数值
     * @return 原子形态为 {@code true}
     */
    public static boolean isAtomicType(int lockTypeValue) {
        return lockTypeValue == LockType.LOCK_TYPE_ATOMIC_LONG_VALUE
                || lockTypeValue == LockType.LOCK_TYPE_ATOMIC_INTEGER_VALUE
                || lockTypeValue == LockType.LOCK_TYPE_ATOMIC_BOOLEAN_VALUE;
    }

    /**
     * 原子操作应用点镜像（ATOMIC_OP_ENTRY 的 GRANTED 落点）：条目不存在且
     * 本条为写操作时按请求初值主张镜像创建（与引擎懒建条件严格一致——
     * GET 不建、镜像亦不建）；写操作刷新值/版本戳并覆盖去重槽（含
     * {@code applied=false} 的 CAS 未命中应答——重发可判性随槽镜像存续，
     * 快照与追赶据此复原）。家族误用与断言拒绝（回执非 OK）不经本方法，
     * 镜像零扰动。
     *
     * @param key         原子变量键
     * @param kindValue   请求携带的协议形态数值（建镜像条目定型用）
     * @param sessionId   逻辑会话 id（槽记录）
     * @param opSeq       请求 op_seq（0=不参与去重，槽不更新）
     * @param initialClaim 请求初值主张（建镜像时定格为定型初值）
     * @param get         本条是否 GET 条目（零迁移，镜像不创建不刷新）
     * @param applied     应答四元组 applied
     * @param oldValue    应答四元组 oldValue
     * @param value       应答四元组 value
     * @param version     应答四元组 version
     */
    public void atomicApplied(String key, int kindValue, long sessionId, long opSeq,
                              long initialClaim, boolean get, boolean applied, long oldValue,
                              long value, long version) {
        SLock l = locks.get(key);
        if (l == null) {
            if (get) {
                return; // GET 且条目缺席：引擎未建条目，镜像同样零扰动
            }
            l = new SLock(kindValue, 0, 0, 0);
            l.atomicInitial = initialClaim;
            locks.put(key, l);
        }
        if (get || l.lockType != kindValue) {
            // GET 零迁移；形态与镜像定型不符（引擎已回互拒）——均零扰动、不重发布。
            return;
        }
        l.atomicValue = value;
        l.atomicVersion = version;
        if (opSeq >= 1) {
            l.atomicSlotSession = sessionId;
            l.atomicSlotOpSeq = opSeq;
            l.atomicSlotApplied = applied;
            l.atomicSlotOldValue = oldValue;
            l.atomicSlotValue = value;
            l.atomicSlotVersion = version;
        }
        adminView.put(key, viewOf(l));
    }

    /**
     * key 是否为屏障条目（含装载态）。
     *
     * @param key 屏障键
     * @return 存在 LATCH 条目为 {@code true}
     */
    public boolean hasLatch(String key) {
        SLock l = locks.get(key);
        return l != null && l.lockType == LockType.LOCK_TYPE_LATCH_VALUE;
    }

    /**
     * 屏障定型初始计数（既有镜像条目读取；不存在或非屏障返回 0）。
     *
     * @param key 屏障键
     * @return 定型初始计数
     */
    public long latchTotalOf(String key) {
        SLock l = locks.get(key);
        return l != null && l.lockType == LockType.LOCK_TYPE_LATCH_VALUE ? l.latchTotal : 0;
    }

    /**
     * 屏障剩余计数（Leader 本地 await 裁决读侧）。
     *
     * @param key 屏障键
     * @return 剩余计数；条目不存在或非屏障为 {@code -1}
     */
    public long latchCount(String key) {
        SLock l = locks.get(key);
        return l != null && l.lockType == LockType.LOCK_TYPE_LATCH_VALUE ? l.latchCount : -1;
    }

    /**
     * key 是否为 Semaphore 条目。
     *
     * @param key 锁键
     * @return 存在 SEMAPHORE 条目为 {@code true}
     */
    public boolean isSemaphore(String key) {
        SLock l = locks.get(key);
        return l != null && l.lockType == LockType.LOCK_TYPE_SEMAPHORE_VALUE;
    }

    /**
     * Semaphore 当前可用许可数（Leader 队首推进判定读侧）。
     *
     * @param key 锁键
     * @return 可用许可数；条目不存在（已回收，池即全量待重建）时返回
     *         {@link Integer#MAX_VALUE}（队首可自立总量重主张）
     */
    public int permitsAvailable(String key) {
        SLock l = locks.get(key);
        if (l == null || l.lockType != LockType.LOCK_TYPE_SEMAPHORE_VALUE) {
            return Integer.MAX_VALUE;
        }
        return l.permitsAvailable;
    }

    /**
     * 当前持锁 key 数（测试与指标用）。
     *
     * @return 锁条目数
     */
    public int lockCount() {
        return locks.size();
    }

    /**
     * 指定 key 的管理观察视图（ADMIN_KEY_DETAIL 集群数据源）。
     * 跨线程弱一致读：结果可落后于 apply 一至数个变更点，MUST NOT 用于
     * 授予判定。
     *
     * @param key 锁键
     * @return 不可变视图；条目不存在（含已被回收）为 {@code null}
     */
    public AdminEntryView adminEntry(String key) {
        return adminView.get(key);
    }

    /**
     * 全部条目的管理观察视图（ADMIN_LIST_KEYS 集群数据源）。
     *
     * @return key → 视图 的弱一致并发视图（不复制，只读用途）
     */
    public Map<String, AdminEntryView> adminEntries() {
        return adminView;
    }

    /**
     * 按家族聚合的持有中条目数（gauge 指标读数）：弱一致遍历无锁
     * 投影 {@code heldIndex}（并发读取口径与本类线程模型注释一致——结果
     * 可旧不可错），SEMAPHORE 家族按 {@code lockType} 归组、其余投影条目
     * 归锁家族；LATCH 无持有语义、恒不入投影，天然排除。
     *
     * @return 长度为 2 的数组：{@code [0]} 锁家族条目数、{@code [1]} Semaphore 家族条目数
     */
    public int[] heldFamilyCounts() {
        int lock = 0;
        int semaphore = 0;
        for (HeldRef ref : heldIndex.values()) {
            if (ref.lockType() == LockType.LOCK_TYPE_SEMAPHORE_VALUE) {
                semaphore++;
            } else {
                lock++;
            }
        }
        return new int[] {lock, semaphore};
    }

    /**
     * 已登记逻辑会话集合（Leader 失联批量清理输入；应用线程外读安全，
     * 返回快照副本）。
     *
     * @return 逻辑会话 id 的无序快照
     */
    public Set<Long> liveSessions() {
        return Set.copyOf(sessionIndex);
    }

    /**
     * 摘要输入的字节长度（digest 前序列化开销的观测口）。
     *
     * @return {@link #toProto()} 序列化的字节数
     */
    int stateBytes() {
        return toProto().toByteArray().length;
    }
}
