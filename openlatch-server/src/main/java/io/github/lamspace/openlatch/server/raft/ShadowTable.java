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
 * 影子状态表（详设 §4.1 复制边界的逻辑镜像）：状态机应用路径同步维护的
 * "已复制锁状态"视图，以逻辑会话 id 为归属标识（各副本引擎的内部 sid 不外露）。
 *
 * <p><b>职责</b>：①跨副本一致性摘要的载体（{@link #digest()}，P2-10 退出门与
 * S4 快照比对共用）；②快照序列化结构（{@link #toProto()}/{@link #load}，
 * §7.1 内容 = 锁条目 + 会话注册表，<b>不含</b>等待队列与本地配置，design D9）；
 * ③Leader 侧预检查与到期扫描的无锁读索引（{@link #isHeld}/{@link #isHeldBy}/{@link #heldEntries()}，
 * 供 {@code ReplicationGateway} 与到期驱动消费）。
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
 * 扫描周期的延后，正确性裁决恒在应用路径（§4.5"以应用结果为准"）。
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
     * 无锁索引的投影记录：当前租约凭证、到期时刻与持有者集快照
     * （重入预检消费；快照仅在授予/装载时刷新，摘除可短暂滞后，
     * 判定容错语义见 {@link #isHeldBy}）。
     *
     * @param leaseToken  当前租约凭证
     * @param expiresAtMs 到期时刻（毫秒时间戳）
     * @param holders     持有者集快照（授予时点的不可变拷贝）
     */
    public record HeldRef(long leaseToken, long expiresAtMs, Set<Holder> holders) { }

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
     * 授予登记（计数增量形态，Phase 3 T1）：Semaphore 一次授予归还多许可，
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
        heldIndex.put(key, new HeldRef(token, expiresAt, Set.copyOf(l.holders.keySet())));
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
        }
    }

    /**
     * 释放登记（计数增量形态，Phase 3 T1）：归还 {@code releaseDelta} 个
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
        heldIndex.computeIfPresent(key, (k, ref) -> new HeldRef(ref.leaseToken(), newExpiresAt, ref.holders()));
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
            if (en.getValue().expiresAtMs <= entryTimeMs) {
                freed.add(en.getKey());
            }
        }
        for (String key : freed) {
            locks.remove(key);
            heldIndex.remove(key);
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
                // 屏障条目存续不随参与者散尽而回收（一次性护栏，design D5
                // 修订）：仅摘除参与身份，条目留在表内。
                l.latchParticipants.remove(sessionId);
                continue;
            }
            for (Map.Entry<Holder, Integer> hh : l.holders.entrySet()) {
                if (hh.getKey().sessionId() == sessionId
                        && l.lockType == LockType.LOCK_TYPE_SEMAPHORE_VALUE) {
                    l.permitsAvailable += hh.getValue();
                }
            }
            l.holders.keySet().removeIf(h -> h.sessionId() == sessionId);
            if (l.holders.isEmpty()) {
                removedKeys.add(en.getKey());
            }
        }
        for (String key : removedKeys) {
            locks.remove(key);
            heldIndex.remove(key);
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
     * 指定归属当前是否持有该 key（Phase 3 T1 重入预检通道）：读取
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
            }
            b.addLocks(lb);
        }
        for (long s : sessions) {
            b.addSessions(s);
        }
        return b.build();
    }

    /**
     * 从 proto 全量恢复（替换当前内容；S4 快照加载使用）。
     *
     * @param st 快照状态
     */
    public void load(SnapshotState st) {
        locks.clear();
        sessions.clear();
        sessionIndex.clear();
        heldIndex.clear();
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
                heldIndex.put(l.getKey(),
                        new HeldRef(l.getLeaseToken(), l.getExpiresAtMs(), Set.copyOf(sl.holders.keySet())));
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
                heldIndex.put(l.getKey(),
                        new HeldRef(l.getLeaseToken(), l.getExpiresAtMs(), Set.copyOf(sl.holders.keySet())));
            }
        }
        sessions.addAll(st.getSessionsList());
        sessionIndex.addAll(st.getSessionsList());
    }

    /**
     * 全量摘要（SHA-256 hex）：跨副本一致性比对基准（P2-10 退出门、
     * 故障演练"锁不丢"断言与 S4 快照比对的公共判据）。
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
     * 屏障倒计数应用点镜像（Phase 3 T1）：条目不存在则按 {@code total}
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
     * 已登记逻辑会话集合（Leader 失联批量清理输入；应用线程外读安全，
     * 返回快照副本）。
     *
     * @return 逻辑会话 id 的无序快照
     */
    public Set<Long> liveSessions() {
        return Set.copyOf(sessionIndex);
    }

    /**
     * 摘要输入的字节长度（digest 前序列化开销的观测口，P2-10 记录用）。
     *
     * @return {@link #toProto()} 序列化的字节数
     */
    int stateBytes() {
        return toProto().toByteArray().length;
    }
}
