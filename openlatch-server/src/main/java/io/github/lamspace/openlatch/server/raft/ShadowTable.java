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
 * ③Leader 侧预检查与到期扫描的无锁读索引（{@link #isHeld}/{@link #heldEntries()}，
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
     * 无锁索引的投影记录：当前租约凭证与到期时刻。
     *
     * @param leaseToken  当前租约凭证
     * @param expiresAtMs 到期时刻（毫秒时间戳）
     */
    public record HeldRef(long leaseToken, long expiresAtMs) { }

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
        SLock l = locks.get(key);
        if (l == null) {
            l = new SLock(lockType, token, expiresAt, leaseMs);
            locks.put(key, l);
        } else {
            l.leaseToken = token;
            l.expiresAtMs = expiresAt;
            l.leaseMs = leaseMs;
        }
        l.holders.merge(new Holder(sessionId, threadId), 1, Integer::sum);
        heldIndex.put(key, new HeldRef(token, expiresAt));
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
        heldIndex.computeIfPresent(key, (k, ref) -> new HeldRef(ref.leaseToken(), newExpiresAt));
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
            locks.put(l.getKey(), sl);
            heldIndex.put(l.getKey(), new HeldRef(l.getLeaseToken(), l.getExpiresAtMs()));
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
