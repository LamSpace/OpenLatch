package io.github.lamspace.openlatch.poc.harness;

import io.github.lamspace.openlatch.poc.proto.RaftPoc.ShadowHolder;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.ShadowLock;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.ShadowState;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * 影子状态表（design D9）：apply 路径同步维护的逻辑持锁视图。
 *
 * <p>以逻辑会话 id 为归属标识（各节点 engine 的内部 sid 不外露），
 * 承担快照序列化载体与 {@code DUMP} 摘要比对基准。持有条目按授予
 * 先后保持插入序（token 单调），快照重建回灌依赖该序对齐 token。
 *
 * <p>线程模型：仅在状态机应用路径（{@link LockStateMachineCore} 的 applyLock 内）
 * 单线程读写。
 */
public final class ShadowTable {

    /** 单 holder 记录：逻辑会话 + 线程 + 持有计数。 */
    public record Holder(long sessionId, long threadId, int count) { }

    /** 单 key 记录。 */
    public record SLock(int lockType, long leaseToken, long expiresAt, long leaseMs,
                        Map<Holder, Integer> holders) { }

    private final LinkedHashMap<String, SLock> locks = new LinkedHashMap<>();
    private final LinkedHashSet<Long> sessions = new LinkedHashSet<>();

    /** 登记逻辑会话。 */
    public void addSession(long sid) {
        sessions.add(sid);
    }

    /** 摘除逻辑会话（PoC 不断连，仅完备性）。 */
    public void removeSession(long sid) {
        sessions.remove(sid);
    }

    /** 逻辑会话是否已登记。 */
    public boolean hasSession(long sid) {
        return sessions.contains(sid);
    }

    /** 授予后登记：同 holder 计数 +1，新 holder 追加。 */
    public void grant(long sid, long threadId, String key, int lockType,
                      long token, long leaseMs, long expiresAt) {
        SLock l = locks.computeIfAbsent(key,
                k -> new SLock(lockType, token, expiresAt, leaseMs, new LinkedHashMap<>()));
        l.holders().merge(new Holder(sid, threadId, 1), 1, Integer::sum);
    }

    /** 释放成功：对应 holder 计数 -1（归零摘除 holder），条目空则移除 key。 */
    public void release(long sid, long threadId, String key) {
        SLock l = locks.get(key);
        if (l == null) {
            return;
        }
        Holder match = null;
        for (Holder h : l.holders().keySet()) {
            if (h.sessionId() == sid && h.threadId() == threadId) {
                match = h;
                break;
            }
        }
        if (match != null) {
            int left = l.holders().get(match) - 1;
            if (left <= 0) {
                l.holders().remove(match);
            } else {
                l.holders().put(match, left);
            }
        }
        if (l.holders().isEmpty()) {
            locks.remove(key);
        }
    }

    /** 回灌（快照重建）：按授予序逐条放入。 */
    public void putLoaded(String key, int lockType, long token, long expiresAt, long leaseMs) {
        SLock l = new SLock(lockType, token, expiresAt, leaseMs, new LinkedHashMap<>());
        locks.put(key, l);
    }

    /** 回灌 holder 计数。 */
    public void putLoadedHolder(String key, Holder h) {
        locks.get(key).holders().put(h, h.count());
    }

    /** 全量 proto 形态（保持插入序，digest 稳定）。 */
    public ShadowState toProto() {
        ShadowState.Builder b = ShadowState.newBuilder();
        for (Map.Entry<String, SLock> en : locks.entrySet()) {
            SLock l = en.getValue();
            ShadowLock.Builder lb = ShadowLock.newBuilder()
                    .setKey(en.getKey()).setLockType(l.lockType())
                    .setLeaseToken(l.leaseToken()).setExpiresAt(l.expiresAt())
                    .setLeaseMs(l.leaseMs());
            for (Map.Entry<Holder, Integer> h : l.holders().entrySet()) {
                Holder k = h.getKey();
                lb.addHolders(ShadowHolder.newBuilder()
                        .setSessionId(k.sessionId()).setThreadId(k.threadId())
                        .setCount(h.getValue()));
            }
            b.addLocks(lb);
        }
        for (long s : sessions) {
            b.addSessions(s);
        }
        return b.build();
    }

    /** 从 proto 恢复（替换当前内容）。 */
    public void load(ShadowState st) {
        locks.clear();
        sessions.clear();
        for (ShadowLock l : st.getLocksList()) {
            putLoaded(l.getKey(), l.getLockType(), l.getLeaseToken(), l.getExpiresAt(), l.getLeaseMs());
            for (ShadowHolder h : l.getHoldersList()) {
                putLoadedHolder(l.getKey(), new Holder(h.getSessionId(), h.getThreadId(), h.getCount()));
            }
        }
        sessions.addAll(st.getSessionsList());
    }

    /** 序列化的全量影子表（快照载体）。 */
    public byte[] serialize() {
        return toProto().toByteArray();
    }

    /** 反序列化恢复。 */
    public void deserialize(byte[] bytes) throws com.google.protobuf.InvalidProtocolBufferException {
        load(ShadowState.parseFrom(bytes));
    }

    /** 全量摘要（跨节点一致性比对基准）。 */
    public String digest() {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(md.digest(serialize()));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** 当前锁条目数。 */
    public int lockCount() {
        return locks.size();
    }
}
