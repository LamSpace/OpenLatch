package io.github.lamspace.openlatch.core.session;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 会话登记表：{@code sessionId → 该会话触及的 key 集合}，加速断连清理（设计说明书 §4.7）。
 * 授予/排队时登记 key；{@link #remove} 原子移除会话并返回其触及的 key 集合，
 * 使会话校验（{@link #touchIfPresent}）与清理（{@link #remove}）相互原子互斥。
 */
public final class SessionRegistry {

    private final ConcurrentHashMap<Long, Set<String>> touchedKeys = new ConcurrentHashMap<>();

    /** 登记新会话。 */
    public void register(long sessionId) {
        touchedKeys.putIfAbsent(sessionId, ConcurrentHashMap.newKeySet());
    }

    /** 会话是否存在（快速预检，非权威）。 */
    public boolean contains(long sessionId) {
        return touchedKeys.containsKey(sessionId);
    }

    /**
     * 原子地"会话仍存在则登记 key"。与 {@link #remove} 在同一 key 上原子互斥：
     * 返回 {@code false} 表示会话已关闭（调用方必须拒绝该请求），
     * 返回 {@code true} 表示已登记，会话关闭时该 key 必会被清理。
     */
    public boolean touchIfPresent(long sessionId, String key) {
        return touchedKeys.computeIfPresent(sessionId, (id, set) -> {
            set.add(key);
            return set;
        }) != null;
    }

    /** 原子移除会话并返回其触及的 key 集合；会话不存在返回 {@code null}（幂等）。 */
    public Set<String> remove(long sessionId) {
        return touchedKeys.remove(sessionId);
    }
}
