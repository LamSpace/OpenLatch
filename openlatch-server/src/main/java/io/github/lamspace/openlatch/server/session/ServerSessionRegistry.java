package io.github.lamspace.openlatch.server.session;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * sessionId → 会话的反向索引（design.md D2）：core 的队首通知事件只携带
 * {@code sessionId}，通知桥经此查得 Channel 写回 {@code AWAIT_NOTIFY}。
 */
public final class ServerSessionRegistry {

    private final ConcurrentMap<Long, ServerSession> sessions = new ConcurrentHashMap<>();

    public void register(ServerSession session) {
        sessions.put(session.sessionId(), session);
    }

    /** 摘除并返回会话；不存在返回 null。重复摘除幂等。 */
    public ServerSession remove(long sessionId) {
        return sessions.remove(sessionId);
    }

    public ServerSession get(long sessionId) {
        return sessions.get(sessionId);
    }

    public int size() {
        return sessions.size();
    }
}
