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

package io.github.lamspace.openlatch.server.session;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * sessionId → 会话的反向索引（design.md D2）：core 的队首通知事件只携带
 * {@code sessionId}，通知桥经此查得 Channel 写回 {@code AWAIT_NOTIFY}。
 */
public final class ServerSessionRegistry {

    /** 构造空会话注册表。 */
    public ServerSessionRegistry() {
    }

    private final ConcurrentMap<Long, ServerSession> sessions = new ConcurrentHashMap<>();

    /**
     * 登记会话（以会话 id 为键）。
     *
     * @param session 已握手的会话
     */
    public void register(ServerSession session) {
        sessions.put(session.sessionId(), session);
    }

    /**
     * 摘除并返回会话；不存在返回 null。重复摘除幂等。
     *
     * @param sessionId 要摘除的会话
     * @return 被摘除的会话；不存在返回 {@code null}
     */
    public ServerSession remove(long sessionId) {
        return sessions.remove(sessionId);
    }

    /**
     * 按会话 id 反查。
     *
     * @param sessionId 会话
     * @return 会话；不存在返回 {@code null}
     */
    public ServerSession get(long sessionId) {
        return sessions.get(sessionId);
    }

    /**
     * 已登记会话数。
     *
     * @return 会话数量
     */
    public int size() {
        return sessions.size();
    }
}
