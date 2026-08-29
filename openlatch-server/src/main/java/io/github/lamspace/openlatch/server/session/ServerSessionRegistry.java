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
 *
 * <p><b>线程模型</b>：跨线程共享索引——{@link #register}/{@link #remove} 由
 * 会话所属连接的 EventLoop 线程调用（握手登记、断连摘除），{@link #get} 由
 * 任意通知来源线程（连接 IO 线程或租约扫描线程）调用。并发安全由内部
 * {@link ConcurrentHashMap} 承载：register 返回后 get 立即可见（发生-先于经
 * map 同步点）；remove 与 get 竞争时读方至多观察到摘除前的会话，由通知桥的
 * "非活跃即丢弃"分支兜底。
 */
public final class ServerSessionRegistry {

    /** 构造空会话注册表。 */
    public ServerSessionRegistry() {
    }

    /** sessionId → 会话映射，并发容器承载。 */
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
