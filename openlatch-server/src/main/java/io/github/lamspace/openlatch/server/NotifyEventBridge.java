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

package io.github.lamspace.openlatch.server;

import io.github.lamspace.openlatch.core.CoreEventListener;
import io.github.lamspace.openlatch.protocol.AwaitNotify;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.server.session.ServerSession;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * core 事件出口 → 协议推送（设计说明书 §5.1）：按 {@code sessionId} 反查连接
 * （design.md D2），写回 {@code AWAIT_NOTIFY}（{@code request_id = 0}，
 * {@code request_id_ref} 指向原获取请求）。
 * <p>
 * 回调可能来自租约扫描线程；连接不存在或写出失败时静默丢弃——队列位置由
 * core 的队首响应超时机制兜底回收（规格"队首通知推送"）。
 */
public final class NotifyEventBridge implements CoreEventListener {

    private static final Logger log = LoggerFactory.getLogger(NotifyEventBridge.class);

    private final ServerSessionRegistry registry;

    /**
     * 构造通知桥。
     *
     * @param registry 会话注册表，按 {@code sessionId} 反查连接
     */
    public NotifyEventBridge(ServerSessionRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void notifyHead(long sessionId, long requestId, String key) {
        ServerSession session = registry.get(sessionId);
        if (session == null || !session.channel().isActive()) {
            return; // 连接已不存在：静默丢弃
        }
        Envelope notify = Envelope.newBuilder()
                .setProtocolVersion(OpenLatchServer.PROTOCOL_VERSION)
                .setType(MessageType.AWAIT_NOTIFY)
                .setRequestId(0)
                .setAwaitNotify(AwaitNotify.newBuilder()
                        .setKey(key)
                        .setRequestIdRef(requestId))
                .build();
        try {
            session.channel().writeAndFlush(notify)
                    .addListener(f -> {
                        if (!f.isSuccess()) {
                            log.debug("failed to push AWAIT_NOTIFY to session {}: {}",
                                    sessionId, f.cause() == null ? "channel closed" : f.cause().toString());
                        }
                    });
        } catch (RuntimeException e) {
            log.debug("failed to push AWAIT_NOTIFY to session {}: {}", sessionId, e.toString());
        }
    }
}
