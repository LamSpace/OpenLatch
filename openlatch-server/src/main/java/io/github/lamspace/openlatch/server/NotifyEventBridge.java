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
 * <b>线程模型</b>：回调线程不定——释放/会话清理触发的通知来自对应连接的 IO
 * 线程，到期回收与队首清扫触发的通知来自租约扫描线程；注册表反查与
 * {@code writeAndFlush} 均线程安全，写出由 Netty 投递到目标连接的 EventLoop
 * 执行。连接不存在或写出失败时静默丢弃——队列位置由 core 的队首响应超时
 * 机制兜底回收（规格"队首通知推送"）。
 */
public final class NotifyEventBridge implements CoreEventListener {

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(NotifyEventBridge.class);

    /** 会话注册表，按 {@code sessionId} 反查连接。 */
    private final ServerSessionRegistry registry;

    /**
     * 构造通知桥。
     *
     * @param registry 会话注册表，按 {@code sessionId} 反查连接
     */
    public NotifyEventBridge(ServerSessionRegistry registry) {
        this.registry = registry;
    }

    /**
     * 向目标会话推送 {@code AWAIT_NOTIFY}（协议推送唯一入口）：信封
     * {@code request_id = 0}（推送无关联请求），{@code request_id_ref}
     * 指向原 {@code ACQUIRE} 的 request id，供客户端关联挂起等待。
     *
     * <p>会话不存在或通道非活跃时静默丢弃（不重试、不报错——core 的队首
     * 响应超时清扫兜底）；写出失败仅记 debug 日志，不向调用方抛出。
     * 可被任意线程调用（IO 线程与租约扫描线程），线程安全依赖注册表与
     * Netty 写投递。
     *
     * @param sessionId 被通知队首所属会话
     * @param requestId 原获取请求的 request id（写入 request_id_ref）
     * @param key       锁键
     */
    @Override
    public void notifyHead(long sessionId, long requestId, String key) {
        ServerSession session = registry.get(sessionId);
        if (session == null || !session.channel().isActive()) {
            return; // 连接已不存在：静默丢弃
        }
        Envelope notify = Envelope.newBuilder()
                .setProtocolVersion(session.protocolVersion())
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
