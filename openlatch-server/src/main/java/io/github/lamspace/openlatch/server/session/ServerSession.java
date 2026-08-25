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

import io.netty.channel.Channel;
import io.netty.util.AttributeKey;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Channel 绑定的会话簿记：握手状态、sessionId、inflight 计数（设计说明书 §5.1）。
 * 实例经 {@link #KEY} 存于 Channel 属性；握手成功后另行登记到
 * {@link ServerSessionRegistry} 供通知桥反查。
 */
public final class ServerSession {

    /** Channel 属性键：会话实例存于该属性，随连接生命周期。 */
    public static final AttributeKey<ServerSession> KEY = AttributeKey.valueOf("openlatch.session");

    private final Channel channel;
    private final AtomicInteger inflight = new AtomicInteger();
    private volatile long sessionId;
    private volatile boolean handshaken;
    private volatile boolean closed;

    /**
     * 构造会话簿记，初始为未握手状态。
     *
     * @param channel 绑定的连接
     */
    public ServerSession(Channel channel) {
        this.channel = channel;
    }

    /**
     * 绑定的连接。
     *
     * @return Channel
     */
    public Channel channel() {
        return channel;
    }

    /**
     * 会话 id（握手成功后有效）。
     *
     * @return sessionId
     */
    public long sessionId() {
        return sessionId;
    }

    /**
     * 是否已完成握手。
     *
     * @return 已握手返回 true
     */
    public boolean isHandshaken() {
        return handshaken;
    }

    /**
     * 握手成功时调用：激活会话。仅由单 IO 线程调用，无需同步。
     *
     * @param sessionId 服务端分配的会话 id
     */
    public void activate(long sessionId) {
        this.sessionId = sessionId;
        this.handshaken = true;
    }

    /**
     * 幂等关闭标记：断连清理只执行一次。
     *
     * @return true 表示本次调用是首次关闭
     */
    public boolean markClosed() {
        boolean was = closed;
        closed = true;
        return !was;
    }

    /**
     * inflight +1 并检查是否超过限额。超限返回 false（调用方回 {@code OVERLOADED}
     * 且不计入在途）。
     *
     * @param maxInflight 单连接在途请求上限
     * @return 未超限返回 true（已计入在途），超限返回 false
     */
    public boolean tryBeginRequest(int maxInflight) {
        int current = inflight.incrementAndGet();
        if (current > maxInflight) {
            inflight.decrementAndGet();
            return false;
        }
        return true;
    }

    /** 响应完成后递减在途计数。 */
    public void endRequest() {
        inflight.decrementAndGet();
    }

    /**
     * 当前在途请求数。
     *
     * @return 在途数量
     */
    public int inflight() {
        return inflight.get();
    }
}
