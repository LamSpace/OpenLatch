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

package io.github.lamspace.openlatch.client.internal;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 当前连接的会话上下文：持有服务端分配的 {@code sessionId} 与本连接的
 * {@code requestId} 分配器（详设 §6.2/§6.4）。
 *
 * <p>每次（重）连接成功都会创建新实例：{@code sessionId} 必然更换，
 * {@code requestId} 重新从 1 分配；旧实例随旧连接作废，其残留响应按
 * {@code requestId} 无法匹配新实例的挂起项，直接被丢弃。
 */
public final class SessionContext {

    /** 服务端分配的会话 id，连接生命周期内有效；握手完成前为 0 占位。 */
    private volatile long sessionId;
    /** 请求 id 分配器：单连接内唯一，从 1 起单调递增（握手请求消耗 1）。 */
    private final AtomicLong requestId = new AtomicLong(1);

    /**
     * 以服务端分配的会话 id 创建上下文。
     *
     * @param sessionId 服务端分配的会话 id
     */
    public SessionContext(long sessionId) {
        this.sessionId = sessionId;
    }

    /**
     * 服务端分配的会话 id。
     *
     * @return sessionId
     */
    public long sessionId() {
        return sessionId;
    }

    /**
     * 握手完成后写入服务端分配的真实会话 id。仅由连接管理器在握手成功时
     * 调用一次，在上下文对外可见（提升为当前会话）之前完成。
     *
     * @param sessionId 服务端分配的会话 id
     */
    void assignSessionId(long sessionId) {
        this.sessionId = sessionId;
    }

    /**
     * 分配下一个请求 id。
     *
     * @return 单调递增的请求 id
     */
    public long nextRequestId() {
        return requestId.getAndIncrement();
    }
}
