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
 *
 * <p><b>线程模型</b>：全部写方法（{@link #activate}、{@link #markClosed()}、
 * {@link #tryBeginRequest}/{@link #endRequest()}）由所属连接的 EventLoop
 * 串行调用——{@code channelInactive} 与写完成 listener 中的 {@code endRequest}
 * 同为该 EventLoop 回调，单连接内不存在并发写。字段声明 volatile/AtomicInteger
 * 是为跨线程<em>观察读</em>提供可见性：租约扫描线程经会话注册表反查读取
 * {@link #channel()}/{@link #sessionId()}，{@link #inflight()} 亦可被任意
 * 线程观测。构造与 {@link #KEY} 挂载在 accept 注册线程完成，先于一切事件
 * 回调（发生-先于由 Netty 事件顺序保证）。
 */
public final class ServerSession {

    /** Channel 属性键：会话实例存于该属性，随连接生命周期。 */
    public static final AttributeKey<ServerSession> KEY = AttributeKey.valueOf("openlatch.session");

    /** 绑定的连接，随连接生命周期。 */
    private final Channel channel;
    /** 在途请求计数。 */
    private final AtomicInteger inflight = new AtomicInteger();
    /** 会话 id，握手成功后有效。 */
    private volatile long sessionId;
    /**
     * 握手协商的客户端协议版本（应答信封回显来源，v2 起服务端按连接协商版本
     * 出站）。未握手连接的兜底值为 1（Phase 1 默认；推送仅可能发生在握手后，
     * 兜底值实际不可观察）。
     */
    private volatile int protocolVersion = 1;
    /** 握手完成标记。 */
    private volatile boolean handshaken;
    /** 关闭标记，保证断连清理只执行一次。 */
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
     * 握手成功时调用：激活会话并记录协商的客户端协议版本（后续应答与推送
     * 的回显依据）。仅由单 IO 线程调用，无需同步。
     *
     * @param sessionId       服务端分配的会话 id
     * @param protocolVersion 握手信封携带的客户端协议版本（服务端已校验在支持区间内）
     */
    public void activate(long sessionId, int protocolVersion) {
        this.sessionId = sessionId;
        this.protocolVersion = protocolVersion;
        this.handshaken = true;
    }

    /**
     * 握手协商的客户端协议版本。
     *
     * @return 握手时记录的版本；未握手连接为兜底值 1
     */
    public int protocolVersion() {
        return protocolVersion;
    }

    /**
     * 幂等关闭标记：断连清理只执行一次。前提是所有调用点（通道失效、
     * 空闲关闭）都在本连接所属 EventLoop 上串行——读-判-写在该前提下
     * 即足够；若未来允许多线程并发调用，"首次"判定将不成立，须改用
     * CAS 实现（代码侧待办，登记于变更 design D5）。
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
