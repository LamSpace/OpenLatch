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

package io.github.lamspace.openlatch.server.net;

import io.github.lamspace.openlatch.core.CoreEngine;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.OpenLatchServer;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.session.ServerSession;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * 连接级业务入口：握手门闩（design.md D8）、请求分发、断连清理。
 * 共享实例（{@code @Sharable}，无可变实例状态），每连接状态存于
 * Channel 属性 {@link ServerSession#KEY}。
 *
 * <p><b>连接生命周期状态机</b>：
 * <pre>
 * 未握手 ──合法 HELLO──▶ 已握手（业务阶段）──断连/空闲──▶ 清理
 *   │  畸形/提前业务请求：回 INVALID_REQUEST，不断连，
 *   │  连接仍可补发合法 HELLO（门闩语义）
 *   └─ 版本不匹配或携带认证令牌：回 INVALID_REQUEST 并断连
 * </pre>
 *
 * <p><b>业务阶段处理矩阵</b>：
 * <ul>
 *   <li>重复 {@code HELLO}：回 {@code INVALID_REQUEST}，不断连、不换会话；</li>
 *   <li>在途请求超过 {@code maxInflightPerConnection}：回 {@code OVERLOADED}，
 *       不计入在途；</li>
 *   <li>{@code PING}：不回复（活动信号已被空闲检测计入）；</li>
 *   <li>其余业务消息：交 {@link RequestDispatcher} 分发并写回响应；</li>
 *   <li>读空闲超时（由上游 {@code IdleStateHandler} 触发）：关闭连接，
 *       走与主动断连相同的清理路径。</li>
 * </ul>
 *
 * <p><b>断连清理</b>：{@link ServerSession#markClosed} 保证
 * {@code channelInactive} 与空闲断连等重复路径只清理一次。清理顺序为
 * <b>先摘注册表、后清会话</b>：摘除后 {@code AWAIT_NOTIFY} 不再路由到
 * 本连接，随后经 {@code CoreEngine.sessionClosed} 释放该会话全部持锁
 * 与等待项。
 */
@ChannelHandler.Sharable
public final class ServerSessionHandler extends SimpleChannelInboundHandler<Envelope> {

    /** 锁语义核心，会话开关与断连清理经此驱动。 */
    private final CoreEngine core;
    /** 服务器配置，提供在途限额等自我保护参数。 */
    private final ServerConfig config;
    /** 会话注册表，断连时摘除 sessionId → 会话映射。 */
    private final ServerSessionRegistry registry;
    /** 请求分发器，已握手业务消息经此映射为 core 命令。 */
    private final RequestDispatcher dispatcher;

    /**
     * 构造会话处理器（共享实例，无连接级可变状态）。
     *
     * @param core       锁语义核心
     * @param config     服务器配置（限额）
     * @param registry   会话注册表
     * @param dispatcher 请求分发器
     */
    public ServerSessionHandler(CoreEngine core, ServerConfig config,
                                ServerSessionRegistry registry, RequestDispatcher dispatcher) {
        this.core = core;
        this.config = config;
        this.registry = registry;
        this.dispatcher = dispatcher;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) {
        ctx.channel().attr(ServerSession.KEY).set(new ServerSession(ctx.channel()));
        ctx.fireChannelActive();
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Envelope msg) {
        ServerSession session = ctx.channel().attr(ServerSession.KEY).get();
        if (!session.isHandshaken()) {
            handleHandshake(ctx, session, msg);
            return;
        }
        if (msg.getType() == MessageType.HELLO) {
            // 重复 HELLO：拒绝但保持原会话（规格"会话握手"）。
            ctx.writeAndFlush(RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        // 自我保护限额（设计说明书 §5.4，design.md D4）。
        if (!session.tryBeginRequest(config.maxInflightPerConnection())) {
            ctx.writeAndFlush(RequestDispatcher.errorResponse(msg, StatusCode.OVERLOADED));
            return;
        }
        Envelope resp = dispatcher.dispatch(session, msg);
        if (resp == null) {
            // PING：不回复（活动信号已被 IdleStateHandler 计入）。
            session.endRequest();
            return;
        }
        ctx.writeAndFlush(resp).addListener(f -> session.endRequest());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        // 断连清理（design.md D3）：先摘注册表（通知不再路由到此连接），后清会话。
        // markClosed 保证 channelInactive 与空闲断连等重复路径只清理一次。
        ServerSession session = ctx.channel().attr(ServerSession.KEY).get();
        if (session != null && session.markClosed()) {
            long sessionId = session.sessionId();
            registry.remove(sessionId);
            if (session.isHandshaken()) {
                core.sessionClosed(sessionId);
            }
        }
        ctx.fireChannelInactive();
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
        if (evt instanceof IdleStateEvent idle && idle.state() == IdleState.READER_IDLE) {
            // 空闲断连：关闭连接，走 channelInactive 同一清理路径。
            ctx.close();
            return;
        }
        ctx.fireUserEventTriggered(evt);
    }

    /**
     * 握手门闩：未握手连接上的首条消息在此裁决。非 {@code HELLO} 或
     * 畸形 {@code HELLO}（无 payload）回 {@code INVALID_REQUEST} 但不断连；
     * 协议版本不匹配或携带认证令牌（Phase 1 必须为空）回
     * {@code INVALID_REQUEST} 并断连；合法 {@code HELLO} 则经
     * {@code CoreEngine.sessionOpened} 分配会话、激活连接簿记、登记注册表，
     * 并回 {@code OK} 与 sessionId。
     *
     * @param ctx     连接上下文
     * @param session 该连接的会话簿记（未握手状态）
     * @param msg     入站消息信封
     */
    private void handleHandshake(ChannelHandlerContext ctx, ServerSession session, Envelope msg) {
        if (msg.getType() != MessageType.HELLO || !msg.hasHelloRequest()) {
            // 握手前业务请求或畸形 HELLO：拒绝、不断连（连接仍可补发合法 HELLO）。
            ctx.writeAndFlush(RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST));
            return;
        }
        HelloRequest hello = msg.getHelloRequest();
        if (hello.getClientProtocolVersion() != OpenLatchServer.PROTOCOL_VERSION
                || !hello.getAuthToken().isEmpty()) {
            // 版本不匹配或携带认证令牌（Phase 1 必须为空）：拒绝并断连（设计说明书 §3.2.1）。
            ctx.writeAndFlush(helloResponse(msg.getRequestId(), StatusCode.INVALID_REQUEST, 0));
            ctx.close();
            return;
        }
        long sessionId = core.sessionOpened();
        session.activate(sessionId);
        registry.register(session);
        ctx.writeAndFlush(helloResponse(msg.getRequestId(), StatusCode.OK, sessionId));
    }

    /**
     * 构造握手响应信封：回显请求的 {@code requestId}，携带服务端协议版本、
     * 状态与分配的 sessionId；{@code OK} 时另携带默认租约时长供客户端参考，
     * 失败路径（{@code INVALID_REQUEST}）sessionId 传 0。
     *
     * @param requestId 原请求的请求 id（回显）
     * @param status    握手结果状态码
     * @param sessionId 分配的会话 id，失败时传 0
     * @return 握手响应信封
     */
    private Envelope helloResponse(long requestId, StatusCode status, long sessionId) {
        return Envelope.newBuilder()
                .setProtocolVersion(OpenLatchServer.PROTOCOL_VERSION)
                .setType(MessageType.HELLO)
                .setRequestId(requestId)
                .setHelloResponse(HelloResponse.newBuilder()
                        .setStatus(status)
                        .setSessionId(sessionId)
                        .setServerProtocolVersion(OpenLatchServer.PROTOCOL_VERSION)
                        .setDefaultLeaseMs(config.defaultLeaseMs()))
                .build();
    }
}
