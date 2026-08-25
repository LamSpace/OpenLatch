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
 * 共享实例（{@code @Sharable}），每连接状态存于 Channel 属性。
 */
@ChannelHandler.Sharable
public final class ServerSessionHandler extends SimpleChannelInboundHandler<Envelope> {

    private final CoreEngine core;
    private final ServerConfig config;
    private final ServerSessionRegistry registry;
    private final RequestDispatcher dispatcher;

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
