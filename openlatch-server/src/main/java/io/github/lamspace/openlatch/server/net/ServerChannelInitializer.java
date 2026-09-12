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

import io.github.lamspace.openlatch.protocol.Envelope;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

/**
 * pipeline 装配（TLS 为可选首位置）：
 * <pre>
 * 入站: [SslHandler(可选)] → LengthFieldBasedFrameDecoder(1MiB) → ProtobufDecoder
 *         → EnvelopeCodecHandler → IdleStateHandler(read) → ServerSessionHandler
 * 出站: ProtobufEncoder → LengthFieldPrepender(4) [→ SslHandler]
 * </pre>
 *
 * <p><b>TLS 语义</b>：注入非空 {@link SslContext}
 * 时，每个新连接在 pipeline 首位装配 {@link SslHandler}——TLS 握手于
 * {@code channelActive} 自动开始，出站字节全部经加密。握手超时钉
 * {@value #TLS_HANDSHAKE_TIMEOUT_MS} 毫秒（开启后拒绝明文/未完成
 * 握手的连接，不悬挂至读空闲时限）：静默连接由握手超时断开、明文协议字节由
 * SslHandler 解码失败（{@code NotSslRecordException}）触发——异常沿入站方向
 * 后传至 {@link EnvelopeCodecHandler} 的 {@code exceptionCaught} 断连路径，
 * 该路径由服务端 TLS 用例显式锁定。未注入（{@code null}）即明文栈，行为与
 * 现状逐字节一致。
 */
public final class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {

    /** 最大帧长 1 MiB，超限断连。 */
    public static final int MAX_FRAME_LENGTH = 1024 * 1024;

    /** TLS 握手超时（毫秒）：开启 TLS 后未完成握手即断开（默认 5s）。 */
    private static final long TLS_HANDSHAKE_TIMEOUT_MS = 5_000L;

    /** 连接读空闲超时（毫秒）。 */
    private final long idleTimeoutMs;
    /** 共享的会话业务处理器。 */
    private final ServerSessionHandler sessionHandler;
    /** 连接组，新连接登记、关停时统一关闭。 */
    private final ChannelGroup channels;
    /** TLS 上下文；{@code null} 表示明文栈（不装配 {@link SslHandler}）。 */
    private final SslContext sslContext;

    /**
     * 构造明文栈 pipeline 装配器（兼容既有装配：不注入 TLS）。
     *
     * @param idleTimeoutMs  连接读空闲超时（毫秒）
     * @param sessionHandler 共享的会话业务处理器
     * @param channels       连接组，用于关停时统一关闭
     */
    public ServerChannelInitializer(long idleTimeoutMs,
                                    ServerSessionHandler sessionHandler,
                                    ChannelGroup channels) {
        this(idleTimeoutMs, sessionHandler, channels, null);
    }

    /**
     * 构造 pipeline 装配器：{@code sslContext} 非空时开启 TLS。
     *
     * @param idleTimeoutMs  连接读空闲超时（毫秒）
     * @param sessionHandler 共享的会话业务处理器
     * @param channels       连接组，用于关停时统一关闭
     * @param sslContext     TLS 上下文，{@code null} 即明文栈
     */
    public ServerChannelInitializer(long idleTimeoutMs,
                                    ServerSessionHandler sessionHandler,
                                    ChannelGroup channels,
                                    SslContext sslContext) {
        this.idleTimeoutMs = idleTimeoutMs;
        this.sessionHandler = sessionHandler;
        this.channels = channels;
        this.sslContext = sslContext;
    }

    /**
     * 新连接装配（accept 后在该 Channel 注册的 EventLoop 上调用一次）：
     * 先登记进关停用连接组（{@code channels}，服务器关停时统一关闭全部
     * 活跃连接），再按类注释的入站/出站顺序挂载 pipeline——其中
     * {@code LengthFieldPrepender} 必须比 {@code ProtobufEncoder} 更靠近
     * head（出站遍历序先编码再补长度头，见行内注释），顺序颠倒则分帧器
     * 无从在编码产物之前补长度头，出站帧装配不成立。
     *
     * @param ch 新建立的连接通道
     */
    @Override
    protected void initChannel(SocketChannel ch) {
        channels.add(ch);
        if (sslContext != null) {
            // TLS 必居 pipeline 首位：先于分帧/编解码握手与加解密。SslHandler
            // 于 channelActive 自动开始握手；静默连接由握手超时（5s）断开，
            // 明文协议字节触发 NotSslRecordException 沿入站断连（见类注释）。
            SslHandler ssl = sslContext.newHandler(ch.alloc());
            ssl.setHandshakeTimeoutMillis(TLS_HANDSHAKE_TIMEOUT_MS);
            ch.pipeline().addLast("ssl", ssl);
        }
        // 出站处理器必须位于业务处理器之前（出站事件自尾向头传播），
        // 且 LengthFieldPrepender 必须比 ProtobufEncoder 更靠近 head：
        // 出站遍历序为先编码器（Envelope → ByteBuf）再分帧器（补长度头）。
        ch.pipeline()
                .addLast("frame", new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4))
                .addLast("protobuf-decoder", new ProtobufDecoder(Envelope.getDefaultInstance()))
                .addLast("codec-guard", new EnvelopeCodecHandler())
                .addLast("idle", new IdleStateHandler(idleTimeoutMs, 0, 0, TimeUnit.MILLISECONDS))
                .addLast("frame-prepender", new LengthFieldPrepender(4))
                .addLast("protobuf-encoder", new ProtobufEncoder())
                .addLast("session", sessionHandler);
    }
}
