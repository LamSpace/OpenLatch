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
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

/**
 * pipeline 装配（设计说明书 §5.2）：
 * <pre>
 * 入站: LengthFieldBasedFrameDecoder(1MiB) → ProtobufDecoder → EnvelopeCodecHandler
 *         → IdleStateHandler(read) → ServerSessionHandler
 * 出站: ProtobufEncoder → LengthFieldPrepender(4)
 * </pre>
 */
public final class ServerChannelInitializer extends ChannelInitializer<SocketChannel> {

    /** 最大帧长 1 MiB，超限断连（设计说明书 §3.1）。 */
    public static final int MAX_FRAME_LENGTH = 1024 * 1024;

    private final long idleTimeoutMs;
    private final ServerSessionHandler sessionHandler;
    private final ChannelGroup channels;

    /**
     * 构造 pipeline 装配器。
     *
     * @param idleTimeoutMs  连接读空闲超时（毫秒）
     * @param sessionHandler 共享的会话业务处理器
     * @param channels       连接组，用于关停时统一关闭
     */
    public ServerChannelInitializer(long idleTimeoutMs,
                                    ServerSessionHandler sessionHandler,
                                    ChannelGroup channels) {
        this.idleTimeoutMs = idleTimeoutMs;
        this.sessionHandler = sessionHandler;
        this.channels = channels;
    }

    @Override
    protected void initChannel(SocketChannel ch) {
        channels.add(ch);
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
