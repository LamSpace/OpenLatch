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
