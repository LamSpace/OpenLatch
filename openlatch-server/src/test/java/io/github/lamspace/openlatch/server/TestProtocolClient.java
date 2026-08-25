package io.github.lamspace.openlatch.server;

import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 测试用最小协议客户端（design.md D7）：仅收发 {@code Envelope}，不含任何客户端
 * 锁语义（无看门狗、重连、本地簿记）。响应按 {@code request_id} 关联；
 * {@code AWAIT_NOTIFY} 推送进入独立队列。
 */
public final class TestProtocolClient implements AutoCloseable {

    private static final long DEFAULT_TIMEOUT_MS = 5000;

    private final EventLoopGroup group = new NioEventLoopGroup(1);
    private final AtomicLong requestId = new AtomicLong(1);
    private final ConcurrentMap<Long, CompletableFuture<Envelope>> pending = new ConcurrentHashMap<>();
    private final BlockingQueue<Envelope> pushes = new LinkedBlockingQueue<>();

    private Channel channel;
    private volatile long sessionId;

    public void connect(String host, int port) throws InterruptedException {
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        // 出站遍历序：先编码器再分帧器，故 prepender 更靠近 head。
                        ch.pipeline()
                                .addLast(new LengthFieldBasedFrameDecoder(
                                        io.github.lamspace.openlatch.server.net.ServerChannelInitializer.MAX_FRAME_LENGTH,
                                        0, 4, 0, 4))
                                .addLast(new ProtobufDecoder(Envelope.getDefaultInstance()))
                                .addLast(new LengthFieldPrepender(4))
                                .addLast(new ProtobufEncoder())
                                .addLast(new SimpleChannelInboundHandler<Envelope>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx, Envelope msg) {
                                        if (msg.getType() == MessageType.AWAIT_NOTIFY) {
                                            pushes.offer(msg);
                                            return;
                                        }
                                        CompletableFuture<Envelope> future = pending.remove(msg.getRequestId());
                                        if (future != null) {
                                            future.complete(msg);
                                        }
                                    }

                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
                                        // 测试夹具：忽略连接级异常，由用例断言观测。
                                    }
                                });
                    }
                });
        channel = bootstrap.connect(host, port).sync().channel();
    }

    /** 握手并返回服务端分配的 sessionId。 */
    public long hello() throws Exception {
        Envelope request = Envelope.newBuilder()
                .setProtocolVersion(OpenLatchServer.PROTOCOL_VERSION)
                .setType(MessageType.HELLO)
                .setRequestId(nextRequestId())
                .setHelloRequest(HelloRequest.newBuilder()
                        .setClientProtocolVersion(OpenLatchServer.PROTOCOL_VERSION))
                .build();
        Envelope response = sendAndAwait(request);
        if (response.getHelloResponse().getStatus() != StatusCode.OK) {
            throw new IllegalStateException("handshake failed: " + response.getHelloResponse().getStatus());
        }
        sessionId = response.getHelloResponse().getSessionId();
        return sessionId;
    }

    /** 发送并等待同 {@code request_id} 的响应。 */
    public Envelope sendAndAwait(Envelope request) throws Exception {
        return sendAndAwait(request, DEFAULT_TIMEOUT_MS);
    }

    public Envelope sendAndAwait(Envelope request, long timeoutMs)
            throws InterruptedException, ExecutionException, TimeoutException {
        CompletableFuture<Envelope> future = new CompletableFuture<>();
        pending.put(request.getRequestId(), future);
        channel.writeAndFlush(request);
        try {
            return future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException | InterruptedException | ExecutionException e) {
            pending.remove(request.getRequestId());
            throw e;
        }
    }

    /** 仅发送，不等待响应（用于断连注入等场景）。 */
    public void send(Envelope request) {
        channel.writeAndFlush(request);
    }

    /** 等待一条 AWAIT_NOTIFY 推送；超时返回 null。 */
    public Envelope awaitPush(long timeoutMs) throws InterruptedException {
        return pushes.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    public long nextRequestId() {
        return requestId.getAndIncrement();
    }

    public long sessionId() {
        return sessionId;
    }

    public boolean isConnected() {
        return channel != null && channel.isActive();
    }

    /** 模拟客户端故障：直接断开，不发任何释放消息。 */
    public void disconnectAbruptly() {
        if (channel != null) {
            channel.close().awaitUninterruptibly();
        }
    }

    @Override
    public void close() {
        disconnectAbruptly();
        group.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
    }
}
