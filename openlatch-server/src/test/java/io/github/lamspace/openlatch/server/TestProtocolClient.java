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

    /** {@code sendAndAwait(request)} 单参重载使用的默认等待超时（毫秒）。 */
    private static final long DEFAULT_TIMEOUT_MS = 5000;

    /** 本客户端独占的单线程 EventLoop，close() 时优雅关停。 */
    private final EventLoopGroup group = new NioEventLoopGroup(1);
    /** 请求 id 发号器，自 1 起单调递增。 */
    private final AtomicLong requestId = new AtomicLong(1);
    /** {@code request_id} → 等待中响应 future 的关联表，响应到达时摘除并完成。 */
    private final ConcurrentMap<Long, CompletableFuture<Envelope>> pending = new ConcurrentHashMap<>();
    /** {@code AWAIT_NOTIFY} 推送队列，与请求-响应关联通道分离。 */
    private final BlockingQueue<Envelope> pushes = new LinkedBlockingQueue<>();

    /** 已建立的连接通道，{@code connect()} 成功后非 null。 */
    private Channel channel;
    /** 握手成功后服务端分配的会话 id（volatile：握手在调用线程完成、后续可跨线程读）。 */
    private volatile long sessionId;

    /**
     * 建立连接并装配协议 pipeline。
     *
     * @param host 服务器地址
     * @param port 服务器端口
     * @throws InterruptedException 连接等待被中断
     */
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

    /**
     * 握手并返回服务端分配的 sessionId。
     *
     * @return 服务端分配的 sessionId
     * @throws Exception 握手失败（状态非 {@code OK}）或收发异常
     */
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

    /**
     * 发送并等待同 {@code request_id} 的响应（默认超时）。
     *
     * @param request 请求信封
     * @return 响应信封
     * @throws Exception 超时、中断或收发异常
     */
    public Envelope sendAndAwait(Envelope request) throws Exception {
        return sendAndAwait(request, DEFAULT_TIMEOUT_MS);
    }

    /**
     * 发送并等待同 {@code request_id} 的响应。
     *
     * @param request   请求信封
     * @param timeoutMs 等待超时（毫秒）
     * @return 响应信封
     * @throws InterruptedException 等待被中断
     * @throws ExecutionException   收发异常
     * @throws TimeoutException     等待超时
     */
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

    /**
     * 仅发送，不等待响应（用于断连注入等场景）。
     *
     * @param request 请求信封
     */
    public void send(Envelope request) {
        channel.writeAndFlush(request);
    }

    /**
     * 等待一条 AWAIT_NOTIFY 推送；超时返回 null。
     *
     * @param timeoutMs 等待超时（毫秒）
     * @return 推送信封；超时返回 {@code null}
     * @throws InterruptedException 等待被中断
     */
    public Envelope awaitPush(long timeoutMs) throws InterruptedException {
        return pushes.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 生成下一个请求 id。
     *
     * @return 单调递增的请求 id
     */
    public long nextRequestId() {
        return requestId.getAndIncrement();
    }

    /**
     * 握手后的会话 id。
     *
     * @return sessionId
     */
    public long sessionId() {
        return sessionId;
    }

    /**
     * 连接是否存活。
     *
     * @return 连接存在且活跃返回 true
     */
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
