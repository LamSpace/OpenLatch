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

package io.github.lamspace.openlatch.console;

import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.LockType;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * 控制台冒烟用最小协议客户端（仅握手与 ACQUIRE 两形）：条件等待折叠
 * ACQUIRE（{@code AcquireRequest.condition} 携带形态）属 v9 折叠车道，
 * 其预置 MUST NOT 依赖业务 SDK 的条件门面（SDK 侧 {@code OCondition}
 * 为并行交付面）——本类以裸信封直发 v9 折叠形态，供控制台页面按管理
 * 应答新字段呈现条件区段。连接保持至 {@link #close}：会话存续期间条目
 * 与登记持续可见（断连即回收，页面断言将失去数据源）。
 *
 * <p><b>应答关联</b>：响应按 {@code request_id} 匹配；服务端推送
 * （{@code AWAIT_NOTIFY}/{@code TOPIC_MESSAGE}，{@code request_id=0}）
 * 落入同一队列后被本类跳过丢弃（观察面用例不消费推送）。分帧与编解码
 * 装配与服务端 {@code ServerChannelInitializer} 同参数系（长度前缀 4 字节）。
 */
final class ConsoleRawClient implements AutoCloseable {

    /** 单响应等待上限（毫秒）——预置流量恒为本地快速裁决，超限即夹具错误。 */
    private static final long REPLY_TIMEOUT_MS = 10_000;
    /** 空闲保活周期（毫秒）：低于两档测试服务器的读空闲超时（单机 60s/集群 30s）。 */
    private static final long KEEPALIVE_PERIOD_MS = 15_000;

    /** 本客户端独占的单线程 EventLoop。 */
    private final EventLoopGroup group = new NioEventLoopGroup(1);
    /** 入站信封队列（响应与推送混流，按 request_id 自筛）。 */
    private final BlockingQueue<Envelope> inbound = new LinkedBlockingQueue<>();
    /** 请求 id 发号器，自 1 起单调递增。 */
    private long requestId = 1;
    /** 连接通道（{@link #connect} 后非 null）。 */
    private Channel channel;
    /** 握手协商版本（后续信封 protocol_version 回显值）。 */
    private int protocolVersion;
    /**
     * 空闲保活调度（daemon）：周期发 {@code PING}（服务端仅作活动信号、
     * 不应答）——慢 CI 下页面断言窗口可能跨过读空闲超时，会话一旦 idle
     * 断连即触发条件登记回收，页面失去数据源。
     */
    private java.util.concurrent.ScheduledExecutorService keepalive;

    /**
     * 建立连接并完成握手（明文号；会话在服务端登记，id 由应答携带但本类不消费）。
     *
     * @param host            服务器地址
     * @param port            服务器业务端口
     * @param clientVersion   握手声明的客户端协议版本（v9 折叠用例传 9）
     * @throws InterruptedException 连接/握手等待被中断
     */
    void connect(String host, int port, int clientVersion) throws InterruptedException {
        this.protocolVersion = clientVersion;
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        // 出站遍历序：先编码器再分帧器，故 prepender 更靠近 head。
                        ch.pipeline()
                                .addLast(new LengthFieldBasedFrameDecoder(
                                        io.github.lamspace.openlatch.server.net
                                                .ServerChannelInitializer.MAX_FRAME_LENGTH,
                                        0, 4, 0, 4))
                                .addLast(new ProtobufDecoder(Envelope.getDefaultInstance()))
                                .addLast(new LengthFieldPrepender(4))
                                .addLast(new ProtobufEncoder())
                                .addLast(new SimpleChannelInboundHandler<Envelope>() {
                                    @Override
                                    protected void channelRead0(ChannelHandlerContext ctx,
                                                                 Envelope msg) {
                                        inbound.offer(msg);
                                    }

                                    @Override
                                    public void exceptionCaught(ChannelHandlerContext ctx,
                                                                 Throwable cause) {
                                        // 测试夹具：连接级异常由后续超时断言暴露。
                                    }
                                });
                    }
                });
        channel = bootstrap.connect(host, port).sync().channel();
        Envelope resp = ask(Envelope.newBuilder()
                .setProtocolVersion(clientVersion)
                .setType(MessageType.HELLO)
                .setRequestId(nextRequestId())
                .setHelloRequest(HelloRequest.newBuilder()
                        .setClientProtocolVersion(clientVersion).setClientName("console-raw"))
                .build());
        if (resp.getHelloResponse().getStatus() != StatusCode.OK) {
            throw new AssertionError("raw 握手失败: " + resp.getHelloResponse().getStatus());
        }
        keepalive = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "console-raw-keepalive");
            t.setDaemon(true);
            return t;
        });
        keepalive.scheduleAtFixedRate(this::ping,
                KEEPALIVE_PERIOD_MS, KEEPALIVE_PERIOD_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * 发送一条 {@code PING}（不应答、不自筛队列）——仅维持读空闲计时器复位，
     * 写失败静默（连接已断时由超时断言显形）。
     */
    private void ping() {
        if (channel != null && channel.isActive()) {
            channel.writeAndFlush(Envelope.newBuilder()
                    .setProtocolVersion(protocolVersion)
                    .setType(MessageType.PING)
                    .setRequestId(nextRequestId())
                    .build());
        }
    }

    /**
     * 发送一条 ACQUIRE（默认租约）并等待同 request_id 应答的获取状态码。
     *
     * @param key       锁键
     * @param threadId  归属线程 id
     * @param waitMs    等待语义（0=立即式；-1=无限挂起——折叠形态必须非 0）
     * @param condition 条件名；null 为普通获取
     * @return 应答状态码（OK/QUEUED 等）
     */
    StatusCode acquire(String key, long threadId, long waitMs, String condition) {
        return acquire(key, threadId, 0L, waitMs, condition);
    }

    /**
     * 发送一条 ACQUIRE 并等待同 request_id 应答的获取状态码。
     * {@code condition} 非 null 即 v9 折叠 await 形态（presence 直载于
     * 协议字段，与服务端接入层门径同形）。{@code leaseMs>0} 显式租约——
     * 集群档持有者须取长租约（raw 客户端无看门狗，默认 30s 租约会在
     * 页面断言窗口内到期清扫）。
     *
     * @param key       锁键
     * @param threadId  归属线程 id
     * @param leaseMs   请求租约毫秒（0=服务端默认）
     * @param waitMs    等待语义（0=立即式；-1=无限挂起）
     * @param condition 条件名；null 为普通获取
     * @return 应答状态码（OK/QUEUED 等）
     */
    StatusCode acquire(String key, long threadId, long leaseMs, long waitMs, String condition) {
        AcquireRequest.Builder rb = AcquireRequest.newBuilder()
                .setKey(key).setLockType(LockType.LOCK_TYPE_REENTRANT)
                .setThreadId(threadId).setWaitMs(waitMs);
        if (leaseMs > 0) {
            rb.setLeaseMs(leaseMs);
        }
        if (condition != null) {
            rb.setCondition(condition);
        }
        Envelope resp = ask(Envelope.newBuilder()
                .setProtocolVersion(protocolVersion)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(nextRequestId())
                .setAcquireRequest(rb).build());
        return resp.getAcquireResponse().getStatus();
    }

    /**
     * 写请求并按 request_id 匹配应答（途中推送跳过；超时限即夹具失败）。
     *
     * @param request 请求信封
     * @return 应答信封
     */
    private Envelope ask(Envelope request) {
        channel.writeAndFlush(request);
        long deadline = System.currentTimeMillis() + REPLY_TIMEOUT_MS;
        while (System.currentTimeMillis() < deadline) {
            Envelope e;
            try {
                e = inbound.poll(200, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new AssertionError("应答等待被中断", ie);
            }
            if (e == null) {
                continue;
            }
            if (e.getType() != MessageType.AWAIT_NOTIFY
                    && e.getType() != MessageType.TOPIC_MESSAGE
                    && e.getRequestId() == request.getRequestId()) {
                return e;
            }
        }
        throw new AssertionError("应答超时: " + request.getType() + " rid="
                + request.getRequestId());
    }

    /**
     * 下一个请求 id（单调递增）。
     *
     * @return 请求 id
     */
    private long nextRequestId() {
        return requestId++;
    }

    /**
     * 断开连接并关停 EventLoop（会话终结 → 服务端条件登记随回收；
     * 调用方 MUST 在全部页面断言完成后才关闭）。
     */
    @Override
    public void close() {
        if (keepalive != null) {
            keepalive.shutdownNow();
        }
        if (channel != null) {
            channel.close().awaitUninterruptibly();
        }
        group.shutdownGracefully(0, 2, TimeUnit.SECONDS).awaitUninterruptibly();
    }
}
