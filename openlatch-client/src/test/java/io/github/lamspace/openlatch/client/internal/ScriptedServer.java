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

import io.github.lamspace.openlatch.protocol.Envelope;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

/**
 * 脚本化测试桩服务器：以真实 TCP + 线路分帧应答 {@link Envelope}，
 * 用 {@link io.github.lamspace.openlatch.client.internal.ConnectionManager}
 * 无法区分的形态驱动客户端 Leader 发现与故障转移分支（§6.3 逐边单测，
 * 变更 s3-leader-discovery-failover 3.4）。
 *
 * <p>应答逻辑由 {@link #handler} 提供（请求 → 响应，返回 {@code null} 不回包）；
 * 全部入站请求按序记入 {@link #received()} 供断言。多实例可互指对方地址，
 * 构造"种子 A 提示直连 B"这类跨节点场景。
 */
public final class ScriptedServer implements AutoCloseable {

    /** 入站帧上限（与真实服务端一致）。 */
    private static final int MAX_FRAME = 1024 * 1024;

    /** 本桩监听组与子连接组（独立小池，测试用）。 */
    private final EventLoopGroup boss;
    private final EventLoopGroup worker;
    /** 监听通道。 */
    private final Channel serverChannel;
    /** 应答函数。 */
    private volatile Function<Envelope, Envelope> handler;
    /** 入站请求记录（跨连接汇聚，断言到达点用）。 */
    private final List<Envelope> received = new CopyOnWriteArrayList<>();

    /**
     * 启动桩服务器于空闲端口。
     *
     * @param handler 初始应答函数
     * @throws IOException 端口/绑定失败
     */
    public ScriptedServer(Function<Envelope, Envelope> handler) throws IOException {
        this.handler = handler;
        this.boss = new NioEventLoopGroup(1);
        this.worker = new NioEventLoopGroup(1);
        int port = freePort();
        ServerBootstrap b = new ServerBootstrap()
                .group(boss, worker)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME, 0, 4, 0, 4))
                                .addLast(new ProtobufDecoder(Envelope.getDefaultInstance()))
                                .addLast(new LengthFieldPrepender(4))
                                .addLast(new ProtobufEncoder())
                                .addLast(new ChannelInboundHandlerAdapter() {
                                    @Override
                                    public void channelRead(ChannelHandlerContext ctx, Object msg) {
                                        if (!(msg instanceof Envelope env)) {
                                            return;
                                        }
                                        received.add(env);
                                        Envelope resp = handler.apply(env);
                                        if (resp != null) {
                                            ctx.writeAndFlush(resp);
                                        }
                                    }
                                });
                    }
                });
        try {
            this.serverChannel = b.bind("127.0.0.1", port).sync().channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("bind interrupted", e);
        }
    }

    /** 空闲端口探测。 */
    private static int freePort() throws IOException {
        try (ServerSocket s = new ServerSocket(0)) {
            return s.getLocalPort();
        }
    }

    /** 本桩 {@code host:port} 接入地址。 */
    public String address() {
        return "127.0.0.1:" + port();
    }

    /** 监听端口。 */
    public int port() {
        return ((java.net.InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    /** 替换应答函数（运行中切换脚本分支）。 */
    public void setHandler(Function<Envelope, Envelope> newHandler) {
        this.handler = newHandler;
    }

    /** 已收请求（不可变快照）。 */
    public List<Envelope> received() {
        return List.copyOf(received);
    }

    /** 已收指定类型请求数。 */
    public int countType(io.github.lamspace.openlatch.protocol.MessageType type) {
        return (int) received.stream().filter(e -> e.getType() == type).count();
    }

    /** 清空入站记录（分段断言用）。 */
    public void clearReceived() {
        received.clear();
    }

    @Override
    public void close() {
        serverChannel.close();
        boss.shutdownGracefully();
        worker.shutdownGracefully();
    }
}
