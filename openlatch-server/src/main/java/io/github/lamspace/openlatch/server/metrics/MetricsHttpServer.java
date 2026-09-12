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

package io.github.lamspace.openlatch.server.metrics;

import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.http.HttpVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * 管理端口 HTTP 服务：在独立
 * 端口上以 Netty HTTP 暴露 {@code GET /metrics}（Prometheus 文本全量快照）与
 * {@code GET /healthz}（存活探针），其余路径一律 404。不引入 Web 框架。
 *
 * <p><b>线程模型</b>：独立 boss/worker 各 1 线程的守护 {@link NioEventLoopGroup}
 * （{@code openlatch-metrics-*}），与锁端口的 IO 线程组完全隔离——抓取慢
 * 客户端（Prometheus 默认 10s 超时）MUST NOT 占用或阻塞锁请求处理线程；
 * {@code /metrics} 的渲染（{@link PrometheusMeterRegistry#scrape()}）在
 * metrics worker 线程执行，对注册表只做无锁弱一致读。
 *
 * <p><b>连接语义</b>：每个响应以 {@code Connection: close} 收尾并关闭通道
 * （抓取是低频短连接，不做 keep-alive 复用，避免半开连接簿记）。
 *
 * <p><b>生命周期</b>：构造只存引用不占资源；{@link #start()} 绑定端口
 * （port=0 时取操作系统分配值，经 {@link #port()} 回读），绑定失败抛
 * {@link IllegalStateException}（调用方按"启动失败退出不进入半启动"处理）；
 * {@link #close()} 幂等，先解除监听再回收线程组。
 */
public final class MetricsHttpServer implements AutoCloseable {

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(MetricsHttpServer.class);

    /** Prometheus 0.0.4 文本 exposition 的标准 Content-Type。 */
    private static final String PROMETHEUS_CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8";

    /** 注册表快照来源（构造后不变）。 */
    private final PrometheusMeterRegistry registry;
    /** 配置端口（0 表示临时端口）。 */
    private final int configuredPort;

    /** boss 线程组，start 后非空。 */
    private EventLoopGroup bossGroup;
    /** worker 线程组，start 后非空。 */
    private EventLoopGroup workerGroup;
    /** 监听 channel，未启动时为 {@code null}。 */
    private Channel serverChannel;

    /**
     * 构造（不启动）。
     *
     * @param registry 指标注册表，{@code /metrics} 的 scrape 来源
     * @param port     监听端口（0 表示由操作系统分配临时端口）
     */
    public MetricsHttpServer(PrometheusMeterRegistry registry, int port) {
        this.registry = registry;
        this.configuredPort = port;
    }

    /**
     * 绑定并启动管理端口。失败路径不残留线程组（自回收后抛出）。
     *
     * @throws IllegalStateException 绑定失败（如端口被占用）或启动被中断
     */
    public synchronized void start() {
        bossGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("openlatch-metrics-boss", true));
        workerGroup = new NioEventLoopGroup(1, new DefaultThreadFactory("openlatch-metrics", true));
        ServerBootstrap bootstrap = new ServerBootstrap()
                .group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline().addLast(new HttpServerCodec());
                        ch.pipeline().addLast(new HttpObjectAggregator(16 * 1024));
                        ch.pipeline().addLast(new MetricsHttpHandler(registry));
                    }
                });
        try {
            serverChannel = bootstrap.bind(new InetSocketAddress(configuredPort)).sync().channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            shutdownGroups();
            throw new IllegalStateException("管理端口启动被中断", e);
        } catch (Exception e) {
            // Netty sync() 以未检查方式抛出 BindException 等，统一包装（与锁端口同策略）。
            shutdownGroups();
            throw new IllegalStateException("管理端口启动失败（端口 " + configuredPort
                    + " 可能被占用）: " + e.getMessage(), e);
        }
        log.info("OpenLatch metrics endpoint started: port={}", port());
    }

    /**
     * 实际监听端口。调用者义务：仅可在 {@link #start()} 成功后调用。
     *
     * @return 监听端口（配置 0 时为操作系统分配值）
     * @throws NullPointerException 尚未启动
     */
    public int port() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    /**
     * 关停：解除监听 → 优雅回收两个线程组。幂等，可重复调用。
     */
    @Override
    public synchronized void close() {
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly();
            serverChannel = null;
        }
        shutdownGroups();
    }

    /**
     * 优雅关停 boss/worker 线程组并置空引用（幂等；等待上限同服务器关停纪律）。
     */
    private void shutdownGroups() {
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
            bossGroup = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(0, 5, TimeUnit.SECONDS).awaitUninterruptibly();
            workerGroup = null;
        }
    }

    /**
     * 两路径裁决处理器：{@code GET /metrics} → 注册表快照；
     * {@code GET /healthz} → {@code ok}；其余（含方法不符与未知路径）404。
     * 每通道一实例（非 sharable），响应写完即关闭连接。
     */
    private static final class MetricsHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        /** 注册表快照来源。 */
        private final PrometheusMeterRegistry registry;

        /**
         * 构造处理器。
         *
         * @param registry scrape 来源
         */
        MetricsHttpHandler(PrometheusMeterRegistry registry) {
            this.registry = registry;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
            String path = req.uri();
            int q = path.indexOf('?');
            if (q >= 0) {
                path = path.substring(0, q);
            }
            if (req.method().equals(io.netty.handler.codec.http.HttpMethod.GET)) {
                if ("/metrics".equals(path)) {
                    respond(ctx, HttpResponseStatus.OK, PROMETHEUS_CONTENT_TYPE, registry.scrape());
                    return;
                }
                if ("/healthz".equals(path)) {
                    respond(ctx, HttpResponseStatus.OK, "text/plain; charset=utf-8", "ok");
                    return;
                }
            }
            respond(ctx, HttpResponseStatus.NOT_FOUND, "text/plain; charset=utf-8", "not found");
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // 半包/恶意报文等异常：直接断连即可（管理端口低频短连接）。
            log.debug("metrics http error, closing: {}", cause.toString());
            ctx.close();
        }

        /**
         * 写出定长响应并关闭连接（{@code Connection: close} 语义）。
         *
         * @param ctx         通道上下文
         * @param status      响应状态
         * @param contentType 内容类型
         * @param body        响应体（UTF-8）
         */
        private static void respond(ChannelHandlerContext ctx, HttpResponseStatus status,
                                    String contentType, String body) {
            byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
            FullHttpResponse resp = new DefaultFullHttpResponse(
                    HttpVersion.HTTP_1_1, status, Unpooled.wrappedBuffer(bytes));
            resp.headers().set(HttpHeaderNames.CONTENT_TYPE, contentType);
            resp.headers().set(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
            resp.headers().set(HttpHeaderNames.CONNECTION, "close");
            ctx.writeAndFlush(resp).addListener((ChannelFuture f) -> f.channel().close());
        }
    }
}
