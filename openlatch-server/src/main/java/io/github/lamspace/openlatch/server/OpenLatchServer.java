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

import io.github.lamspace.openlatch.core.CoreEngine;
import io.github.lamspace.openlatch.core.SystemClock;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.net.ServerBootstrapFactory;
import io.github.lamspace.openlatch.server.net.ServerChannelInitializer;
import io.github.lamspace.openlatch.server.net.ServerSessionHandler;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * OpenLatch 单节点服务器入口：加载配置 → 组装锁语义核心 → 启动租约扫描调度
 * 与 Netty 监听（设计说明书 §5.2）。
 *
 * <p><b>线程模型</b>：
 * <ul>
 *   <li><b>业务逻辑在 Netty IO 线程同步执行</b>：握手、分发、锁操作均不切换
 *       线程，单连接内请求天然串行；跨连接的并发由 {@link CoreEngine} 的
 *       条目锁与并发容器保证安全；</li>
 *   <li><b>租约扫描独立单线程</b>（守护线程 {@code openlatch-lease-sweeper}）：
 *       周期调用 {@code expireDue} 与 {@code sweepNotifiedHeads}，
 *       与业务线程并发进入 {@link CoreEngine}；</li>
 *   <li><b>通知回调线程来源不定</b>：{@code AWAIT_NOTIFY} 推送可能来自
 *       业务 IO 线程（释放触发）或扫描线程（到期/清扫触发），
 *       {@link NotifyEventBridge} 对两者均安全。</li>
 * </ul>
 *
 * <p><b>生命周期</b>：构造只组装不占资源；{@link #start} 启动调度与监听
 * （失败抛出，调用方负责退出）；{@link #stop} 幂等，关停顺序见该方法。
 */
public final class OpenLatchServer {

    /** 服务器协议版本，握手时校验客户端版本一致性。 */
    public static final int PROTOCOL_VERSION = 1;

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(OpenLatchServer.class);
    /** Netty 优雅关停的安静期（毫秒），取 0 表示立即进入关停。 */
    private static final long SHUTDOWN_QUIET_MS = 0;
    /** Netty 优雅关停与调度器等待的超时（秒）。 */
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    /** 服务器配置（不可变）。 */
    private final ServerConfig config;
    /** 锁语义核心，构造时组装，生命周期与服务器相同。 */
    private final CoreEngine core;
    /** sessionId → 会话反向索引，通知推送经此路由。 */
    private final ServerSessionRegistry sessions = new ServerSessionRegistry();
    /** 全部活动连接，关停时统一关闭。 */
    private final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    /** 租约扫描调度器，启动后非空，关停后置回 {@code null}。 */
    private ScheduledExecutorService scheduler;
    /** accept 线程组（1 线程）。 */
    private EventLoopGroup bossGroup;
    /** IO 线程组。 */
    private EventLoopGroup workerGroup;
    /** 监听 channel，未启动时为 {@code null}。 */
    private Channel serverChannel;

    /**
     * 构造服务器：组装锁语义核心与通知桥，不启动任何资源。
     *
     * @param config 服务器配置
     */
    public OpenLatchServer(ServerConfig config) {
        this.config = config;
        this.core = new CoreEngine(config.toCoreConfig(), new SystemClock(), new NotifyEventBridge(sessions));
    }

    /**
     * 启动扫描调度与网络监听。端口冲突等失败抛出异常，调用方负责退出处理。
     *
     * @throws IllegalStateException 启动被中断或监听失败（如端口被占用）
     */
    public void start() {
        startScheduler();
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(config.workerThreads());
        ServerChannelInitializer initializer = new ServerChannelInitializer(
                config.idleTimeoutMs(),
                new ServerSessionHandler(core, config, sessions, new RequestDispatcher(core)),
                channels);
        ServerBootstrap bootstrap = ServerBootstrapFactory.create(bossGroup, workerGroup, initializer);
        try {
            serverChannel = bootstrap.bind(config.port()).sync().channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            stop();
            throw new IllegalStateException("启动被中断", e);
        } catch (Exception e) {
            // Netty 的 sync() 会以未检查方式抛出 BindException 等受检异常，统一包装。
            stop();
            throw new IllegalStateException("启动失败（端口 " + config.port() + " 可能被占用）: "
                    + e.getMessage(), e);
        }
        log.info("OpenLatch server started: port={}, protocolVersion={}, maxKeyLength={}, "
                        + "maxQueueDepthPerKey={}, maxInflightPerConnection={}, defaultLeaseMs={}",
                port(), PROTOCOL_VERSION, config.maxKeyLength(), config.maxQueueDepthPerKey(),
                config.maxInflightPerConnection(), config.defaultLeaseMs());
    }

    /**
     * 实际监听端口（配置端口为 0 时返回操作系统分配的端口）。
     * 调用者义务：仅可在 {@link #start()} 成功后调用——启动前
     * {@code serverChannel} 尚未创建，调用抛 {@link NullPointerException}。
     *
     * @return 监听端口
     */
    public int port() {
        return ((InetSocketAddress) serverChannel.localAddress()).getPort();
    }

    /**
     * 锁语义核心，供测试直接驱动。
     *
     * @return 核心引擎
     */
    public CoreEngine core() {
        return core;
    }

    /**
     * 服务器配置。
     *
     * @return 配置
     */
    public ServerConfig config() {
        return config;
    }

    /**
     * 会话注册表，供测试断言会话状态。
     *
     * @return 会话注册表
     */
    public ServerSessionRegistry sessions() {
        return sessions;
    }

    /**
     * 关停序列（设计说明书 §5.6）：先停租约扫描（不再产生新通知）→ 关闭全部连接
     * → 回收网络资源。幂等，可重复调用。
     */
    public synchronized void stop() {
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                scheduler.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            scheduler = null;
        }
        channels.close().awaitUninterruptibly(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        if (serverChannel != null) {
            serverChannel.close().awaitUninterruptibly();
            serverChannel = null;
        }
        if (bossGroup != null) {
            bossGroup.shutdownGracefully(SHUTDOWN_QUIET_MS, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .awaitUninterruptibly();
            bossGroup = null;
        }
        if (workerGroup != null) {
            workerGroup.shutdownGracefully(SHUTDOWN_QUIET_MS, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .awaitUninterruptibly();
            workerGroup = null;
        }
        log.info("OpenLatch server stopped");
    }

    /**
     * 启动租约扫描调度器：单守护线程（{@code openlatch-lease-sweeper}），
     * 以 {@code leaseTickIntervalMs} 为固定周期调用 {@code expireDue}
     * （回收过期租约）与 {@code sweepNotifiedHeads}（清扫超时未重发的
     * 已通知队首）。单次扫描抛出的运行时异常仅记日志，不中断后续调度。
     * 仅由 {@link #start} 调用一次。
     */
    private void startScheduler() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "openlatch-lease-sweeper");
            t.setDaemon(true);
            return t;
        });
        long tickMs = config.leaseTickIntervalMs();
        scheduler.scheduleAtFixedRate(() -> {
            try {
                core.expireDue();
                core.sweepNotifiedHeads();
            } catch (RuntimeException e) {
                log.error("lease sweep failed", e);
            }
        }, tickMs, tickMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 进程入口：加载配置、注册关停钩子并启动服务器；配置或启动失败以非零码退出。
     *
     * @param args 命令行参数（当前不使用，配置路径经系统属性 {@code openlatch.config} 传入）
     */
    public static void main(String[] args) {
        ServerConfig config;
        try {
            config = ServerConfig.load(System.getProperty(ServerConfig.CONFIG_PATH_PROPERTY));
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(1);
            return;
        }
        OpenLatchServer server = new OpenLatchServer(config);
        Runtime.getRuntime().addShutdownHook(new Thread(server::stop, "openlatch-shutdown"));
        try {
            server.start();
        } catch (RuntimeException e) {
            log.error("startup failed: {}", e.getMessage());
            System.exit(1);
        }
        // Netty 事件循环线程为非守护线程，进程保持存活直至关停。
    }
}
