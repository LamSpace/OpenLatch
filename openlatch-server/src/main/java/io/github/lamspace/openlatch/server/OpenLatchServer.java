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
import io.github.lamspace.openlatch.server.admin.AdminRequestHandler;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.metrics.MetricsHttpServer;
import io.github.lamspace.openlatch.server.metrics.ServerMetrics;
import io.github.lamspace.openlatch.server.net.ServerBootstrapFactory;
import io.github.lamspace.openlatch.server.net.ServerChannelInitializer;
import io.github.lamspace.openlatch.server.net.ServerSessionHandler;
import io.github.lamspace.openlatch.server.raft.ClusterRuntime;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.ssl.ClientAuth;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * OpenLatch 服务器入口（每个节点 = Raft 副本 + 完整接入层）：
 * 加载配置 → 组装锁语义核心（单机）或集群运行时（{@code enabled=true}，
 * 经 {@link ClusterRuntime}）→ 启动租约扫描调度与 Netty 监听。
 * {@code cluster.enabled=false} 时行为即纯单机模式（同一二进制，不做
 * 任何集群装配）。
 *
 * <p><b>线程模型</b>：
 * <ul>
 *   <li><b>单机业务逻辑在 Netty IO 线程同步执行</b>：握手、分发、锁操作均不切换
 *       线程，单连接内请求天然串行；跨连接的并发由 {@link CoreEngine} 的
 *       条目锁与并发容器保证安全；</li>
 *   <li><b>集群写路径跨线程</b>：应答于状态机应用线程完成后弹回连接
 *       EventLoop 写回，见 {@code ReplicationGateway} 类注释；</li>
 *   <li><b>租约扫描独立单线程</b>（守护线程 {@code openlatch-lease-sweeper}）：
 *       周期调用 {@code expireDue} 与 {@code sweepNotifiedHeads}，
 *       与业务线程并发进入 {@link CoreEngine}；集群模式改由
 *       {@code LeaseExpiryDriver} 驱动到期条目（不直驱引擎）；</li>
 *   <li><b>通知回调线程来源不定</b>：{@code AWAIT_NOTIFY} 推送可能来自
 *       业务 IO 线程（释放触发）或扫描线程（到期/清扫触发），
 *       {@link NotifyEventBridge} 对两者均安全；集群路径的唤醒推送在
 *       状态机应用线程投递（{@code ReplicationGateway#pushAwaitNotify}）。</li>
 * </ul>
 *
 * <p><b>生命周期</b>：构造只组装不占资源；{@link #start} 启动调度与监听
 * （集群模式先组网后开端口，失败抛出，调用方负责退出），并按
 * {@link MetricsConfig} 绑定管理端口（{@code /metrics} + {@code /healthz}）、
 * 按 {@link AdminConfig} 装配 {@code ADMIN_*} 管理观察处理器
 * （业务端口上的只读观察通道，未配置令牌即一律拒绝）；
 * {@link #stop} 幂等，关停顺序见该方法。
 */
public final class OpenLatchServer {

    /**
     * 服务器自身协议版本（握手响应 {@code server_protocol_version} 回此值）。
     * v4 起握手接受 {@value #MIN_CLIENT_PROTOCOL_VERSION}–
     * {@value #PROTOCOL_VERSION} 的客户端版本；应答信封的 {@code protocol_version}
     * 回显客户端请求版本，低版本客户端因此看到与既有阶段同形的响应。
     * 各版本专属语义（新锁类型/新消息）由接入层按会话握手版本门控。
     */
    public static final int PROTOCOL_VERSION = 4;

    /** 握手可接受的最小客户端协议版本（v1 客户端在集群模式下持续可用）。 */
    public static final int MIN_CLIENT_PROTOCOL_VERSION = 1;

    /**
     * 客户端协议版本是否在支持区间 [{@value #MIN_CLIENT_PROTOCOL_VERSION},
     * {@value #PROTOCOL_VERSION}] 内（区间外握手 MUST 拒绝并断连，不做隐式兼容）。
     *
     * @param clientVersion 握手携带的 {@code client_protocol_version}
     * @return 受支持返回 {@code true}
     */
    public static boolean isClientVersionSupported(int clientVersion) {
        return clientVersion >= MIN_CLIENT_PROTOCOL_VERSION && clientVersion <= PROTOCOL_VERSION;
    }

    /**
     * 服务端版本字符串（ADMIN_SUMMARY 的 {@code version} 来源）：优先取 jar manifest 的 {@code Implementation-Version}；
     * manifest 不可得（测试 classpath、未 repackage 的模块直接运行）回落
     * 构建常量 {@value #VERSION_FALLBACK}。
     *
     * @return 版本字符串，恒非空
     */
    public static String serverVersion() {
        return SERVER_VERSION;
    }

    /**
     * 解析 manifest 版本并在解析失败时回落——静态初始化一次，调用方无感。
     *
     * @return 版本字符串
     */
    private static String resolveServerVersion() {
        String v = OpenLatchServer.class.getPackage().getImplementationVersion();
        return v == null || v.isBlank() ? VERSION_FALLBACK : v;
    }

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(OpenLatchServer.class);
    /** Netty 优雅关停的安静期（毫秒），取 0 表示立即进入关停。 */
    private static final long SHUTDOWN_QUIET_MS = 0;
    /** Netty 优雅关停与调度器等待的超时（秒）。 */
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    /** 服务器配置（不可变）。 */
    private final ServerConfig config;
    /** 集群配置（不可变；{@code enabled=false} 即单机）。 */
    private final ClusterConfig clusterConfig;
    /** 指标配置（不可变；兼容构造重载取 {@link MetricsConfig#disabled()}）。 */
    private final MetricsConfig metricsConfig;
    /** 管理观察配置（不可变；兼容构造重载取 {@link AdminConfig#unconfigured()}）。 */
    private final AdminConfig adminConfig;
    /** 业务令牌认证配置（不可变；兼容构造重载取 {@link AuthConfig#unconfigured()} 兼容守卫）。 */
    private final AuthConfig authConfig;
    /** 传输层 TLS 配置（不可变；兼容构造重载取 {@link TlsConfig#disabled()} 明文栈）。 */
    private final TlsConfig tlsConfig;
    /** 指标词表门面：双路径埋点数据落点，{@code /metrics} 的 scrape 来源。 */
    private final ServerMetrics metrics = new ServerMetrics();
    /** 管理端口 HTTP 服务（{@code enabled} 时于 {@link #start} 绑定，此前与关停后为 {@code null}）。 */
    private volatile MetricsHttpServer metricsHttp;
    /**
     * 锁语义核心：单机模式构造时组装，生命周期与服务器相同；集群模式为
     * {@code null}（引擎状态唯一经 {@link ClusterRuntime} 的复制路径迁移，
     * 避免双引擎持有者视图）。
     */
    private final CoreEngine core;
    /** 集群运行时（{@code enabled=true} 时于 {@link #start} 内装配并启动，此前为 {@code null}）。 */
    private volatile ClusterRuntime cluster;
    /** sessionId → 会话反向索引，通知推送经此路由。 */
    private final ServerSessionRegistry sessions = new ServerSessionRegistry();
    /** 全部活动连接，关停时统一关闭。 */
    private final ChannelGroup channels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    /**
     * 版本回落常量：manifest 的 {@code Implementation-Version} 不可得时
     * （测试 classpath / 非 repackage 形态）ADMIN_SUMMARY 报此值，与根
     * pom 项目版本同步维护。
     */
    private static final String VERSION_FALLBACK = "1.0.0-dev";
    /** 服务端版本（manifest Implementation-Version，缺失回落 {@value #VERSION_FALLBACK}）。 */
    private static final String SERVER_VERSION = resolveServerVersion();

    /** 租约扫描调度器，启动后非空，关停后置回 {@code null}。 */
    private ScheduledExecutorService scheduler;
    /** 启动成功时刻（epoch 毫秒，0=未启动）——ADMIN_SUMMARY 运行时长基准。 */
    private volatile long startedAtMs;
    /** accept 线程组（1 线程）。 */
    private EventLoopGroup bossGroup;
    /** IO 线程组。 */
    private EventLoopGroup workerGroup;
    /** 监听 channel，未启动时为 {@code null}。 */
    private Channel serverChannel;

    /**
     * 构造服务器（单机模式）：组装锁语义核心与通知桥，不启动任何资源；
     * 管理端口取 {@link MetricsConfig#disabled()}（库内嵌不抢占固定端口，
     * 文件加载路径的默认开启语义见 {@link MetricsConfig#load(String)}）。
     *
     * @param config 服务器配置
     */
    public OpenLatchServer(ServerConfig config) {
        this(config, ClusterConfig.disabled());
    }

    /**
     * 构造服务器：{@code clusterConfig.enabled=false} 时与单机构造完全一致
     * （同一二进制回退保证）；{@code true} 时不组装
     * 单机核心（引擎仅在复制路径内），集群组件于 {@link #start()} 装配。
     * 管理端口取 {@link MetricsConfig#disabled()}。
     *
     * @param config        服务器配置
     * @param clusterConfig 集群配置（已校验）
     */
    public OpenLatchServer(ServerConfig config, ClusterConfig clusterConfig) {
        this(config, clusterConfig, MetricsConfig.disabled());
    }

    /**
     * 构造服务器（全装配形态，不含管理观察配置）：ADMIN 一律拒绝
     * （{@link AdminConfig#unconfigured()} 语义）。
     *
     * @param config        服务器配置
     * @param clusterConfig 集群配置（已校验）
     * @param metricsConfig 指标配置（已校验）
     */
    public OpenLatchServer(ServerConfig config, ClusterConfig clusterConfig,
                           MetricsConfig metricsConfig) {
        this(config, clusterConfig, metricsConfig, AdminConfig.unconfigured());
    }

    /**
     * 构造服务器（全配置形态）：指标启停与管理端口由 {@code metricsConfig}
     * 决定（{@code enabled=true} 时 {@link #start()} 绑定，端口冲突快速失败）；
     * 管理观察通道由 {@code adminConfig} 决定（未配置令牌即
     * 拒绝一切 {@code ADMIN_*}，配置后 {@link #start()} 装配管理处理器）。
     *
     * @param config        服务器配置
     * @param clusterConfig 集群配置（已校验）
     * @param metricsConfig 指标配置（已校验）
     * @param adminConfig   管理观察配置（已归一）
     */
    public OpenLatchServer(ServerConfig config, ClusterConfig clusterConfig,
                           MetricsConfig metricsConfig, AdminConfig adminConfig) {
        this(config, clusterConfig, metricsConfig, adminConfig, TlsConfig.disabled());
    }

    /**
     * 构造服务器（全配置形态，不含业务认证）：认证取 {@link AuthConfig#unconfigured()}
     * （兼容守卫）。
     *
     * @param config        服务器配置
     * @param clusterConfig 集群配置（已校验）
     * @param metricsConfig 指标配置（已校验）
     * @param adminConfig   管理观察配置（已归一）
     * @param tlsConfig     TLS 传输配置（已结构性校验；文件可读性于
     *                      {@link #start()} 校验）
     */
    public OpenLatchServer(ServerConfig config, ClusterConfig clusterConfig,
                           MetricsConfig metricsConfig, AdminConfig adminConfig,
                           TlsConfig tlsConfig) {
        this(config, clusterConfig, metricsConfig, adminConfig, AuthConfig.unconfigured(), tlsConfig);
    }

    /**
     * 构造服务器（全配置形态）：业务认证由 {@code authConfig} 决定
     * （{@code enabled=true} 时 HELLO 令牌命中校验，失败统一拒并断连；关闭默认
     * 维持"非空即拒"兼容守卫）；TLS 由 {@code tlsConfig} 决定——
     * {@code enabled=true} 时 {@link #start()} 构造 {@link SslContext}（PEM
     * cert/key，mTLS 时 trust-store + 要求客户端证书）并在监听 pipeline 首位
     * 装配 {@code SslHandler}（明文连接拒绝、握手超时 5s 断开）；
     * 两者默认关闭即明文无认证，行为与现状逐字节一致。
     *
     * @param config        服务器配置
     * @param clusterConfig 集群配置（已校验）
     * @param metricsConfig 指标配置（已校验）
     * @param adminConfig   管理观察配置（已归一）
     * @param authConfig    业务令牌认证配置（已结构性校验）
     * @param tlsConfig     TLS 传输配置（已结构性校验；文件可读性于
     *                      {@link #start()} 校验）
     */
    public OpenLatchServer(ServerConfig config, ClusterConfig clusterConfig,
                           MetricsConfig metricsConfig, AdminConfig adminConfig,
                           AuthConfig authConfig, TlsConfig tlsConfig) {
        this.config = config;
        this.clusterConfig = clusterConfig;
        this.metricsConfig = metricsConfig;
        this.adminConfig = adminConfig;
        this.authConfig = authConfig;
        this.tlsConfig = tlsConfig;
        this.core = clusterConfig.enabled()
                ? null
                : new CoreEngine(config.toCoreConfig(), new SystemClock(), new NotifyEventBridge(sessions));
        if (this.core != null) {
            // 单机 gauge 数据源：统计观察面与会话注册表的弱一致回调读数。
            this.metrics.bindStandaloneGauges(this.core, this.sessions);
        }
    }

    /**
     * 启动扫描调度与网络监听。端口冲突等失败抛出异常，调用方负责退出处理。
     *
     * @throws IllegalStateException 启动被中断或监听失败（如端口被占用）
     */
    public void start() {
        if (clusterConfig.enabled()) {
            // 先完成 Raft 组网与状态机初始化，后开放客户端接入端口。
            try {
                cluster = ClusterRuntime.create(clusterConfig, config, sessions, metrics);
            } catch (IOException e) {
                throw new IllegalStateException("集群装配失败: " + e.getMessage(), e);
            }
        } else {
            startScheduler();
        }
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup(config.workerThreads());
        // 传输层 TLS：enabled 时构造 SslContext——PEM 文件不可读
        // 或不可解析即启动快速失败（stop 回收已启动的集群/调度器与线程组，
        // 不进入半启动，对齐"端口占用启动失败"语义）。
        SslContext sslContext = null;
        if (tlsConfig.enabled()) {
            try {
                sslContext = serverSslContext();
            } catch (IllegalStateException e) {
                stop();
                throw e;
            }
        }
        // 管理观察处理器：数据源按装配形态二选一，未配置
        // 令牌时同样注入——由处理器自身执行"一律拒绝"的安全默认。
        AdminRequestHandler adminHandler =
                new AdminRequestHandler(adminConfig, core, cluster, sessions, this::uptimeMs);
        ServerSessionHandler handler = clusterConfig.enabled()
                ? new ServerSessionHandler(null, config, sessions, null, cluster, adminHandler, authConfig)
                : new ServerSessionHandler(core, config, sessions,
                        new RequestDispatcher(core, metrics), null, adminHandler, authConfig);
        ServerChannelInitializer initializer = new ServerChannelInitializer(
                config.idleTimeoutMs(), handler, channels, sslContext);
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
        if (metricsConfig.enabled()) {
            // 管理端口冲突与锁端口同策略——
            // 整体启动失败退出，不进入半启动（stop 回收已绑定的锁端口与线程组）。
            metricsHttp = new MetricsHttpServer(metrics.registry(), metricsConfig.port());
            try {
                metricsHttp.start();
            } catch (RuntimeException e) {
                stop();
                throw e;
            }
        }
        startedAtMs = System.currentTimeMillis();
        log.info("OpenLatch server started: port={}, protocolVersion={}, maxKeyLength={}, "
                        + "maxQueueDepthPerKey={}, maxInflightPerConnection={}, defaultLeaseMs={}, "
                        + "clusterEnabled={}, clusterNodeId={}, metricsPort={}, adminEnabled={}, "
                        + "authEnabled={}, tlsEnabled={}, mTls={}",
                port(), PROTOCOL_VERSION, config.maxKeyLength(), config.maxQueueDepthPerKey(),
                config.maxInflightPerConnection(), config.defaultLeaseMs(),
                clusterConfig.enabled(), clusterConfig.nodeId(), metricsPort(),
                adminConfig.isConfigured(), authConfig.isEnabled(), tlsConfig.enabled(),
                tlsConfig.requireClientCert());
    }

    /**
     * 由 TLS 配置构造服务端 {@link SslContext}：
     * PEM cert/key 经 {@code SslContextBuilder.forServer} 直供；mTLS
     * （{@code require-client-cert=true}）时以 trust-store 为可信任 CA 并
     * 要求客户端证书。文件不可读/不可解析抛 {@link IllegalStateException}
     * （启动快速失败，由调用方回收资源）。
     *
     * @return 已构建的 SslContext
     * @throws IllegalStateException cert/key/trust-store 文件不可读或不可解析
     */
    private SslContext serverSslContext() {
        try {
            SslContextBuilder builder = SslContextBuilder.forServer(
                    Path.of(tlsConfig.cert()).toFile(), Path.of(tlsConfig.key()).toFile());
            if (tlsConfig.requireClientCert()) {
                builder.trustManager(Path.of(tlsConfig.trustStore()).toFile())
                        .clientAuth(ClientAuth.REQUIRE);
            }
            return builder.build();
        } catch (IOException | RuntimeException e) {
            // SslContextBuilder 对不可读/不可解析的 PEM 文件抛 SSLException（IOException）
            // 或 IllegalArgumentException（keyManager/trustManager 解析失败）——统一
            // 包装为启动失败的 IllegalStateException（快速失败）。
            throw new IllegalStateException(
                    "TLS 配置加载失败（cert/key/trust-store 文件不可读或不可解析）: "
                            + e.getMessage(), e);
        }
    }

    /**
     * 管理端口实际监听端口。未启用或尚未 {@link #start()}
     * 时返回 {@code -1}（可观测哨兵，供日志与测试判定）。
     *
     * @return 管理端口；关闭或未启动为 {@code -1}
     */
    public int metricsPort() {
        MetricsHttpServer http = metricsHttp;
        return http == null ? -1 : http.port();
    }

    /**
     * 自启动以来的运行时长（毫秒，ADMIN_SUMMARY 的 {@code uptime_ms}
     * 来源）。尚未 {@link #start()} 或已 {@link #stop()} 时为 0。
     *
     * @return 运行时长（毫秒）
     */
    public long uptimeMs() {
        long at = startedAtMs;
        return at == 0 ? 0 : System.currentTimeMillis() - at;
    }

    /**
     * 集群运行时（仅 {@code enabled=true} 且 {@link #start()} 后可得）。
     *
     * @return 运行时，单机模式或未启动为 {@code null}
     */
    public ClusterRuntime cluster() {
        return cluster;
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
     * 锁语义核心，供测试直接驱动。集群模式不组装单机核心（引擎状态唯一
     * 经复制路径迁移），此时返回 {@code null}，测试请改用
     * {@link #cluster()} 的 {@code ClusterRuntime#core()}。
     *
     * @return 核心引擎；集群模式为 {@code null}
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
     * 关停序列：先解除管理端口监听 → 停租约扫描（不再产生新通知）→ 关闭全部
     * 连接 → 回收网络资源 → 集群模式逆序关停运行时（在途以可重试错误终结、
     * 探针/扫描线程停止、Raft 服务关闭，无悬挂请求）。幂等，可重复调用。
     */
    public synchronized void stop() {
        startedAtMs = 0;
        if (metricsHttp != null) {
            metricsHttp.close();
            metricsHttp = null;
        }
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
        if (cluster != null) {
            cluster.close();
            cluster = null;
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
                // 到期计数走扫描返回值（expireDue 即"本轮实际释放数"，零 core 侵入）。
                metrics.recordLeaseExpired(core.expireDue());
                core.sweepNotifiedHeads();
            } catch (RuntimeException e) {
                log.error("lease sweep failed", e);
            }
        }, tickMs, tickMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 服务端指标门面（埋点词表与注册表宿主；集群装配经
     * {@link ClusterRuntime} 复用同一实例，管理端点从其注册表 scrape）。
     *
     * @return 指标门面
     */
    public ServerMetrics metrics() {
        return metrics;
    }

    /**
     * 进程入口：加载配置、注册关停钩子并启动服务器；配置或启动失败以非零码退出。
     *
     * @param args 命令行参数（当前不使用，配置路径经系统属性 {@code openlatch.config} 传入）
     */
    public static void main(String[] args) {
        ServerConfig config;
        ClusterConfig clusterConfig;
        MetricsConfig metricsConfig;
        AdminConfig adminConfig;
        AuthConfig authConfig;
        TlsConfig tlsConfig;
        try {
            String path = System.getProperty(ServerConfig.CONFIG_PATH_PROPERTY);
            config = ServerConfig.load(path);
            clusterConfig = ClusterConfig.load(path);
            metricsConfig = MetricsConfig.load(path);
            adminConfig = AdminConfig.load(path);
            authConfig = AuthConfig.load(path);
            tlsConfig = TlsConfig.load(path);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.exit(1);
            return;
        }
        OpenLatchServer server =
                new OpenLatchServer(config, clusterConfig, metricsConfig, adminConfig,
                        authConfig, tlsConfig);
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
