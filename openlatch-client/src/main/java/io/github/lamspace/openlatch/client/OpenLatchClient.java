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

package io.github.lamspace.openlatch.client;

import io.github.lamspace.openlatch.client.internal.AwaitTracker;
import io.github.lamspace.openlatch.client.internal.ClientConfig;
import io.github.lamspace.openlatch.client.internal.ConnectionManager;
import io.github.lamspace.openlatch.client.internal.HeldLockRegistry;
import io.github.lamspace.openlatch.client.internal.SeedDiscovery;
import io.github.lamspace.openlatch.client.internal.RequestMultiplexer;
import io.github.lamspace.openlatch.client.internal.SessionContext;
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.EventExecutorGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * OpenLatch 客户端入口（详设 §6.1/§6.3）。
 *
 * <p><b>职责</b>：以长连接访问锁服务，对外提供异步获取/释放内核与
 * JUC 风格同步锁包装；对内维护请求多路复用、等待跟踪、看门狗续租与断连重连。
 * 锁语义裁决（是否授予、是否可重入）全部由服务端完成，客户端仅做本地簿记。
 *
 * <p><b>连接车道（S3，design D6）</b>：稳态单连接（home 即 Leader，或单机
 * 服务）。集群形态下 home 握手提示或 {@code NOT_LEADER} 重定向驱动改道：
 * 存在 {@link AcquireLane} 获取车道时新获取与其等待闭环在车道上承载，
 * home 车道保留为存量锁的续租/释放出口（服务端转发车道送达当值 Leader），
 * 存量清零后车道收口退役；连续 {@value #FORCE_DISCOVERY_THRESHOLD} 次
 * {@code NOT_LEADER} 触发种子扇出强制发现（{@link SeedDiscovery}）。
 * 发现/改道全程受请求超时与等待总预算约束，预算耗尽快速失败。
 * 提示为 {@code -1}（选举空窗）时同会话同 {@code requestId} 原地退避重发，
 * 改连产生的新会话使用新 {@code requestId} 空间（幂等口径，详设 §6.3）。
 *
 * <p><b>线程模型</b>（详设 §6.8）：全部网络读写在客户端 EventLoop 线程；
 * 各类超时由共享 {@link HashedWheelTimer} 驱动；锁丢失回调在专用单线程
 * 执行器上调用。异步接口返回的 future 在网络/定时器线程上完成，
 * <b>用户链接的回调不得执行阻塞操作</b>，需要阻塞处理时应切换至调用方自己的执行器。
 *
 * <p><b>生命周期</b>：经 {@link #builder()} 构建，构建时创建后台资源并发起
 * 首次异步连接；{@link #shutdown()} 先对本地登记持锁尽力释放（单条目至多
 * 一个 requestTimeout，失败以服务端租约到期兜底），再幂等关停全部资源并
 * 进入终态，终态后不再受理请求、不再重连。实现 {@link AutoCloseable}，
 * {@code close()} 等价于 {@code shutdown()}。
 */
public final class OpenLatchClient implements AutoCloseable {

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(OpenLatchClient.class);
    /** 共享定时器的刻度间隔（毫秒）：满足毫秒级超时的精度要求即可。 */
    private static final long TIMER_TICK_MS = 100;
    /** Netty EventLoopGroup 优雅关停的安静期（毫秒），取 0 表示立即进入关停。 */
    private static final long SHUTDOWN_QUIET_MS = 0;
    /** Netty EventLoopGroup 优雅关停的等待上限（秒）。 */
    private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;

    /** 客户端配置（不可变）。 */
    private final ClientConfig config;
    /** 网络 IO 线程组：全部读写与连接状态迁移在此执行。 */
    private final NioEventLoopGroup eventLoopGroup;
    /** 共享定时器：每请求超时、等待总超时、看门狗周期、失锁时刻定时均挂于此。 */
    private final HashedWheelTimer timer;
    /** 锁丢失回调执行器：单线程，用户回调异常在此被隔离捕获。 */
    private final ExecutorService lockLostExecutor;
    /** 请求多路复用：home 车道全部出站请求与入站响应的收口（详设 §6.4）。 */
    private final RequestMultiplexer multiplexer;
    /** home 车道的连接与重连状态机（测试注入与故障裁决入口，Phase 1 语义保持）。 */
    private final ConnectionManager connectionManager;
    /** 等待跟踪：排队挂起、通知重发、孤儿授予补偿。 */
    private final AwaitTracker awaitTracker;
    /** 本地持锁簿记：只记归属不记重入计数（详设 §6.3）。 */
    private final HeldLockRegistry heldLockRegistry = new HeldLockRegistry();
    /**
     * 获取车道（Leader 车道，design D6）：{@code null} 即稳态单连接——home 即
     * Leader（或单机）。Leader 改连时按需建/换指向；新获取与等待走此车道，
     * home 车道降级为存量锁的转发出口。
     */
    private volatile AcquireLane acquireLane;
    /** 会话 id → 归属车道路由表：存量锁续租/释放回其获取车道（design D6）。 */
    private final java.util.concurrent.ConcurrentMap<Long, LaneRef> lanesBySession =
            new java.util.concurrent.ConcurrentHashMap<>();
    /** 待退车道池：让位后仍持有存量锁出口的旧车道，清零时收口关闭。 */
    private final java.util.List<AcquireLane> spareLanes =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    /** 连续 NOT_LEADER 计数：达阈值触发种子扇出强制发现（详设 §6.3）。 */
    private final java.util.concurrent.atomic.AtomicInteger notLeaderStreak =
            new java.util.concurrent.atomic.AtomicInteger();
    /** 强制发现阈值（详设 §6.3"连续 N 次（默认 3）"，常量取向不配置化）。 */
    private static final int FORCE_DISCOVERY_THRESHOLD = 3;
    /** 选举空窗（hint=-1）原地重发的退避步长（毫秒）。 */
    private static final long GAP_RETRY_BACKOFF_MS = 300;
    /** 看门狗：持锁期间的自动续租与失锁判定（详设 §6.6）。 */
    private final io.github.lamspace.openlatch.client.internal.Watchdog watchdog;
    /** 全局锁丢失监听器列表。 */
    private final java.util.List<LockLostListener> globalLockLostListeners =
            new java.util.concurrent.CopyOnWriteArrayList<>();
    /** 锁键维度的锁丢失监听器表。 */
    private final java.util.concurrent.ConcurrentMap<String, java.util.List<LockLostListener>>
            keyLockLostListeners = new ConcurrentHashMap<>();
    /** 关停标志：置位后拒绝新请求、停止重连。 */
    private volatile boolean closed;

    /**
     * 以构建好的配置创建客户端并启动后台资源。仅由 {@link Builder#build()} 调用。
     *
     * @param config 已校验的客户端配置
     */
    private OpenLatchClient(ClientConfig config) {
        this.config = config;
        this.eventLoopGroup = new NioEventLoopGroup(config.workerThreads(), r -> {
            Thread t = new Thread(r, "openlatch-client-io");
            t.setDaemon(true);
            return t;
        });
        this.timer = new HashedWheelTimer(r -> {
            Thread t = new Thread(r, "openlatch-client-timer");
            t.setDaemon(true);
            return t;
        }, TIMER_TICK_MS, TimeUnit.MILLISECONDS);
        this.lockLostExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "openlatch-client-lock-lost");
            t.setDaemon(true);
            return t;
        });
        this.connectionManager = new ConnectionManager(config, eventLoopGroup, timer);
        this.multiplexer = new RequestMultiplexer(timer, connectionManager::activeChannel,
                connectionManager::session);
        this.awaitTracker = new AwaitTracker(timer, multiplexer,
                config.requestTimeout().toMillis(),
                (spec, grant) -> registerHeld(homeSessionId, spec, grant));
        this.awaitTracker.setNotLeaderHandler(req -> handleNotLeader(req, null));
        this.watchdog = new io.github.lamspace.openlatch.client.internal.Watchdog(
                timer, this::muxForSession, this::laneActive,
                heldLockRegistry, config.requestTimeout().toMillis(),
                (entry, cause) -> fireLockLost(entry.key(), cause));
        this.connectionManager.bind(multiplexer);
        this.connectionManager.setDisconnectHandler(cause ->
                onLaneDisconnected(homeSessionId, multiplexer, awaitTracker));
        this.connectionManager.setAwaitNotifySink(awaitTracker::onNotify);
        this.connectionManager.setActiveListener(this::onHomeActive);
        this.connectionManager.setHelloListener(this::onHomeHello);
        this.multiplexer.setOrphanSink(awaitTracker::onOrphanResponse);
        // 构建即发起首次连接（异步）：连接失败自动退避重连并轮询种子。
        this.connectionManager.connectAsync();
    }

    // ==================== S3：Leader 发现与故障转移（design D6） ====================

    /**
     * 获取车道：指向当值 Leader 的第二连接（详设 §6.3，design D6）。
     * 新获取与其等待闭环在此承载；home 车道在 Leader 切换后仅保留
     * 存量锁的转发出口职责。钉住目标不轮询种子——目标失效由重发现重建车道。
     */
    private final class AcquireLane {
        /** 目标主机。 */
        private final String host;
        /** 目标端口。 */
        private final int port;
        /** 连接状态机。 */
        private final ConnectionManager cm;
        /** 出站收口。 */
        private final RequestMultiplexer mux;
        /** 等待跟踪（本车道会话的队列）。 */
        private final AwaitTracker awaits;
        /** 当前 ACTIVE 会话 id（握手完成前为 0）。 */
        private volatile long sessionId;

        /**
         * 构造并接线获取车道（不发起连接，连接由 retarget 流程触发）。
         *
         * @param target Leader 接入地址
         */
        private AcquireLane(ClientConfig.SeedAddress target) {
            this.host = target.host();
            this.port = target.port();
            this.cm = new ConnectionManager(config, eventLoopGroup, timer, host, port, false);
            this.mux = new RequestMultiplexer(timer, cm::activeChannel, cm::session);
            this.awaits = new AwaitTracker(timer, mux, config.requestTimeout().toMillis(),
                    this::onAcquiredOnLane);
            this.awaits.setNotLeaderHandler(req -> handleNotLeader(req, this));
            this.cm.bind(mux);
            this.cm.setDisconnectHandler(cause -> onLaneDisconnected(sessionId, mux, awaits));
            this.cm.setAwaitNotifySink(awaits::onNotify);
            this.cm.setActiveListener(this::onActive);
            this.cm.setHelloListener(this::onHello);
            this.mux.setOrphanSink(awaits::onOrphanResponse);
        }

        /** 车道激活：登记路由并裁决本车道旧会话残留持锁（重连换会话）。 */
        private void onActive() {
            SessionContext ctx = cm.session();
            long prev = sessionId;
            sessionId = ctx == null ? 0 : ctx.sessionId();
            if (sessionId != 0) {
                lanesBySession.put(sessionId, new LaneRef(mux, cm));
            }
            if (prev != 0 && prev != sessionId) {
                for (HeldLockRegistry.HeldEntry entry : heldLockRegistry.entries()) {
                    if (entry.sessionId() == prev) {
                        loseEntry(entry, new LockLostException(
                                "lock '" + entry.key() + "' lost: old session cleaned by server"));
                    }
                }
            }
        }

        /**
         * 握手提示回调：Leader 可能又易主，跟随提示继续改道。
         *
         * @param hello 握手响应（含 leader 提示字段）
         */
        private void onHello(HelloResponse hello) {
            followLeaderHint(hello);
        }

        /**
         * 本车道会话上的授予登记。
         *
         * @param spec  获取参数
         * @param grant 授予结果
         */
        private void onAcquiredOnLane(AcquireSpec spec, LockGrant grant) {
            registerHeld(sessionId, spec, grant);
        }

    }

    /**
     * 车道出站引用（会话 → 多路复用器/连接）。
     *
     * @param mux 该车道多路复用器
     * @param cm  该车道连接状态机
     */
    private record LaneRef(RequestMultiplexer mux, ConnectionManager cm) {
    }

    /** home 车道当前会话 id（重连换会话后据以裁决旧会话持锁失效）。 */
    private volatile long homeSessionId;

    /**
     * home 车道激活（首连与每次重连）：登记会话路由；重连换会话时旧 home
     * 会话已被服务端清理，其持锁必然失效（详设 §6.2 断连裁决）。仅裁决
     * home 会话的锁，不触碰获取车道锁（后者由本车道会话独立裁决）。
     */
    private void onHomeActive() {
        SessionContext ctx = connectionManager.session();
        long newSid = ctx == null ? 0 : ctx.sessionId();
        long prev = homeSessionId;
        homeSessionId = newSid;
        if (newSid != 0) {
            lanesBySession.put(newSid, new LaneRef(multiplexer, connectionManager));
        }
        if (prev != 0 && prev != newSid) {
            for (HeldLockRegistry.HeldEntry entry : heldLockRegistry.entries()) {
                if (entry.sessionId() == prev) {
                    loseEntry(entry, new LockLostException(
                            "lock '" + entry.key() + "' lost: old session cleaned by server"));
                }
            }
        }
    }

    /**
     * home 车道握手：按 v2 提示做启动直连发现（详设 §6.3）。
     *
     * @param hello 握手响应
     */
    private void onHomeHello(HelloResponse hello) {
        followLeaderHint(hello);
    }

    /**
     * 依握手提示调整获取车道：提示指向 home 当前地址或已在车道则无动作；
     * 指向 home（回主）则车道退役、等待迁回 home；指向第三节点则建/换车道。
     *
     * @param hello 握手响应（leaderHint&le;0 或地址为空时不做任何改道）
     */
    private void followLeaderHint(HelloResponse hello) {
        if (closed || hello.getLeaderHint() <= 0 || hello.getLeaderAddress().isEmpty()) {
            return; // 单机/无提示能力/地址未知：由 NOT_LEADER 兜底路径处理
        }
        ClientConfig.SeedAddress target = SeedDiscovery.parse(hello.getLeaderAddress());
        if (target == null) {
            return;
        }
        boolean targetIsHome = target.host().equals(connectionManager.targetHost())
                && target.port() == connectionManager.targetPort();
        AcquireLane lane = acquireLane;
        if (targetIsHome) {
            if (lane != null) {
                demoteToHome(lane);
            }
            return;
        }
        if (lane != null && target.host().equals(lane.host) && target.port() == lane.port) {
            return; // 已指向该 Leader
        }
        retargetAcquireLane(target, null);
    }

    /**
     * 提示所指即 home：等待项迁回 home 跟踪器，第二车道退役。
     *
     * @param lane 让位的获取车道
     */
    private void demoteToHome(AcquireLane lane) {
        acquireLane = null;
        migrateWaitsTo(lane.awaits, awaitTracker, connectionManager);
        maybeRetire(lane);
    }

    /**
     * 建立/替换获取车道；{@code req} 非空时改道完成后以新会话重放该等待。
     *
     * @param target 目标 Leader 地址
     * @param req    待重放的接管请求（可空：纯换道不重放）
     */
    private void retargetAcquireLane(ClientConfig.SeedAddress target,
                                     AwaitTracker.NotLeaderRequest req) {
        AcquireLane next = new AcquireLane(target);
        // connectAsync 对不可达目标永不完成（钉住重连），必须自设边界：
        // 有界窗口内未 ACTIVE 即关道并降级到种子发现（design D3——陈旧/不可达
        // 提示不得导致无限改连，落入种子轮询兜底）。
        java.util.concurrent.atomic.AtomicBoolean settled =
                new java.util.concurrent.atomic.AtomicBoolean();
        // 改道建连预算封顶 1.5s：不可达目标的 TCP 拒绝是毫秒级，预算只用于
        // 容忍慢而活的建连；超预算即降级种子发现——不可达提示绝不许吃掉
        // 等待总预算的大头（否则 crash failover 端到端恢复被拖过判定线）。
        long remainingMs = req == null ? config.requestTimeout().toMillis()
                : (req.remainingMs() < 0 ? config.requestTimeout().toMillis() : req.remainingMs());
        long connectBudgetMs = Math.min(1_500, Math.max(500, remainingMs / 4));
        timer.newTimeout(t -> {
            if (settled.compareAndSet(false, true)) {
                next.cm.shutdown();
                if (req != null) {
                    forceDiscover(req);
                }
            }
        }, connectBudgetMs, TimeUnit.MILLISECONDS);
        next.cm.connectAsync().whenComplete((v, err) -> {
            if (closed || err != null || !next.cm.isActive()) {
                if (settled.compareAndSet(false, true)) {
                    next.cm.shutdown();
                    if (req != null) {
                        forceDiscover(req);
                    }
                }
                return;
            }
            if (!settled.compareAndSet(false, true)) {
                next.cm.shutdown(); // 已被超时降级接管，丢弃本次结果
                return;
            }
            AcquireLane prev = acquireLane;
            acquireLane = next;
            if (prev != null) {
                migrateWaitsTo(prev.awaits, next.awaits, next.cm);
                maybeRetire(prev);
            }
            if (req != null) {
                replayOnLane(next, req);
            }
        });
    }

    /**
     * 把来源跟踪器的挂起等待迁移到目标车道（重放换新 requestId，位次重置）。
     *
     * @param from 来源跟踪器（其挂起项被摘取）
     * @param to   目标跟踪器（迁移项重新登记）
     * @param toCm 目标车道连接（新会话 requestId 分配来源）
     */
    private void migrateWaitsTo(AwaitTracker from, AwaitTracker to, ConnectionManager toCm) {
        for (AwaitTracker.PendingWait pw : from.drainPending()) {
            SessionContext s = toCm.session();
            long budget = pw.remainingMs() < 0 ? config.requestTimeout().toMillis()
                    : pw.remainingMs();
            if (s == null || budget <= 0) {
                pw.userFuture().completeExceptionally(
                        new ServerUnavailableException("leader migrated, wait budget exhausted"));
                continue;
            }
            Envelope env = pw.envelope().toBuilder().setRequestId(s.nextRequestId()).build();
            to.startAcquire(env.getRequestId(), env, pw.spec(), pw.userFuture(), budget);
        }
    }

    /**
     * NOT_LEADER 接管（详设 §6.3）：按提示改道重放 / 空窗原地退避 / 阈值强制发现。
     *
     * @param req    接管上下文（含提示与剩余预算）
     * @param origin 收到拒绝的获取车道；{@code null} 表示 home 车道
     * @return {@code true} 表示接管成功（等待由重放链延续终态）；
     *         {@code false} 表示放弃接管（跟踪器按错误码使等待失败）
     */
    private boolean handleNotLeader(AwaitTracker.NotLeaderRequest req, AcquireLane origin) {
        if (closed || req.remainingMs() == 0) {
            return false; // 预算耗尽：按错误码失败（快速失败语义）
        }
        if (req.leaderNodeId() > 0 && !req.leaderAddress().isEmpty()) {
            notLeaderStreak.set(0);
            ClientConfig.SeedAddress target = SeedDiscovery.parse(req.leaderAddress());
            if (target == null) {
                return false;
            }
            boolean targetIsHome = target.host().equals(connectionManager.targetHost())
                    && target.port() == connectionManager.targetPort();
            if (targetIsHome) {
                // 提示指回 home：等待在 home 跟踪器上以同 requestId 重发（同会话幂等）
                AwaitTracker tracker = origin == null ? awaitTracker : origin.awaits;
                if (origin == null) {
                    inPlaceRetry(tracker, req);
                } else {
                    demoteToHome(origin);
                    replayOnHome(req);
                }
                return true;
            }
            AcquireLane lane = acquireLane;
            if (lane != null && target.host().equals(lane.host) && target.port() == lane.port
                    && lane.cm.isActive()) {
                replayOnLane(lane, req); // 车道已在正确 Leader：新会话重放
            } else {
                retargetAcquireLane(target, req);
            }
            return true;
        }
        int streak = notLeaderStreak.incrementAndGet();
        if (streak >= FORCE_DISCOVERY_THRESHOLD) {
            notLeaderStreak.set(0);
            forceDiscover(req);
            return true;
        }
        if (req.leaderNodeId() > 0) {
            forceDiscover(req); // 提示有主但无地址：种子扇出自报兜底（design D4）
            return true;
        }
        // hint = -1（选举空窗）：同车道同 requestId 退避原地重发
        inPlaceRetry(origin == null ? awaitTracker : origin.awaits, req);
        return true;
    }

    /**
     * 选举空窗原地重发：同会话同 requestId 幂等复用，退避一步后在剩余预算内重投。
     *
     * @param tracker 收到拒绝的等待跟踪器（同车道重发）
     * @param req     接管上下文
     */
    private void inPlaceRetry(AwaitTracker tracker, AwaitTracker.NotLeaderRequest req) {
        long budget = req.remainingMs() < 0 ? config.requestTimeout().toMillis()
                : req.remainingMs();
        if (budget <= GAP_RETRY_BACKOFF_MS) {
            failReplay(req, new OpenLatchException(StatusCode.NOT_LEADER,
                    "no leader available within budget (election window)"));
            return;
        }
        timer.newTimeout(t -> {
            // 同会话原地重发：本路径仅在连接存活、会话未变时抵达（换会话已由
            // 该车道的断连 failAll 处理），复用原 requestId 保服务端幂等。
            SessionContext s = trackerSession(tracker);
            if (closed) {
                failReplay(req, new ServerUnavailableException("client closed"));
                return;
            }
            if (s == null) {
                failReplay(req, new ServerUnavailableException("connection lost during backoff"));
                return;
            }
            tracker.startAcquire(req.requestId(), req.envelope(), req.spec(),
                    req.userFuture(), budget - GAP_RETRY_BACKOFF_MS);
        }, GAP_RETRY_BACKOFF_MS, TimeUnit.MILLISECONDS);
    }

    /**
     * 提示所指即 home 时的重放（同会话同 requestId，幂等复用）。
     *
     * @param req 接管上下文
     */
    private void replayOnHome(AwaitTracker.NotLeaderRequest req) {
        SessionContext s = connectionManager.session();
        long budget = req.remainingMs() < 0 ? config.requestTimeout().toMillis()
                : req.remainingMs();
        if (s == null || budget <= 0) {
            failReplay(req, new ServerUnavailableException("home session unavailable"));
            return;
        }
        awaitTracker.startAcquire(req.requestId(), req.envelope(), req.spec(),
                req.userFuture(), budget);
    }

    /**
     * 获取车道上的新会话重放（改连即新 requestId 空间，详设 §6.3 幂等口径）。
     *
     * @param lane 目标获取车道
     * @param req  接管上下文
     */
    private void replayOnLane(AcquireLane lane, AwaitTracker.NotLeaderRequest req) {
        SessionContext s = lane.cm.session();
        long budget = req.remainingMs() < 0 ? config.requestTimeout().toMillis()
                : req.remainingMs();
        if (s == null || budget <= 0) {
            failReplay(req, new OpenLatchException(StatusCode.NOT_LEADER,
                    "leader lane not usable"));
            return;
        }
        Envelope env = req.envelope().toBuilder().setRequestId(s.nextRequestId()).build();
        lane.awaits.startAcquire(env.getRequestId(), env, req.spec(), req.userFuture(), budget);
    }

    /**
     * 强制种子发现（连续阈值/无地址提示）：扇出后以发现的 Leader 改道重放。
     *
     * @param req 接管上下文
     */
    private void forceDiscover(AwaitTracker.NotLeaderRequest req) {
        long budget = req.remainingMs() < 0 ? config.requestTimeout().toMillis()
                : req.remainingMs();
        SeedDiscovery.discoverLeader(config, eventLoopGroup, timer,
                        Math.max(200, budget - GAP_RETRY_BACKOFF_MS))
                .whenComplete((addr, err) -> {
                    if (err != null || addr == null) {
                        failReplay(req, new OpenLatchException(StatusCode.NOT_LEADER,
                                "leader discovery failed: " + (err == null ? "empty" : err.getMessage())));
                        return;
                    }
                    boolean targetIsHome = addr.host().equals(connectionManager.targetHost())
                            && addr.port() == connectionManager.targetPort();
                    if (targetIsHome) {
                        if (acquireLane != null) {
                            demoteToHome(acquireLane);
                        }
                        replayOnHome(req);
                    } else {
                        retargetAcquireLane(addr, req);
                    }
                });
    }

    /**
     * 重放链失败出口。
     *
     * @param req   接管上下文
     * @param cause 失败原因
     */
    private void failReplay(AwaitTracker.NotLeaderRequest req, Throwable cause) {
        req.userFuture().completeExceptionally(cause);
    }

    /**
     * 跟踪器当前会话（home 或获取车道，由 map 反查）。
     *
     * @param tracker 等待跟踪器
     * @return 该跟踪器所属车道的当前会话；车道已卸任/未激活为 {@code null}
     */
    private SessionContext trackerSession(AwaitTracker tracker) {
        if (tracker == awaitTracker) {
            return connectionManager.session();
        }
        AcquireLane lane = acquireLane;
        return lane != null && lane.awaits == tracker ? lane.cm.session() : null;
    }

    /**
     * 会话 → 多路复用器解析（看门狗/释放路由；未登记会话回落 home）。
     *
     * @param sessionId 持锁归属会话 id
     * @return 归属车道多路复用器
     */
    private RequestMultiplexer muxForSession(long sessionId) {
        LaneRef ref = lanesBySession.get(sessionId);
        return ref == null ? multiplexer : ref.mux();
    }

    /**
     * 会话归属车道可用性（D5 正交化：不可得按 home 连接状态）。
     *
     * @param sessionId 持锁归属会话 id
     * @return 归属车道 ACTIVE 返回 {@code true}
     */
    private boolean laneActive(long sessionId) {
        LaneRef ref = lanesBySession.get(sessionId);
        ConnectionManager cm = ref == null ? connectionManager : ref.cm();
        return cm.isActive();
    }

    /**
     * 车道退役（design D6 收口规则）：车道不再是当前获取车道时，无在册持锁
     * 且无挂起等待则立即关闭并摘路由；仍有存量锁（该锁的转发出口）或等待
     * 则入待退池，由释放/失锁事件二次收口（{@link #tryRetireSpares()}）。
     *
     * @param lane 让位的获取车道（null 或仍为当前车道时空操作）
     */
    private void maybeRetire(AcquireLane lane) {
        if (lane == null || lane == acquireLane) {
            return;
        }
        long sid = lane.sessionId;
        for (HeldLockRegistry.HeldEntry e : heldLockRegistry.entries()) {
            if (e.sessionId() == sid) {
                if (!spareLanes.contains(lane)) {
                    spareLanes.add(lane); // 存量锁仍归属本车道，保留转发出口
                }
                return;
            }
        }
        if (lane.awaits.waitingCount() > 0) {
            if (!spareLanes.contains(lane)) {
                spareLanes.add(lane);
            }
            return;
        }
        if (sid != 0) {
            lanesBySession.remove(sid);
        }
        lane.cm.shutdown();
    }

    /**
     * 断连车道联动：该车道挂起请求与等待快速失败，并为归属本车道会话的
     * 持锁登记失锁时刻裁决（详设 §6.2，按会话隔离不误伤他车道锁）。
     *
     * @param laneSid 断连车道的会话 id（0 表示未握手，跳过持锁裁决）
     * @param mux     本车道多路复用器
     * @param awaits  本车道等待跟踪器
     */
    private void onLaneDisconnected(long laneSid, RequestMultiplexer mux, AwaitTracker awaits) {
        ServerUnavailableException cause = new ServerUnavailableException("connection lost");
        mux.failAll(cause);
        awaits.failAll(cause);
        if (laneSid == 0) {
            return;
        }
        long now = System.currentTimeMillis();
        for (HeldLockRegistry.HeldEntry entry : heldLockRegistry.entries()) {
            if (entry.sessionId() != laneSid) {
                continue;
            }
            long delayMs = entry.lostAtMs() - now;
            if (delayMs <= 0) {
                loseEntry(entry, new LockLostException(
                        "lock '" + entry.key() + "' lease already expired at disconnect"));
            } else {
                entry.setLostAtTask(timer.newTimeout(t -> {
                    if (heldLockRegistry.get(entry.key(), entry.threadId()) == entry) {
                        loseEntry(entry, new LockLostException(
                                "lock '" + entry.key() + "' lost: not reconnected before lostAt"));
                    }
                }, delayMs, TimeUnit.MILLISECONDS));
            }
        }
    }

    /**
     * 创建构建器。服务地址为唯一必填项，其余参数取详设 §6.7 默认值。
     *
     * @return 新的构建器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 客户端配置，供内部组件与测试读取。
     *
     * @return 不可变配置
     */
    public ClientConfig config() {
        return config;
    }

    /**
     * 请求建立连接。构建时已自动发起首次连接；本方法返回在首次进入
     * 可用状态或客户端关停时完成的 future，供调用方显式等待握手完成。
     * 已可用时返回立即完成的 future。
     *
     * @return 连接 future
     */
    public CompletableFuture<Void> connectAsync() {
        return connectionManager.connectAsync();
    }

    /**
     * 是否处于可收发业务请求的状态。
     *
     * @return 连接可用返回 {@code true}
     */
    public boolean isActive() {
        return connectionManager.isActive();
    }

    /**
     * 等待获取车道建立（S3 测试钩子，包私有）：启动直连发现/重定向为异步
     * 编排，本方法自旋等待 {@code acquireLane} 就位或超时。单机模式无获取
     * 车道，调用方应在确认非集群形态后使用。
     *
     * @param timeoutMs 等待上限（毫秒）
     * @return 车道已建立返回 {@code true}；超时返回 {@code false}
     */
    boolean awaitAcquireLaneReady(long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            AcquireLane lane = acquireLane;
            if (lane != null && lane.cm.isActive()) {
                return true;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    /**
     * 异步获取锁（详设 §6.3）。行为按 {@link AcquireSpec#waitMs()} 分支：
     * 立即式被拒或以错误码失败时，future 以携带状态码的异常完成；
     * 排队/限时等待受等待总超时兜底（{@code waitMs > 0} 用其本身，
     * {@code waitMs = -1} 用 {@code defaultWaitTimeout}），到点未授予以
     * {@link LockAcquisitionTimeoutException} 失败。
     *
     * <p>授予成功时客户端自动登记本地持锁状态；锁的续租与丢失通知随
     * 看门狗机制提供。返回的 future 在网络/定时器线程上完成，
     * 链接其上的回调不得阻塞。
     *
     * @param spec 获取参数
     * @return 授予结果 future
     */
    public CompletableFuture<LockGrant> acquireAsync(AcquireSpec spec) {
        java.util.Objects.requireNonNull(spec, "spec must not be null");
        if (closed) {
            return failedFuture(new IllegalStateException("client is shut down"));
        }
        // 获取车道优先（design D6）：存在指向 Leader 的车道时新获取以其会话
        // 发出；车道暂不可用（重连窗口）回落 home——home 若非 Leader 会以
        // NOT_LEADER 触发重定向编排，不产生错误授予。
        AcquireLane lane = acquireLane;
        AwaitTracker tracker = awaitTracker;
        SessionContext session = connectionManager.session();
        if (lane != null && lane.cm.session() != null) {
            tracker = lane.awaits;
            session = lane.cm.session();
        }
        if (session == null) {
            return failedFuture(new ServerUnavailableException("connection is not active"));
        }
        long requestId = session.nextRequestId();
        Envelope envelope = Envelope.newBuilder()
                .setProtocolVersion(2)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(requestId)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey(spec.key())
                        .setLockType(spec.lockType().wireType())
                        .setThreadId(spec.threadId())
                        .setLeaseMs(spec.leaseMs())
                        .setWaitMs(spec.waitMs() == 0 ? 0 : -1))
                .build();
        long totalTimeoutMs;
        if (spec.waitMs() == 0) {
            totalTimeoutMs = 0;
        } else if (spec.waitMs() > 0) {
            totalTimeoutMs = spec.waitMs();
        } else {
            totalTimeoutMs = config.defaultWaitTimeout().toMillis();
        }
        CompletableFuture<LockGrant> future = new CompletableFuture<>();
        tracker.startAcquire(requestId, envelope, spec, future, totalTimeoutMs);
        return future;
    }

    /**
     * 异步释放锁（详设 §6.3）：以获取时签发的租约凭据发送释放请求。
     * 服务端按凭据与归属裁决：凭据不匹配回 {@code INVALID_TOKEN}，
     * 未持有回 {@code NOT_HELD}，均以携带状态码的异常完成。
     *
     * @param key        锁键
     * @param leaseToken 获取时签发的租约凭据
     * @param threadId   申请释放的线程标识
     * @return 释放结果 future；服务端确认释放后正常完成
     */
    public CompletableFuture<Void> releaseAsync(String key, long leaseToken, long threadId) {
        if (closed) {
            CompletableFuture<Void> failed = new CompletableFuture<>();
            failed.completeExceptionally(new IllegalStateException("client is shut down"));
            return failed;
        }
        // 存量锁跟家（design D6）：释放按其获取车道路由——home 会话的锁在
        // 降级节点上经服务端转发车道送达 Leader，获取车道会话的锁直发 Leader。
        HeldLockRegistry.HeldEntry held = heldLockRegistry.get(key, threadId);
        RequestMultiplexer targetMux = held == null ? multiplexer : muxForSession(held.sessionId());
        Envelope.Builder builder = Envelope.newBuilder()
                .setType(MessageType.LOCK_RELEASE)
                .setReleaseRequest(ReleaseRequest.newBuilder()
                        .setKey(key)
                        .setLeaseToken(leaseToken)
                        .setThreadId(threadId));
        return targetMux.send(builder, config.requestTimeout().toMillis())
                .thenApply(resp -> {
                    StatusCode status = resp.getReleaseResponse().getStatus();
                    if (status != StatusCode.OK) {
                        throw new OpenLatchException(status, "release of '" + key + "' failed: " + status);
                    }
                    if (resp.getReleaseResponse().getFullyReleased()) {
                        HeldLockRegistry.HeldEntry entry = heldLockRegistry.remove(key, threadId);
                        if (entry != null) {
                            watchdog.stop(entry);
                        }
                        // design D4：该键无人重持时丢弃监听器登记——监听表
                        // 不随历史 key 基数无界增长；重持后需重新注册。
                        if (!heldLockRegistry.hasAnyFor(key)) {
                            keyLockLostListeners.remove(key);
                        }
                        tryRetireSpares();
                    }
                    return null;
                });
    }

    /**
     * 客户端是否已关停。
     *
     * @return 已关停返回 {@code true}
     */
    public boolean isClosed() {
        return closed;
    }

    /**
     * 关停客户端。幂等，可重复调用；进入终态后新请求被拒绝、不再重连。
     *
     * <p>关停前对本地登记持锁尽力释放（详设 §6.3，见
     * {@link #releaseAllHeldBestEffort()}）：逐条目发送释放请求，单条目至多
     * 等待一个 requestTimeout；失败不阻塞关停，由服务端租约到期兜底。
     * 随后停止全部后台资源（网络线程组、定时器、回调执行器）并置位终态标志。
     */
    public synchronized void shutdown() {
        if (closed) {
            return;
        }
        releaseAllHeldBestEffort();
        closed = true;
        AcquireLane lane = acquireLane;
        acquireLane = null;
        if (lane != null) {
            lane.cm.shutdown();
        }
        for (AcquireLane spare : spareLanes) {
            spare.cm.shutdown();
        }
        spareLanes.clear();
        connectionManager.shutdown();
        timer.stop();
        lockLostExecutor.shutdown();
        eventLoopGroup.shutdownGracefully(SHUTDOWN_QUIET_MS, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        log.info("OpenLatch client shut down");
    }

    /**
     * 失锁裁决的统一出口：停止续租、取消失锁时刻定时、移除簿记、触发回调，
     * 并尝试收口因此清空的待退车道（design D6"存量清零→连接收口"）。
     * 以簿记移除的原子性保证同一条目只被裁决一次。
     *
     * @param entry 持锁条目
     * @param cause 失锁原因
     */
    private void loseEntry(HeldLockRegistry.HeldEntry entry, LockLostException cause) {
        if (heldLockRegistry.remove(entry.key(), entry.threadId()) == null) {
            return;
        }
        watchdog.stop(entry);
        io.netty.util.Timeout lostAtTask = entry.lostAtTask();
        if (lostAtTask != null) {
            lostAtTask.cancel();
        }
        fireLockLost(entry.key(), cause);
        tryRetireSpares();
    }

    /**
     * 关停前尽力释放本地持锁（详设 §6.3）：对每个条目发送释放请求并等待
     * 至多一个请求超时；失败者不阻塞关停——服务端租约到期兜底释放。
     * 在 {@code closed} 置位前调用，此时释放请求仍可受理。
     */
    private void releaseAllHeldBestEffort() {
        java.util.List<CompletableFuture<Void>> releases = new java.util.ArrayList<>();
        for (HeldLockRegistry.HeldEntry entry : heldLockRegistry.entries()) {
            watchdog.stop(entry);
            releases.add(releaseAsync(entry.key(), entry.leaseToken(), entry.threadId())
                    .exceptionally(x -> null));
        }
        if (!releases.isEmpty()) {
            try {
                CompletableFuture.allOf(releases.toArray(new CompletableFuture<?>[0]))
                        .get(config.requestTimeout().toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                log.debug("best-effort release on shutdown incomplete: {}", e.toString());
            }
        }
        for (HeldLockRegistry.HeldEntry entry : java.util.List.copyOf(heldLockRegistry.entries())) {
            heldLockRegistry.remove(entry.key(), entry.threadId());
        }
    }

    /**
     * 授予回调：登记本地持锁归属（只记归属不记重入计数，design.md D4）
     * 并为新登记条目启动看门狗续租；重入授予命中既有条目时不重复启动。
     *
     * @param spec  获取参数
     * @param grant 授予结果
     */
    /**
     * 授予登记（获取/ home 车道共用）：登记本地持锁归属（只记归属不记重入
     * 计数，design.md D4）并为新登记条目启动看门狗续租；重入授予命中既有
     * 条目时不重复启动。
     *
     * @param sessionId 授予所属车道的会话 id（存量锁跟家的路由键）
     * @param spec      获取参数
     * @param grant     授予结果
     */
    private void registerHeld(long sessionId, AcquireSpec spec, LockGrant grant) {
        notLeaderStreak.set(0); // 授予成功：故障转移闭环达成，重计
        HeldLockRegistry.HeldEntry entry = heldLockRegistry.register(spec.key(), spec.threadId(),
                spec.lockType(), grant.leaseToken(), grant.grantedLeaseMs(), sessionId,
                System.currentTimeMillis());
        if (entry.watchdogTask() == null) {
            watchdog.start(entry);
        }
    }

    /**
     * 退役车道收口：把已无在册持锁与挂起等待的非当前获取车道关闭，并从
     * 会话路由表摘除（design D6"存量清零→连接收口"）。
     */
    private void tryRetireSpares() {
        for (AcquireLane spare : spareLanes) {
            long sid = spare.sessionId;
            boolean hasEntry = false;
            for (HeldLockRegistry.HeldEntry e : heldLockRegistry.entries()) {
                if (e.sessionId() == sid) {
                    hasEntry = true;
                    break;
                }
            }
            if (!hasEntry && spare.awaits.waitingCount() == 0 && spare != acquireLane) {
                spareLanes.remove(spare);
                if (sid != 0) {
                    lanesBySession.remove(sid);
                }
                spare.cm.shutdown();
            }
        }
    }

    /**
     * 构造已失败的 future。
     *
     * @param cause 失败原因
     * @return 失败的 future
     * @param <T> future 值类型
     */
    private static <T> CompletableFuture<T> failedFuture(Throwable cause) {
        CompletableFuture<T> future = new CompletableFuture<>();
        future.completeExceptionally(cause);
        return future;
    }

    /**
     * 等价于 {@link #shutdown()}，支持 try-with-resources。
     */
    @Override
    public void close() {
        shutdown();
    }

    /**
     * 网络 IO 线程组，内部组件经此提交连接与写任务。
     *
     * @return EventLoop 线程组
     */
    EventExecutorGroup eventLoopGroup() {
        return eventLoopGroup;
    }

    /**
     * 共享定时器，内部组件经此挂各类超时任务。
     *
     * @return 定时器
     */
    HashedWheelTimer timer() {
        return timer;
    }

    /**
     * 锁丢失回调执行器。
     *
     * @return 单线程执行器
     */
    ExecutorService lockLostExecutor() {
        return lockLostExecutor;
    }

    /**
     * 本地持锁簿记，同步包装与看门狗共用。
     *
     * @return 持锁簿记
     */
    HeldLockRegistry heldLockRegistry() {
        return heldLockRegistry;
    }

    /**
     * 看门狗，同步包装的失锁解锁路径经此停止续租。
     *
     * @return 看门狗
     */
    io.github.lamspace.openlatch.client.internal.Watchdog watchdog() {
        return watchdog;
    }

    /**
     * 请求多路复用器，测试经此装配出站门（半开连接注入，design.md D7）。
     *
     * @return 多路复用器
     */
    RequestMultiplexer requestMultiplexer() {
        return multiplexer;
    }

    /**
     * 连接管理器，测试经此关闭活动通道（故障注入：持锁/等待中断连）。
     *
     * @return 连接管理器
     */
    ConnectionManager connectionManager() {
        return connectionManager;
    }

    /**
     * 锁键维度监听器表的当前登记键数，测试断言清理语义用
     * （design D4：完全释放后丢弃登记）。
     *
     * @return 有监听器登记的锁键数量
     */
    int keyListenerCount() {
        return keyLockLostListeners.size();
    }

    /**
     * 登记锁键维度的锁丢失监听器。监听器随该键锁完全释放（计数归零且
     * 本地无人重持）被丢弃：之后该键重新获取并丢锁时旧监听器不触发，
     * 调用方需重新注册（详设 §6.3，design D4）。
     *
     * @param key      锁键
     * @param listener 监听器
     */
    void addLockLostListener(String key, LockLostListener listener) {
        keyLockLostListeners
                .computeIfAbsent(key, k -> new java.util.concurrent.CopyOnWriteArrayList<>())
                .add(Objects.requireNonNull(listener));
    }

    /**
     * 触发锁丢失通知：全局与锁键维度监听器在专用执行器上回调；
     * 单个回调异常被捕获并记录，不影响其他监听器（详设 §6.6）。
     *
     * @param key   丢失的锁键
     * @param cause 丢失原因
     */
    void fireLockLost(String key, LockLostException cause) {
        lockLostExecutor.execute(() -> {
            for (LockLostListener listener : globalLockLostListeners) {
                invokeLockLost(listener, key, cause);
            }
            java.util.List<LockLostListener> keyListeners = keyLockLostListeners.get(key);
            if (keyListeners != null) {
                for (LockLostListener listener : keyListeners) {
                    invokeLockLost(listener, key, cause);
                }
            }
        });
    }

    /**
     * 调用单个锁丢失监听器并隔离其异常。
     *
     * @param listener 监听器
     * @param key      锁键
     * @param cause    丢失原因
     */
    private void invokeLockLost(LockLostListener listener, String key, LockLostException cause) {
        try {
            listener.onLockLost(key, cause);
        } catch (RuntimeException e) {
            log.warn("lock-lost listener for '{}' threw", key, e);
        }
    }

    /**
     * 创建可重入互斥锁句柄（详设 §6.3）。同一锁键可创建多个句柄，
     * 共享服务端锁状态与本地持锁簿记。
     *
     * @param key 锁键
     * @return 锁句柄
     */
    public OLock newReentrantLock(String key) {
        return new RemoteLock(this, Objects.requireNonNull(key), LockType.REENTRANT);
    }

    /**
     * 创建不可重入互斥锁句柄（详设 §6.3）。
     *
     * <p><b>警示</b>：同持有者再次获取将排队等待自身，直至租约到期才解开
     * （详设 §4.4"SimpleLock 的自锁问题"）。
     *
     * @param key 锁键
     * @return 锁句柄
     */
    public OLock newSimpleLock(String key) {
        return new RemoteLock(this, Objects.requireNonNull(key), LockType.SIMPLE);
    }

    /**
     * 创建读写锁门面（详设 §6.3）。
     *
     * @param key 锁键
     * @return 读写锁门面
     */
    public OReadWriteLock newReadWriteLock(String key) {
        Objects.requireNonNull(key);
        return new RemoteReadWriteLock(key,
                new RemoteLock(this, key, LockType.READ),
                new RemoteLock(this, key, LockType.WRITE));
    }

    /**
     * 登记全局锁丢失监听器：任何锁丢失都会收到回调。
     *
     * @param listener 监听器
     */
    public void addLockLostListener(LockLostListener listener) {
        globalLockLostListeners.add(Objects.requireNonNull(listener));
    }

    /**
     * 客户端构建器：收集配置并校验（详设 §6.7 默认值表；§6.3 种子列表）。
     *
     * <p>服务地址必填：{@code address}（Phase 1 单地址入口）与 {@code seeds}
     * （v2 种子列表）至少其一，同配以 {@code seeds} 为准；其余未设置时使用默认值。
     * 地址格式为 {@code host:port}，缺端口或端口非法时构建失败。
     */
    public static final class Builder {

        /** 服务地址（单地址入口），格式 {@code host:port}；与 seeds 二选一必填。 */
        private String address;
        /** 种子地址列表（{@code host:port} 逐项），非空时优先于 {@code address}。 */
        private java.util.List<String> seedAddresses = java.util.List.of();
        /** 单个请求超时，默认 5s。 */
        private Duration requestTimeout = Duration.ofSeconds(5);
        /** {@code lock()} 总等待兜底超时，默认 30s。 */
        private Duration defaultWaitTimeout = Duration.ofSeconds(30);
        /** TCP 连接 + 握手超时，默认 3s。 */
        private Duration connectTimeout = Duration.ofSeconds(3);
        /** 重连指数退避初始值，默认 200ms。 */
        private Duration reconnectInitialBackoff = Duration.ofMillis(200);
        /** 重连指数退避上限，默认 10s。 */
        private Duration reconnectMaxBackoff = Duration.ofSeconds(10);
        /** 客户端 Netty EventLoop 线程数，默认 1。 */
        private int workerThreads = 1;

        /**
         * 私有构造：仅由 {@link OpenLatchClient#builder()} 创建。
         */
        private Builder() {
        }

        /**
         * 设置服务地址（单地址入口，等价于一元种子列表）。
         *
         * @param address 服务地址，格式 {@code host:port}
         * @return 本构建器
         */
        public Builder address(String address) {
            this.address = address;
            return this;
        }

        /**
         * 设置种子地址列表（集群形态，详设 §6.3：任一可建连、断连轮询、
         * 强制发现扇出均以本表为集合）。与 {@link #address(String)} 同配时
         * 本表为准。
         *
         * @param seeds 种子地址，逐项格式 {@code host:port}，至少一个
         * @return 本构建器
         */
        public Builder seeds(String... seeds) {
            java.util.Objects.requireNonNull(seeds, "seeds");
            if (seeds.length == 0) {
                throw new IllegalArgumentException("seeds must not be empty");
            }
            this.seedAddresses = java.util.List.of(seeds.clone());
            return this;
        }

        /**
         * 设置种子地址列表（集合形态）。
         *
         * @param seeds 种子地址集合，逐项格式 {@code host:port}，至少一个
         * @return 本构建器
         */
        public Builder seeds(java.util.Collection<String> seeds) {
            java.util.Objects.requireNonNull(seeds, "seeds");
            if (seeds.isEmpty()) {
                throw new IllegalArgumentException("seeds must not be empty");
            }
            this.seedAddresses = java.util.List.copyOf(seeds);
            return this;
        }

        /**
         * 设置单个请求超时。
         *
         * @param requestTimeout 请求超时，必须为正数时长
         * @return 本构建器
         */
        public Builder requestTimeout(Duration requestTimeout) {
            this.requestTimeout = Objects.requireNonNull(requestTimeout);
            return this;
        }

        /**
         * 设置 {@code lock()} 总等待兜底超时。
         *
         * @param defaultWaitTimeout 兜底超时，必须为正数时长
         * @return 本构建器
         */
        public Builder defaultWaitTimeout(Duration defaultWaitTimeout) {
            this.defaultWaitTimeout = Objects.requireNonNull(defaultWaitTimeout);
            return this;
        }

        /**
         * 设置 TCP 连接 + 握手超时。
         *
         * @param connectTimeout 连接超时，必须为正数时长
         * @return 本构建器
         */
        public Builder connectTimeout(Duration connectTimeout) {
            this.connectTimeout = Objects.requireNonNull(connectTimeout);
            return this;
        }

        /**
         * 设置重连指数退避初始值。
         *
         * @param reconnectInitialBackoff 初始退避，必须为正数时长
         * @return 本构建器
         */
        public Builder reconnectInitialBackoff(Duration reconnectInitialBackoff) {
            this.reconnectInitialBackoff = Objects.requireNonNull(reconnectInitialBackoff);
            return this;
        }

        /**
         * 设置重连指数退避上限。
         *
         * @param reconnectMaxBackoff 退避上限，必须为正数时长且不小于初始退避
         * @return 本构建器
         */
        public Builder reconnectMaxBackoff(Duration reconnectMaxBackoff) {
            this.reconnectMaxBackoff = Objects.requireNonNull(reconnectMaxBackoff);
            return this;
        }

        /**
         * 设置客户端 Netty EventLoop 线程数。
         *
         * @param workerThreads 线程数，至少为 1
         * @return 本构建器
         */
        public Builder workerThreads(int workerThreads) {
            this.workerThreads = workerThreads;
            return this;
        }

        /**
         * 校验配置并构建客户端。
         *
         * @return 新的客户端实例，后台资源已启动
         * @throws IllegalStateException    未设置服务地址
         * @throws IllegalArgumentException 地址格式非法、时长非正、退避上限小于初始值或线程数小于 1
         */
        public OpenLatchClient build() {
            java.util.List<String> effective = seedAddresses.isEmpty()
                    ? (address == null ? java.util.List.of() : java.util.List.of(address))
                    : seedAddresses;
            if (effective.isEmpty()) {
                throw new IllegalStateException("address or seeds is required");
            }
            java.util.List<ClientConfig.SeedAddress> seeds = new java.util.ArrayList<>(effective.size());
            for (String s : effective) {
                seeds.add(parseSeed(s));
            }
            String host = seeds.get(0).host();
            int port = seeds.get(0).port();
            requirePositive(requestTimeout, "requestTimeout");
            requirePositive(defaultWaitTimeout, "defaultWaitTimeout");
            requirePositive(connectTimeout, "connectTimeout");
            requirePositive(reconnectInitialBackoff, "reconnectInitialBackoff");
            requirePositive(reconnectMaxBackoff, "reconnectMaxBackoff");
            if (reconnectMaxBackoff.compareTo(reconnectInitialBackoff) < 0) {
                throw new IllegalArgumentException(
                        "reconnectMaxBackoff must not be less than reconnectInitialBackoff");
            }
            if (workerThreads < 1) {
                throw new IllegalArgumentException("workerThreads must be >= 1");
            }
            ClientConfig config = new ClientConfig(host, port, java.util.List.copyOf(seeds),
                    requestTimeout, defaultWaitTimeout,
                    connectTimeout, reconnectInitialBackoff, reconnectMaxBackoff, workerThreads);
            return new OpenLatchClient(config);
        }

        /**
         * 解析单个种子地址。
         *
         * @param seed {@code host:port} 形态地址
         * @return 已分离的种子
         * @throws IllegalArgumentException 缺 {@code :}、端口缺失或非整数
         */
        private static ClientConfig.SeedAddress parseSeed(String seed) {
            int colon = seed.lastIndexOf(':');
            if (colon <= 0 || colon == seed.length() - 1) {
                throw new IllegalArgumentException("seed must be in host:port form: " + seed);
            }
            try {
                return new ClientConfig.SeedAddress(seed.substring(0, colon),
                        Integer.parseInt(seed.substring(colon + 1)));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("invalid port in seed: " + seed, e);
            }
        }

        /**
         * 校验时长为正数。
         *
         * @param value 待校验时长
         * @param name  参数名，用于错误信息
         * @throws IllegalArgumentException 时长为 {@code null}、零或负数
         */
        private static void requirePositive(Duration value, String name) {
            if (value == null || value.isZero() || value.isNegative()) {
                throw new IllegalArgumentException(name + " must be positive");
            }
        }
    }
}
