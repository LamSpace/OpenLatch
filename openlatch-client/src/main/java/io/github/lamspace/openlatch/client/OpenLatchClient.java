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
import io.github.lamspace.openlatch.client.internal.RequestMultiplexer;
import io.github.lamspace.openlatch.client.internal.SessionContext;
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
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
 * <p><b>职责</b>：以单条长连接访问锁服务器，对外提供异步获取/释放内核与
 * JUC 风格同步锁包装；对内维护请求多路复用、等待跟踪、看门狗续租与断连重连。
 * 锁语义裁决（是否授予、是否可重入）全部由服务端完成，客户端仅做本地簿记。
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
    /** 请求多路复用：全部出站请求与入站响应的收口。 */
    private final RequestMultiplexer multiplexer;
    /** 连接与重连状态机。 */
    private final ConnectionManager connectionManager;
    /** 等待跟踪：排队挂起、通知重发、孤儿授予补偿。 */
    private final AwaitTracker awaitTracker;
    /** 本地持锁簿记：只记归属不记重入计数（详设 §6.3）。 */
    private final HeldLockRegistry heldLockRegistry = new HeldLockRegistry();
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
                config.requestTimeout().toMillis(), this::onAcquired);
        this.watchdog = new io.github.lamspace.openlatch.client.internal.Watchdog(
                timer, multiplexer, heldLockRegistry, config.requestTimeout().toMillis(),
                connectionManager::isActive,
                (entry, cause) -> fireLockLost(entry.key(), cause));
        this.connectionManager.bind(multiplexer);
        this.connectionManager.setDisconnectHandler(this::onDisconnect);
        this.connectionManager.setAwaitNotifySink(awaitTracker::onNotify);
        this.connectionManager.setActiveListener(this::onActive);
        this.multiplexer.setOrphanSink(awaitTracker::onOrphanResponse);
        // 构建即发起首次连接（异步）：连接失败自动进入退避重连。
        this.connectionManager.connectAsync();
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
        SessionContext session = connectionManager.session();
        if (session == null) {
            return failedFuture(new ServerUnavailableException("connection is not active"));
        }
        long requestId = session.nextRequestId();
        Envelope envelope = Envelope.newBuilder()
                .setProtocolVersion(1)
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
        awaitTracker.startAcquire(requestId, envelope, spec, future, totalTimeoutMs);
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
        Envelope.Builder builder = Envelope.newBuilder()
                .setType(MessageType.LOCK_RELEASE)
                .setReleaseRequest(ReleaseRequest.newBuilder()
                        .setKey(key)
                        .setLeaseToken(leaseToken)
                        .setThreadId(threadId));
        return multiplexer.send(builder, config.requestTimeout().toMillis())
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
        connectionManager.shutdown();
        timer.stop();
        lockLostExecutor.shutdown();
        eventLoopGroup.shutdownGracefully(SHUTDOWN_QUIET_MS, SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        log.info("OpenLatch client shut down");
    }

    /**
     * 断连回调（详设 §6.2）：挂起的请求与等待快速失败；为每个本地持锁
     * 登记失锁时刻 {@code lostAt = 上次成功续租 + 实际生效租约} 的定时裁决——
     * 若重连先行成功，由 {@link #onActive()} 立即裁决；若 {@code lostAt}
     * 先到，则到时触发锁丢失回调。
     *
     * @param ignored 断连原因占位（当前不使用）
     */
    private void onDisconnect(Throwable ignored) {
        ServerUnavailableException cause = new ServerUnavailableException("connection lost");
        multiplexer.failAll(cause);
        awaitTracker.failAll(cause);
        long now = System.currentTimeMillis();
        for (HeldLockRegistry.HeldEntry entry : heldLockRegistry.entries()) {
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
     * 进入 ACTIVE 回调（首连与每次重连成功）。重连成功时旧会话已被服务端
     * 清理、旧锁必然失效（详设 §6.2）：对全部旧会话的持锁立即触发锁丢失
     * 回调并清除本地状态；以 {@code sessionId} 甄别新旧条目，避免误伤
     * 重连后新获取的锁。首连时簿记为空，本方法空转。
     */
    private void onActive() {
        SessionContext current = connectionManager.session();
        for (HeldLockRegistry.HeldEntry entry : heldLockRegistry.entries()) {
            if (current == null || entry.sessionId() != current.sessionId()) {
                loseEntry(entry, new LockLostException(
                        "lock '" + entry.key() + "' lost: old session cleaned by server"));
            }
        }
    }

    /**
     * 失锁裁决的统一出口：停止续租、取消失锁时刻定时、移除簿记、触发回调。
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
    private void onAcquired(AcquireSpec spec, LockGrant grant) {
        SessionContext session = connectionManager.session();
        long sessionId = session == null ? 0 : session.sessionId();
        HeldLockRegistry.HeldEntry entry = heldLockRegistry.register(spec.key(), spec.threadId(),
                spec.lockType(), grant.leaseToken(), grant.grantedLeaseMs(), sessionId,
                System.currentTimeMillis());
        if (entry.watchdogTask() == null) {
            watchdog.start(entry);
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
     * 客户端构建器：收集配置并校验（详设 §6.7 默认值表）。
     *
     * <p>仅 {@code address} 必填；其余未设置时使用默认值。
     * 地址格式为 {@code host:port}，缺端口或端口非法时构建失败。
     */
    public static final class Builder {

        /** 服务地址，格式 {@code host:port}，未设置时构建失败。 */
        private String address;
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
         * 设置服务地址。
         *
         * @param address 服务地址，格式 {@code host:port}
         * @return 本构建器
         */
        public Builder address(String address) {
            this.address = address;
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
            if (address == null) {
                throw new IllegalStateException("address is required");
            }
            int colon = address.lastIndexOf(':');
            if (colon <= 0 || colon == address.length() - 1) {
                throw new IllegalArgumentException("address must be in host:port form: " + address);
            }
            String host = address.substring(0, colon);
            int port;
            try {
                port = Integer.parseInt(address.substring(colon + 1));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("invalid port in address: " + address, e);
            }
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
            ClientConfig config = new ClientConfig(host, port, requestTimeout, defaultWaitTimeout,
                    connectTimeout, reconnectInitialBackoff, reconnectMaxBackoff, workerThreads);
            return new OpenLatchClient(config);
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
