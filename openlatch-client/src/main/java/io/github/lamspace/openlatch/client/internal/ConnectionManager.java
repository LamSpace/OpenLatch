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

import io.github.lamspace.openlatch.protocol.AwaitNotify;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * 连接与重连状态机（详设 §6.2）。
 *
 * <p><b>状态迁移</b>：
 * <pre>
 * DISCONNECTED ─connect──▶ CONNECTING ─TCP 成功──▶ HELLO_SENT ─握手成功──▶ ACTIVE
 *      ▲                        │ 失败                       │ 断连/握手失败
 *      │                        ▼                            ▼
 *      └──────────────── RECONNECTING ◀──────────────────────┘
 *                        （指数退避重连）
 * </pre>
 * 终态 {@code CLOSED} 由 {@link #shutdown()} 进入：取消待执行的重连、
 * 关闭活动连接，此后不再发起任何连接尝试。
 *
 * <p><b>重连退避</b>：初始 {@code reconnectInitialBackoff}（默认 200ms），
 * 每次失败倍增，上限 {@code reconnectMaxBackoff}（默认 10s），
 * 每次延时附加 ±20% 随机抖动。重连成功后退避复位为初始值。
 *
 * <p><b>目标选择（S3，详设 §6.3）</b>：连接目标为可轮换的 {@code (currentHost,
 * currentPort)}，初值 = 种子列表首项。主动断连后的重试先试原地址；TCP 连接
 * 失败或握手失败则游标推进到下一种子（按序循环），实现"先试原地址，失败后
 * 轮询种子列表"。Leader 改连由客户端新建专用车道完成，不在本状态机内切换目标。
 *
 * <p><b>线程模型</b>：状态迁移由三方线程发起——调用方线程
 * （{@link #connectAsync()}/{@link #shutdown()}）、共享定时器线程（退避重连
 * 触发 {@link #doConnect()}）与 EventLoop 线程（TCP 连接回调、通道失效）。
 * {@code stateLock} 仅保护四个非 volatile 字段（{@code state}、
 * {@code connectFuture}、{@code currentBackoffMs}、{@code reconnectTask}）；
 * {@code channel}、{@code session}、{@code pendingSession} 为 volatile，
 * {@link #activeChannel()}/{@link #session()} 不经锁读取，读方可能短暂
 * 看到旧值——「读到 {@code null} 即视为非 ACTIVE」是调用方须容忍的窗口语义。
 * 断连/握手回调中的用户级联动（断连回调、ACTIVE 监听）一律在锁外执行。
 *
 * <p><b>断连处理</b>：通道失效时先调用断连回调（由客户端装配：挂起请求
 * 快速失败、等待清空、持锁失锁时刻登记），再进入退避重连。
 *
 * <p><b>握手</b>：TCP 连接成功后立即发送 {@code HELLO}（{@code requestId = 1}，
 * 经 {@link RequestMultiplexer} 关联响应）；{@code HelloResponse} 状态为
 * {@code OK} 时以响应中的 {@code sessionId} 创建新 {@link SessionContext}
 * 并进入 ACTIVE。握手任何环节失败都关闭通道，走断连→重连路径。
 * 重连成功必然更换 {@code sessionId}，{@code requestId} 重新从 1 分配。
 */
public final class ConnectionManager {

    /** 连接状态。 */
    public enum State {
        /** 初始态：尚未发起连接。 */
        DISCONNECTED,
        /** TCP 连接进行中。 */
        CONNECTING,
        /** TCP 已连接，等待握手响应。 */
        HELLO_SENT,
        /** 握手完成，可收发业务请求。 */
        ACTIVE,
        /** 断连后等待退避重连。 */
        RECONNECTING,
        /** 终态：已关停，不再连接。 */
        CLOSED
    }

    /** v2 协议版本，握手请求固定携带（服务端兼容 v1/v2，应答回显本版本）。 */
    private static final int PROTOCOL_VERSION = 2;
    /** 入站帧最大长度（1 MiB），与服务端帧长限制一致（详设 §3.1）。 */
    private static final int MAX_FRAME_LENGTH = 1024 * 1024;
    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    /** 客户端配置。 */
    private final ClientConfig config;
    /** 网络线程组：连接与读写均在此执行。 */
    private final EventLoopGroup group;
    /** 共享定时器：重连退避定时挂于此。 */
    private final HashedWheelTimer timer;
    /**
     * 状态迁移锁：仅保护 state、connectFuture、currentBackoffMs、reconnectTask
     * 四个非 volatile 字段。channel/session/pendingSession 为 volatile，
     * 读取不经本锁；connectDeadlineMs 由 doConnect 在锁内写、sendHello 在
     * 连接回调线程锁外读（可见性由连接回调的同步点保证）。
     */
    private final Object stateLock = new Object();
    /** 随机抖动源：退避延时 ±20%。 */
    private final ThreadLocalRandom jitter = ThreadLocalRandom.current();

    /** 当前状态。 */
    private State state = State.DISCONNECTED;
    /** 当前活动通道；非活动状态为 {@code null}。 */
    private volatile Channel channel;
    /** 当前会话上下文；仅 ACTIVE 状态非 {@code null}。 */
    private volatile SessionContext session;
    /** 本次连接尝试的会话上下文（握手完成前分配请求 id 用）。 */
    private volatile SessionContext pendingSession;
    /** 连接完成的共享 future；首次 {@link #connectAsync()} 时创建。 */
    private CompletableFuture<Void> connectFuture;
    /** 多路复用器，经 {@link #bind(RequestMultiplexer)} 装配（握手收发经此）。 */
    private RequestMultiplexer multiplexer;
    /** 断连回调：通道失效时、重连调度前调用（快速失败挂起请求等）。 */
    private Consumer<Throwable> disconnectHandler;
    /** 当前退避时长（毫秒），重连成功后复位。 */
    private long currentBackoffMs;
    /** 待执行的重连任务句柄，关停时取消。 */
    private Timeout reconnectTask;
    /** 本次连接尝试的握手截止时刻（epoch 毫秒），用于约束握手剩余超时。 */
    private long connectDeadlineMs;
    /** {@code AWAIT_NOTIFY} 下沉点；未装配时静默丢弃。 */
    private volatile Consumer<AwaitNotify> awaitNotifySink = notify -> {
        // 等待跟踪组件装配前的窗口期不应有通知到达
    };
    /** 进入 ACTIVE 时的回调（含首次连接与每次重连成功），由客户端装配。 */
    private volatile Runnable activeListener = () -> {
    };
    /**
     * 握手成功监听器（v2）：进入 ACTIVE 时收到（且仅在收到）{@code OK} 的
     * {@code HelloResponse} 上回调，携带服务端 leader 提示字段——客户端据此
     * 做启动直连发现（详设 §6.3）。在 EventLoop 线程调用，MUST NOT 阻塞。
     */
    private volatile Consumer<HelloResponse> helloListener = hello -> {
        // 客户端装配前不通知
    };
    /** 当前连接目标主机（stateLock 写、doConnect 锁内读；初值 = 种子首项）。 */
    private String currentHost;
    /** 当前连接目标端口（与 {@link #currentHost} 同锁域）。 */
    private int currentPort;
    /** 种子轮询游标（指向当前目标在 {@code config.seeds()} 中的下标）。 */
    private int seedCursor;
    /** 本轮已扫种子计数：多种子时满一轮才抬升退避（stateLock 内使用）。 */
    private int seedSweep;
    /** 失败换点开关：home 车道按种子轮询，leader 车道钉住目标（见全参构造）。 */
    private final boolean seedRotationOnFailure;

    /**
     * 计算本次重连延时并推进退避（stateLock 内调用）。
     *
     * <p><b>单种子（Phase 1 语义，逐字节不变）</b>：每次失败即指数倍增
     * （初始 200ms，上限 reconnectMaxBackoff）。
     * <p><b>多种子（集群）</b>：一轮内快速轮换种子（延时取当前退避不倍增），
     * 扫完全部种子一圈后才抬升退避——使 crash failover 能在亚秒级触及
     * 存活/新主节点，而非在恢复窗口内把退避翻倍到秒级而拖过恢复判定线。
     *
     * @return 本次重连的抖动延时（毫秒），基于推进前的当前退避
     */
    private long nextReconnectDelayLocked() {
        long delay = currentBackoffMs;
        int n = config.seeds().size();
        if (seedRotationOnFailure && n > 1) {
            if (++seedSweep >= n) {
                seedSweep = 0;
                currentBackoffMs = Math.min(currentBackoffMs * 2,
                        config.reconnectMaxBackoff().toMillis());
            }
        } else {
            currentBackoffMs = Math.min(currentBackoffMs * 2,
                    config.reconnectMaxBackoff().toMillis());
        }
        return withJitter(delay);
    }

    /**
     * 创建连接管理器：不发起连接，首次连接由 {@link #connectAsync()} 触发。
     *
     * @param config 客户端配置
     * @param group  网络线程组
     * @param timer  共享定时器
     */
    public ConnectionManager(ClientConfig config, EventLoopGroup group, HashedWheelTimer timer) {
        this(config, group, timer, config.host(), config.port(), true);
    }

    /**
     * 全参构造（S3 多车道，design D6）：车道以显式初始目标建连。
     *
     * @param config                 客户端配置（超时/退避/种子表来源）
     * @param group                  网络线程组
     * @param timer                  共享定时器
     * @param initialHost            初始连接目标主机（home 车道 = 种子首项；
     *                               leader 车道 = 提示所指地址）
     * @param initialPort            初始连接目标端口
     * @param seedRotationOnFailure  连接/握手失败是否按种子表轮询换点：home 车道
     *                               {@code true}（先原地址后轮询种子）；leader
     *                               车道 {@code false}（钉住提示地址，目标失效由
     *                               客户端重新发现并重建车道）
     */
    public ConnectionManager(ClientConfig config, EventLoopGroup group, HashedWheelTimer timer,
            String initialHost, int initialPort, boolean seedRotationOnFailure) {
        this.config = config;
        this.group = group;
        this.timer = timer;
        this.currentBackoffMs = config.reconnectInitialBackoff().toMillis();
        this.currentHost = initialHost;
        this.currentPort = initialPort;
        this.seedRotationOnFailure = seedRotationOnFailure;
    }

    /**
     * 设置握手成功监听器（v2 启动发现：HELLO 的 leader 提示由此上抛）。
     *
     * @param listener 监听器；在 EventLoop 线程收到 {@code OK} 握手响应回调
     */
    public void setHelloListener(Consumer<HelloResponse> listener) {
        this.helloListener = listener;
    }

    /**
     * 连接失败时把目标推进到下一粒种子（stateLock 内调用；种子表非空由
     * Builder 校验保证）。主动断连不推进——重连先试原地址（详设 §6.3）。
     */
    private void advanceSeedLocked() {
        if (!seedRotationOnFailure) {
            return; // leader 车道钉住目标：换点由客户端重发现驱动，不在本状态机内轮询
        }
        java.util.List<ClientConfig.SeedAddress> seeds = config.seeds();
        seedCursor = (seedCursor + 1) % seeds.size();
        ClientConfig.SeedAddress next = seeds.get(seedCursor);
        currentHost = next.host();
        currentPort = next.port();
    }

    /**
     * 装配多路复用器（握手请求经其收发）。仅由客户端装配阶段调用一次。
     *
     * @param multiplexer 多路复用器
     */
    public void bind(RequestMultiplexer multiplexer) {
        this.multiplexer = multiplexer;
    }

    /**
     * 设置断连回调。仅由客户端装配阶段调用一次。
     *
     * @param handler 断连处理器
     */
    public void setDisconnectHandler(Consumer<Throwable> handler) {
        this.disconnectHandler = handler;
    }

    /**
     * 设置 {@code AWAIT_NOTIFY} 下沉点（等待跟踪组件）。
     *
     * @param sink 通知处理器
     */
    public void setAwaitNotifySink(Consumer<AwaitNotify> sink) {
        this.awaitNotifySink = sink;
    }

    /**
     * 设置进入 ACTIVE 状态的回调：首次握手成功与每次重连成功都会触发，
     * 由客户端据此区分首连（无持锁，空操作）与重连（旧锁裁决）。
     *
     * @param listener 回调
     */
    public void setActiveListener(Runnable listener) {
        this.activeListener = listener;
    }

    /**
     * 入站信封分发：{@code AWAIT_NOTIFY} 交给通知下沉点，其余视为响应
     * 交给多路复用器按 {@code request_id} 关联。
     *
     * @param envelope 入站信封
     */
    public void dispatch(Envelope envelope) {
        if (envelope.getType() == MessageType.AWAIT_NOTIFY) {
            awaitNotifySink.accept(envelope.getAwaitNotify());
            return;
        }
        multiplexer.onResponse(envelope);
    }

    /**
     * 请求建立连接：已 ACTIVE 时立即完成；否则确保连接/重连进行中，
     * 返回在首次进入 ACTIVE 或客户端关停时完成的 future。
     *
     * @return 连接 future
     */
    public CompletableFuture<Void> connectAsync() {
        synchronized (stateLock) {
            if (state == State.CLOSED) {
                CompletableFuture<Void> failed = new CompletableFuture<>();
                failed.completeExceptionally(new IllegalStateException("client is closed"));
                return failed;
            }
            if (connectFuture == null) {
                connectFuture = new CompletableFuture<>();
            }
            if (state == State.ACTIVE) {
                connectFuture.complete(null);
            } else if (state == State.DISCONNECTED) {
                // 立即推进状态防止重复调度：connectAsync 被多次调用（构造器
                // 自动连接 + 调用方显式等待）时，第二次调用看到 CONNECTING
                // 便不再重复发起连接，杜绝双连接/双握手。
                state = State.CONNECTING;
                scheduleConnect(0);
            }
            return connectFuture;
        }
    }

    /**
     * 当前状态。
     *
     * @return 连接状态
     */
    public State state() {
        synchronized (stateLock) {
            return state;
        }
    }

    /**
     * 是否处于可收发业务请求的 ACTIVE 状态。
     *
     * @return ACTIVE 返回 {@code true}
     */
    public boolean isActive() {
        return state() == State.ACTIVE;
    }

    /**
     * 当前活动通道。
     *
     * @return 活动通道；非活动状态返回 {@code null}
     */
    public Channel activeChannel() {
        return channel;
    }

    /**
     * 当前连接目标主机（初始种子或失败换点后的种子；车道身份判定用）。
     *
     * @return 目标主机
     */
    public String targetHost() {
        synchronized (stateLock) {
            return currentHost;
        }
    }

    /**
     * 当前连接目标端口。
     *
     * @return 目标端口
     */
    public int targetPort() {
        synchronized (stateLock) {
            return currentPort;
        }
    }

    /**
     * 当前会话上下文。
     *
     * @return 会话上下文；非 ACTIVE 状态返回 {@code null}
     */
    public SessionContext session() {
        return session;
    }

    /**
     * 通道失效回调：由入站 handler 在 {@code channelInactive} 时调用。
     * 非 CLOSED 状态下：清空通道与会话、调用断连回调、调度退避重连。
     * 幂等——同一通道的重复通知只处理一次（以状态判定）。
     */
    public void handleChannelInactive() {
        synchronized (stateLock) {
            if (state == State.CLOSED) {
                channel = null;
                return;
            }
            if (state == State.RECONNECTING) {
                // 已处理过本次断连（如握手失败主动关闭后再次收到事件）
                return;
            }
            channel = null;
            session = null;
            state = State.RECONNECTING;
            if (disconnectHandler != null) {
                disconnectHandler.accept(null);
            }
            // 主动断连：先试原地址（不推进种子），退避推进见 nextReconnectDelayLocked
            scheduleConnect(nextReconnectDelayLocked());
        }
    }

    /**
     * 关停：进入终态，取消待执行重连，关闭活动连接，失败未完成的连接
     * future。幂等。
     */
    public void shutdown() {
        Channel toClose = null;
        CompletableFuture<Void> toFail = null;
        synchronized (stateLock) {
            if (state == State.CLOSED) {
                return;
            }
            state = State.CLOSED;
            if (reconnectTask != null) {
                reconnectTask.cancel();
                reconnectTask = null;
            }
            toClose = channel;
            channel = null;
            session = null;
            if (connectFuture != null && !connectFuture.isDone()) {
                toFail = connectFuture;
            }
        }
        if (toClose != null) {
            toClose.close();
        }
        if (toFail != null) {
            toFail.completeExceptionally(new IllegalStateException("client shut down"));
        }
    }

    /**
     * 调度一次连接尝试。
     *
     * @param delayMs 延时（毫秒）；0 表示立即
     */
    private void scheduleConnect(long delayMs) {
        // 状态锁内调用；状态迁移在 doConnect 内复检，关停后不再实际连接。
        reconnectTask = timer.newTimeout(t -> doConnect(), delayMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 执行一次连接尝试：TCP 连接 → 成功后发握手。
     * 失败统一经 {@link #handleChannelInactive()} 或 {@link #onAttemptFailed()}
     * 进入退避重连。
     */
    private void doConnect() {
        synchronized (stateLock) {
            if (state == State.CLOSED || state == State.ACTIVE) {
                return;
            }
            state = State.CONNECTING;
            connectDeadlineMs = System.currentTimeMillis() + config.connectTimeout().toMillis();
        }
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                        (int) config.connectTimeout().toMillis())
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        // 出站遍历序：先编码器再分帧器，故 prepender 更靠近 head。
                        ch.pipeline()
                                .addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4))
                                .addLast(new ProtobufDecoder(Envelope.getDefaultInstance()))
                                .addLast(new LengthFieldPrepender(4))
                                .addLast(new ProtobufEncoder())
                                .addLast(new ClientChannelHandler(
                                        ConnectionManager.this::dispatch,
                                        ConnectionManager.this::handleChannelInactive));
                    }
                });
        String targetHost;
        int targetPort;
        synchronized (stateLock) {
            targetHost = currentHost;
            targetPort = currentPort;
        }
        bootstrap.connect(targetHost, targetPort).addListener((ChannelFuture f) -> {
            if (!f.isSuccess()) {
                onAttemptFailed();
                return;
            }
            Channel ch = f.channel();
            synchronized (stateLock) {
                if (state == State.CLOSED) {
                    ch.close();
                    return;
                }
                channel = ch;
                state = State.HELLO_SENT;
            }
            sendHello(ch);
        });
    }

    /**
     * TCP 连接失败：走统一的断连→退避重连路径，并把下次目标推进到下一种子
     * （连接不可达即换点；主动断连不换点，见 {@link #handleChannelInactive()}）。
     */
    private void onAttemptFailed() {
        synchronized (stateLock) {
            if (state == State.CLOSED || state == State.RECONNECTING) {
                return;
            }
            state = State.RECONNECTING;
            advanceSeedLocked();
            scheduleConnect(nextReconnectDelayLocked());
        }
    }

    /**
     * 发送握手请求：{@code requestId = 1}，响应经多路复用器关联。
     * 超时取握手截止时刻的剩余时长。
     *
     * @param ch 已连接的通道
     */
    private void sendHello(Channel ch) {
        long remainingMs = Math.max(1, connectDeadlineMs - System.currentTimeMillis());
        // 握手是每条连接的第一条请求：在会话上下文上分配 requestId（消耗 1），
        // 保证其后的业务请求从 2 起，避免与握手的 requestId 冲突。
        SessionContext context = new SessionContext(0);
        this.pendingSession = context;
        Envelope hello = Envelope.newBuilder()
                .setProtocolVersion(PROTOCOL_VERSION)
                .setType(MessageType.HELLO)
                .setRequestId(context.nextRequestId())
                .setHelloRequest(HelloRequest.newBuilder()
                        .setClientProtocolVersion(PROTOCOL_VERSION)
                        .setClientName("openlatch-client"))
                .build();
        multiplexer.sendWithId(hello, remainingMs)
                .whenComplete((resp, err) -> onHelloResult(ch, resp, err));
    }

    /**
     * 握手结果处理：成功则创建新会话并进入 ACTIVE；任何失败关闭通道
     * （由 {@code channelInactive} 走重连路径）。
     *
     * @param ch   握手所在通道
     * @param resp 握手响应；失败为 {@code null}
     * @param err  收发异常；成功为 {@code null}
     */
    private void onHelloResult(Channel ch, Envelope resp, Throwable err) {
        CompletableFuture<Void> toComplete = null;
        boolean becameActive = false;
        HelloResponse activeHello = null;
        synchronized (stateLock) {
            if (state != State.HELLO_SENT) {
                return;
            }
            HelloResponse hello = resp == null ? null : resp.getHelloResponse();
            if (err != null || hello == null || hello.getStatus() != StatusCode.OK) {
                log.debug("handshake failed: {}", err == null ? hello.getStatus() : err.toString());
                // 握手失败常因该节点无主可登记（集群 SESSION_OPEN 不可提交）：
                // 推进游标，关闭后的退避重连换下一种子（详设 §6.3）。
                advanceSeedLocked();
                // 离开锁再关闭，避免 close 事件与状态锁交叉
            } else {
                SessionContext context = pendingSession;
                if (context != null) {
                    context.assignSessionId(hello.getSessionId());
                }
                session = context;
                state = State.ACTIVE;
                becameActive = true;
                activeHello = hello;
                currentBackoffMs = config.reconnectInitialBackoff().toMillis();
                toComplete = connectFuture;
            }
        }
        if (becameActive) {
            // 锁外回调：首连/重连成功通知（重连裁决经此触发，详设 §6.2）；
            // 握手监听携带 v2 leader 提示，客户端启动发现据此直连（§6.3）。
            activeListener.run();
            helloListener.accept(activeHello);
        }
        if (toComplete != null) {
            toComplete.complete(null);
            return;
        }
        ch.close();
    }

    /**
     * 附加 ±20% 随机抖动的延时。
     *
     * @param baseMs 基础延时（毫秒）
     * @return 抖动后的延时（毫秒），至少为 0
     */
    private long withJitter(long baseMs) {
        if (baseMs <= 0) {
            return 0;
        }
        double factor = 0.8 + jitter.nextDouble() * 0.4;
        return Math.max(0, (long) (baseMs * factor));
    }
}
