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

package io.github.lamspace.openlatch.console.admin;

import io.github.lamspace.openlatch.console.ConsoleConfig;
import io.github.lamspace.openlatch.protocol.AdminKeyDetailRequest;
import io.github.lamspace.openlatch.protocol.AdminKeyDetailResponse;
import io.github.lamspace.openlatch.protocol.AdminListKeysRequest;
import io.github.lamspace.openlatch.protocol.AdminListKeysResponse;
import io.github.lamspace.openlatch.protocol.AdminListSessionsRequest;
import io.github.lamspace.openlatch.protocol.AdminListSessionsResponse;
import io.github.lamspace.openlatch.protocol.AdminSummaryRequest;
import io.github.lamspace.openlatch.protocol.AdminSummaryResponse;
import io.github.lamspace.openlatch.protocol.ClusterView;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.net.ServerChannelInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.ssl.SslContext;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 单节点管理客户端（Phase 3 T3 design D6）：懒连接、HELLO(v3) 后同步
 * ADMIN 问答、断线懒重连、认证失败退避——控制台与服务端之间唯一的管理
 * 协议通道，每节点一实例（{@link AdminClientPool} 编排）。
 *
 * <p><b>与业务 client SDK 零重叠</b>：不引 openlatch-client（看门狗/锁语义/
 * 重发机制与控制台只读观察无涉），pipeline 直挂 netty-codec-protobuf 组件、
 * 分帧参数与服务端 {@link ServerChannelInitializer} 对齐（4 字节长度头，
 * 1MiB 帧上限）。
 *
 * <p><b>单飞问答</b>：控制台是观察型串行负载，全部请求在本实例的
 * {@code synchronized} 临界区内"发送-等待"一一对应（requestId 严格自增，
 * 响应以 id 回表）；服务端推送（requestId=0，如 AWAIT_NOTIFY）无对应
 * future，直接丢弃——控制台不持锁、不应有推送语义。
 *
 * <p><b>失败面</b>：连接/读写异常与超时统一折算 {@link AdminUnavailableException}
 * （kind=UNREACHABLE/TIMEOUT）；ADMIN 应答携带 {@code INVALID_REQUEST} 且随后
 * 连接被服务端断开（令牌被拒的线路形态，spec"管理令牌认证"）折算 kind=AUTH，
 * 并进入退避窗（默认 30s）——退避窗内 MUST NOT 反复重握手造成风暴
 * （spec"节点连接管理与故障降级"）。
 *
 * <p><b>线程模型</b>：{@code request*} 由 Web 线程调用（synchronized 串行）；
 * Netty EventLoop 仅完成 promise 投递；实例字段经 {@code synchronized(this)}
 * 与 volatile 发布，跨 Web 线程共享单实例安全。
 */
public final class AdminClient implements AutoCloseable {

    /** 认证失败后的握手退避窗（毫秒）。 */
    static final long AUTH_BACKOFF_MS = 30_000L;
    /** 控制台握手声明的协议版本（ADMIN 为 v3 专属语义，wire-protocol"管理消息增量"）。 */
    private static final int CLIENT_VERSION = 3;

    /** 目标节点地址（业务端口）。 */
    private final ConsoleConfig.Address address;
    /** 管理令牌。 */
    private final String token;
    /** 单请求超时（毫秒）。 */
    private final long timeoutMs;
    /** 共享 IO 线程组（由池创建，本实例不拥有）。 */
    private final EventLoopGroup group;
    /** 节点 TLS 上下文（Phase 3 T4）；{@code null} 即明文（明文形态与 T3 一致）。 */
    private final SslContext sslContext;
    /** 业务令牌（HELLO 携带）；{@code null} 表示服务端业务认证关闭或未配置。 */
    private final String businessToken;

    /** 当前连接（未连接/已断开为 {@code null}）。 */
    private volatile Channel channel;
    /** requestId → 在途应答 future。 */
    private final ConcurrentHashMap<Long, CompletableFuture<Envelope>> pending = new ConcurrentHashMap<>();
    /** 请求 id 发号器。 */
    private final AtomicLong requestSeq = new AtomicLong();
    /** 认证失败退避截止时刻（epoch 毫秒，0=无退避）。 */
    private volatile long authBackoffUntilMs;

    /**
     * 构造单节点客户端（不连接；明文、无业务令牌——T4 前的既有形态）。
     *
     * @param address   目标节点地址
     * @param token     管理令牌
     * @param timeoutMs 单请求超时（毫秒）
     * @param group     共享 EventLoop 组
     */
    public AdminClient(ConsoleConfig.Address address, String token, long timeoutMs,
                       EventLoopGroup group) {
        this(address, token, timeoutMs, group, null, null);
    }

    /**
     * 构造单节点客户端（Phase 3 T4 全形态：可选节点 TLS 与业务令牌）。
     *
     * @param address       目标节点地址
     * @param token         管理令牌
     * @param timeoutMs     单请求超时（毫秒）
     * @param group         共享 EventLoop 组
     * @param sslContext    节点 TLS 上下文；{@code null} 即明文
     * @param businessToken 业务令牌（HELLO 携带）；{@code null} 表示未配置
     */
    public AdminClient(ConsoleConfig.Address address, String token, long timeoutMs,
                       EventLoopGroup group, SslContext sslContext, String businessToken) {
        this.address = address;
        this.token = token;
        this.timeoutMs = timeoutMs;
        this.group = group;
        this.sslContext = sslContext;
        this.businessToken = businessToken;
    }

    /**
     * 目标节点地址。
     *
     * @return 地址值对象
     */
    public ConsoleConfig.Address address() {
        return address;
    }

    /**
     * ADMIN_SUMMARY 问答。
     *
     * @return 应答载荷（admin_summary_response）
     * @throws AdminUnavailableException 节点不可达/超时/认证被拒
     */
    public AdminSummaryResponse summary() {
        return request(Envelope.newBuilder()
                .setAdminSummaryRequest(AdminSummaryRequest.newBuilder().setToken(token)),
                MessageType.ADMIN_SUMMARY).getAdminSummaryResponse();
    }

    /**
     * ADMIN_LIST_KEYS 问答。
     *
     * @param page     页号（0 起）
     * @param pageSize 页大小
     * @param prefix   前缀过滤（空串不过滤）
     * @return 应答载荷（admin_list_keys_response）
     * @throws AdminUnavailableException 节点不可达/超时/认证被拒
     */
    public AdminListKeysResponse listKeys(int page, int pageSize, String prefix) {
        return request(Envelope.newBuilder()
                .setAdminListKeysRequest(AdminListKeysRequest.newBuilder()
                        .setToken(token).setPage(page).setPageSize(pageSize).setPrefix(prefix)),
                MessageType.ADMIN_LIST_KEYS).getAdminListKeysResponse();
    }

    /**
     * ADMIN_KEY_DETAIL 问答。
     *
     * @param key 锁键
     * @return 应答载荷（admin_key_detail_response）
     * @throws AdminUnavailableException 节点不可达/超时/认证被拒
     */
    public AdminKeyDetailResponse keyDetail(String key) {
        return request(Envelope.newBuilder()
                .setAdminKeyDetailRequest(AdminKeyDetailRequest.newBuilder()
                        .setToken(token).setKey(key)),
                MessageType.ADMIN_KEY_DETAIL).getAdminKeyDetailResponse();
    }

    /**
     * ADMIN_LIST_SESSIONS 问答。
     *
     * @return 应答载荷（admin_list_sessions_response）
     * @throws AdminUnavailableException 节点不可达/超时/认证被拒
     */
    public AdminListSessionsResponse listSessions() {
        return request(Envelope.newBuilder()
                .setAdminListSessionsRequest(AdminListSessionsRequest.newBuilder()
                        .setToken(token)),
                MessageType.ADMIN_LIST_SESSIONS).getAdminListSessionsResponse();
    }

    /**
     * CLUSTER_VIEW 问答（v2 既有消息，任意节点作答）。
     *
     * @return cluster_view 载荷（单机模式其 status=INVALID_REQUEST）
     * @throws AdminUnavailableException 节点不可达/超时/认证被拒
     */
    public ClusterView clusterView() {
        return request(Envelope.newBuilder(), MessageType.CLUSTER_VIEW).getClusterView();
    }

    /**
     * 同步单飞问答：确保连接（含 HELLO 握手）→ 发送 → 限时等待。
     * ADMIN 应答的 {@code INVALID_REQUEST} 按令牌被拒处理（管理消息的
     * 该码仅产生于认证/门控失败，且服务端紧随断连）；CLUSTER_VIEW 的
     * {@code INVALID_REQUEST} 是"单机无视图"业务语义，不折算认证失败。
     *
     * @param builder 载荷填充完毕的信封构造器（类型与 requestId 由本方法定）
     * @param type    请求类型
     * @return 应答信封
     * @throws AdminUnavailableException 失败（kind 见类注释）
     */
    private synchronized Envelope request(Envelope.Builder builder, MessageType type) {
        Channel ch = ensureConnected();
        long rid = requestSeq.incrementAndGet();
        Envelope msg = builder.setType(type).setRequestId(rid)
                .setProtocolVersion(CLIENT_VERSION).build();
        CompletableFuture<Envelope> future = new CompletableFuture<>();
        pending.put(rid, future);
        ch.writeAndFlush(msg).addListener(w -> {
            if (!w.isSuccess()) {
                pending.remove(rid);
                future.completeExceptionally(w.cause());
            }
        });
        Envelope resp;
        try {
            resp = future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            pending.remove(rid);
            disconnect();
            throw new AdminUnavailableException(AdminUnavailableException.Kind.TIMEOUT,
                    address + " 请求超时（" + timeoutMs + "ms）", e);
        } catch (InterruptedException e) {
            pending.remove(rid);
            Thread.currentThread().interrupt();
            throw new AdminUnavailableException(AdminUnavailableException.Kind.UNREACHABLE,
                    "等待应答被中断", e);
        } catch (ExecutionException e) {
            pending.remove(rid);
            disconnect();
            throw new AdminUnavailableException(AdminUnavailableException.Kind.UNREACHABLE,
                    address + " 请求失败: " + e.getCause(), e);
        }
        if (isAdminType(type) && adminStatus(resp, type) == StatusCode.INVALID_REQUEST) {
            authBackoffUntilMs = System.currentTimeMillis() + AUTH_BACKOFF_MS;
            disconnect();
            throw new AdminUnavailableException(AdminUnavailableException.Kind.AUTH,
                    address + " 管理认证失败（令牌不符或服务端未配置令牌）", null);
        }
        return resp;
    }

    /**
     * 确保连接存活且已完成 HELLO；退避窗内直接拒绝。
     *
     * @return 可用连接
     * @throws AdminUnavailableException 不可达或处于认证退避窗
     */
    private Channel ensureConnected() {
        long until = authBackoffUntilMs;
        if (until != 0 && System.currentTimeMillis() < until) {
            throw new AdminUnavailableException(AdminUnavailableException.Kind.AUTH,
                    address + " 处于认证退避窗（剩余 "
                            + (until - System.currentTimeMillis()) / 1000 + "s）", null);
        }
        Channel ch = channel;
        if (ch != null && ch.isActive()) {
            return ch;
        }
        disconnect();
        try {
            Bootstrap b = new Bootstrap().group(group)
                    .channel(io.netty.channel.socket.nio.NioSocketChannel.class)
                    .handler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel c) {
                            // TLS 必居 pipeline 首位（Phase 3 T4）：握手/加解密先于
                            // 分帧编解码；与服务端/客户端同构。
                            if (sslContext != null) {
                                c.pipeline().addLast("ssl", sslContext.newHandler(c.alloc()));
                            }
                            c.pipeline()
                                    .addLast("frame", new LengthFieldBasedFrameDecoder(
                                            ServerChannelInitializer.MAX_FRAME_LENGTH, 0, 4, 0, 4))
                                    .addLast("decoder", new ProtobufDecoder(
                                            Envelope.getDefaultInstance()))
                                    .addLast("prepender", new LengthFieldPrepender(4))
                                    .addLast("encoder", new ProtobufEncoder())
                                    .addLast("client", new ResponseDispatcher());
                        }
                    });
            ch = b.connect(address.host(), address.port())
                    .sync().channel();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AdminUnavailableException(AdminUnavailableException.Kind.UNREACHABLE,
                    address + " 连接被中断", e);
        } catch (RuntimeException e) {
            throw new AdminUnavailableException(AdminUnavailableException.Kind.UNREACHABLE,
                    address + " 连接失败: " + e.getMessage(), e);
        }
        channel = ch;
        // HELLO v3：配置了业务令牌即随 HELLO 携带（服务端业务认证开启时必需——
        // 认证开启下逐消息 admin-token 不构成 HELLO 放行依据，spec"不可借道"）；
        // 未配置（认证关闭）维持既有"auth_token 留空"形态。client_name 自证身份。
        long rid = requestSeq.incrementAndGet();
        CompletableFuture<Envelope> future = new CompletableFuture<>();
        pending.put(rid, future);
        HelloRequest.Builder helloReq = HelloRequest.newBuilder()
                .setClientProtocolVersion(CLIENT_VERSION)
                .setClientName("openlatch-console");
        if (businessToken != null && !businessToken.isBlank()) {
            helloReq.setAuthToken(businessToken);
        }
        ch.writeAndFlush(Envelope.newBuilder()
                .setType(MessageType.HELLO).setRequestId(rid)
                .setProtocolVersion(CLIENT_VERSION)
                .setHelloRequest(helloReq)
                .build());
        Envelope hello;
        try {
            hello = future.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            pending.remove(rid);
            disconnect();
            throw new AdminUnavailableException(AdminUnavailableException.Kind.UNREACHABLE,
                    address + " 握手失败: " + e.getMessage(), e);
        }
        if (hello.getHelloResponse().getStatus() != StatusCode.OK) {
            disconnect();
            throw new AdminUnavailableException(AdminUnavailableException.Kind.UNREACHABLE,
                    address + " 握手被拒: " + hello.getHelloResponse().getStatus(), null);
        }
        return ch;
    }

    /**
     * 摘除当前连接（幂等；在途 future 以不可达终结）。
     */
    private void disconnect() {
        Channel ch = channel;
        channel = null;
        if (ch != null) {
            ch.close();
        }
        pending.values().forEach(f ->
                f.completeExceptionally(new IllegalStateException("connection closed")));
        pending.clear();
    }

    /**
     * 是否 ADMIN 类型。
     *
     * @param type 消息类型
     * @return ADMIN 之一返回 true
     */
    private static boolean isAdminType(MessageType type) {
        return switch (type) {
            case ADMIN_SUMMARY, ADMIN_LIST_KEYS, ADMIN_KEY_DETAIL, ADMIN_LIST_SESSIONS -> true;
            default -> false;
        };
    }

    /**
     * 取 ADMIN 应答的状态码（按请求类型选载荷）。
     *
     * @param resp 应答信封
     * @param type 请求类型
     * @return 状态码；无对应载荷返回 {@code null}
     */
    private static StatusCode adminStatus(Envelope resp, MessageType type) {
        return switch (type) {
            case ADMIN_SUMMARY -> resp.hasAdminSummaryResponse()
                    ? resp.getAdminSummaryResponse().getStatus() : null;
            case ADMIN_LIST_KEYS -> resp.hasAdminListKeysResponse()
                    ? resp.getAdminListKeysResponse().getStatus() : null;
            case ADMIN_KEY_DETAIL -> resp.hasAdminKeyDetailResponse()
                    ? resp.getAdminKeyDetailResponse().getStatus() : null;
            case ADMIN_LIST_SESSIONS -> resp.hasAdminListSessionsResponse()
                    ? resp.getAdminListSessionsResponse().getStatus() : null;
            default -> null;
        };
    }

    /**
     * 关停连接（EventLoop 组由池统一回收）。
     */
    @Override
    public void close() {
        disconnect();
    }

    /**
     * 应答分发器：按 requestId 回表完成在途 future；推送/未知 id 丢弃。
     * 在连接 EventLoop 上执行，仅做并发表操作。
     */
    private final class ResponseDispatcher extends SimpleChannelInboundHandler<Envelope> {

        /** 构造应答分发器（由 AdminClient 实例持有，无需参数）。 */
        private ResponseDispatcher() {
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Envelope msg) {
            CompletableFuture<Envelope> f = pending.remove(msg.getRequestId());
            if (f != null) {
                f.complete(msg);
            }
            // requestId=0 或无在途对应：控制台无订阅语义，静默丢弃。
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close(); // 断开由 ensureConnected 的下一次懒重连接管
        }
    }
}
