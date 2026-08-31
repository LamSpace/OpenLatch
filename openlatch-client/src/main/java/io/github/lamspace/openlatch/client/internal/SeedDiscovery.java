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
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.NodeInfo;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 种子扇出发现（详设 §6.3"连续 N 次 NOT_LEADER → 强制走一次种子列表发现"，
 * s3 design D4/D6 的降级路径实现体）。
 *
 * <p><b>过程</b>：对全部种子<b>并发</b>建短连接 → HELLO(v2) → 依提示定位 Leader：
 * {@code leader_address} 非空直取；为空（服务端未配置地址映射）则补一发
 * {@code CLUSTER_VIEW}——应答节点自报本机接入地址，取其中 {@code is_leader}
 * 且地址非空的条目。首个报告可用 Leader 的种子胜出；全部落空（不可达/无主/
 * 无地址）以异常完成。<b>并发扇出</b>而非串行：无主窗口的 Follower HELLO 会挂
 * 满其超时，串行会把发现延迟叠加成"种子数 × 单点超时"，可能整体错过已新选出
 * 的 Leader。正确性由客户端对陈旧提示的容忍（改连/发现失败再入本流程）保证。
 *
 * <p><b>线程模型</b>：静态入口在调用线程同时发起全部种子的异步连接，各探针在
 * Netty 回调线程独立收敛；返回 future 完成于首个命中或末个失败的回调线程。
 * 每粒种子超时取满 {@code budgetMs}（并发下互不占用时间），另有总预算封顶。
 */
public final class SeedDiscovery {

    /** 入站帧上限（与连接状态机一致）。 */
    private static final int MAX_FRAME_LENGTH = 1024 * 1024;
    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(SeedDiscovery.class);

    /** 工具类禁止实例化。 */
    private SeedDiscovery() {
    }

    /**
     * 逐种子发现 Leader 接入地址。
     *
     * @param config   客户端配置（种子表与线程/超时参数来源）
     * @param group    共享网络线程组（短连接复用）
     * @param timer    共享定时器（挂请求超时）
     * @param budgetMs 发现总预算（毫秒），按种子数均摊单点超时
     * @return Leader 种子地址 future；预算内无任何种子报告可用 Leader 时
     *         以 {@link DiscoveryFailedException} 异常完成
     */
    public static CompletableFuture<ClientConfig.SeedAddress> discoverLeader(
            ClientConfig config, EventLoopGroup group, HashedWheelTimer timer, long budgetMs) {
        List<ClientConfig.SeedAddress> seeds = config.seeds();
        CompletableFuture<ClientConfig.SeedAddress> out = new CompletableFuture<>();
        if (seeds.isEmpty()) {
            out.completeExceptionally(new DiscoveryFailedException("empty seed list"));
            return out;
        }
        // 并发扇出：全部种子同时探测，首个报告可用 Leader 者胜出。不串行——
        // 无主窗口的 Follower HELLO 会挂满其探测超时，串行会把发现延迟叠加到
        // "种子数 × 单点超时"，可能整体错过已新选出的 Leader（crash failover
        // wedge 根因）。每种子超时取满预算（并发下互不占用彼此时间）。
        AtomicInteger pending = new AtomicInteger(seeds.size());
        for (ClientConfig.SeedAddress seed : seeds) {
            probe(seed, config, group, timer, budgetMs)
                    .whenComplete((found, err) -> {
                        if (found != null && !out.isDone()) {
                            out.complete(found);
                            return;
                        }
                        if (err != null) {
                            log.debug("discovery probe failed on {}: {}", seed, err.toString());
                        }
                        if (pending.decrementAndGet() == 0 && !out.isDone()) {
                            out.completeExceptionally(new DiscoveryFailedException(
                                    "no seed reported a reachable leader within budget"));
                        }
                    });
        }
        // 总预算封顶：超时以失败完成（在途探针结果被忽略并各自收敛关闭）。
        timer.newTimeout(t -> out.completeExceptionally(
                new DiscoveryFailedException("discovery budget exhausted (" + budgetMs + "ms)")),
                budgetMs, TimeUnit.MILLISECONDS);
        return out;
    }

    /**
     * 单粒种子探针：建连 → HELLO → （必要时）CLUSTER_VIEW → 关闭。
     *
     * @param seed     目标种子
     * @param config   配置（连接超时来源）
     * @param group    网络线程组
     * @param timer    定时器
     * @param probeMs  单点超时预算
     * @return Leader 地址 future；本种子不可达/无主/无地址时异常完成（不外抛）
     */
    private static CompletableFuture<ClientConfig.SeedAddress> probe(
            ClientConfig.SeedAddress seed, ClientConfig config, EventLoopGroup group,
            HashedWheelTimer timer, long probeMs) {
        CompletableFuture<ClientConfig.SeedAddress> result = new CompletableFuture<>();
        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS,
                        (int) Math.min(Integer.MAX_VALUE, config.connectTimeout().toMillis()))
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) {
                        ch.pipeline()
                                .addLast(new LengthFieldBasedFrameDecoder(MAX_FRAME_LENGTH, 0, 4, 0, 4))
                                .addLast(new ProtobufDecoder(Envelope.getDefaultInstance()))
                                .addLast(new LengthFieldPrepender(4))
                                .addLast(new ProtobufEncoder());
                    }
                });
        bootstrap.connect(seed.host(), seed.port()).addListener((ChannelFuture f) -> {
            if (!f.isSuccess()) {
                result.completeExceptionally(f.cause());
                return;
            }
            Channel ch = f.channel();
            SessionContext sc = new SessionContext(0);
            RequestMultiplexer mux = new RequestMultiplexer(timer, () -> ch, () -> sc);
            // 入站响应喂给探针自有的多路复用器（与连接状态机同构，作用域仅此连接）。
            ch.pipeline().addLast(new ClientChannelHandler(mux::onResponse, () -> {
                // 探针连接失效：在途请求由多路复用器超时收敛，无需额外联动
            }));
            Envelope hello = Envelope.newBuilder()
                    .setProtocolVersion(2)
                    .setType(MessageType.HELLO)
                    .setRequestId(sc.nextRequestId())
                    .setHelloRequest(HelloRequest.newBuilder()
                            .setClientProtocolVersion(2).setClientName("openlatch-discovery"))
                    .build();
            mux.sendWithId(hello, probeMs).whenComplete((resp, err) -> {
                if (err != null || resp == null || !resp.hasHelloResponse()) {
                    finish(ch, result, err);
                    return;
                }
                HelloResponse hr = resp.getHelloResponse();
                if (hr.getStatus() != StatusCode.OK || hr.getLeaderHint() <= 0) {
                    finish(ch, result, null); // 握手失败/无主/单机（hint=0）：换点
                    return;
                }
                if (!hr.getLeaderAddress().isEmpty()) {
                    ClientConfig.SeedAddress addr = parse(hr.getLeaderAddress());
                    if (addr != null) {
                        finishWith(ch, result, addr);
                    } else {
                        finish(ch, result, null);
                    }
                    return;
                }
                // 地址未配置：CLUSTER_VIEW 取 Leader 自报地址（design D4 降级路径）。
                Envelope viewReq = Envelope.newBuilder()
                        .setProtocolVersion(2)
                        .setType(MessageType.CLUSTER_VIEW)
                        .setRequestId(sc.nextRequestId())
                        .build();
                mux.sendWithId(viewReq, probeMs).whenComplete((vResp, vErr) -> {
                    ClientConfig.SeedAddress found = null;
                    if (vErr == null && vResp != null && vResp.hasClusterView()
                            && vResp.getClusterView().getStatus() == StatusCode.OK) {
                        for (NodeInfo n : vResp.getClusterView().getNodesList()) {
                            if (n.getIsLeader() && !n.getAddress().isEmpty()) {
                                found = parse(n.getAddress());
                                if (found != null) {
                                    break;
                                }
                            }
                        }
                    }
                    if (found != null) {
                        finishWith(ch, result, found);
                    } else {
                        finish(ch, result, vErr);
                    }
                });
            });
        });
        return result;
    }

    /**
     * 关闭探针连接并以失败完成（内部信号，不外抛给调用方）。
     *
     * @param ch  探针连接
     * @param r   结果 future
     * @param err 失败原因；{@code null} 时以通用原因完成
     */
    private static void finish(Channel ch, CompletableFuture<ClientConfig.SeedAddress> r,
                               Throwable err) {
        ch.close();
        r.completeExceptionally(err == null
                ? new DiscoveryFailedException("seed unreachable or no leader") : err);
    }

    /**
     * 关闭探针连接并以命中地址完成。
     *
     * @param ch   探针连接
     * @param r    结果 future
     * @param addr 发现的 Leader 地址
     */
    private static void finishWith(Channel ch,
                                   CompletableFuture<ClientConfig.SeedAddress> r,
                                   ClientConfig.SeedAddress addr) {
        ch.close();
        r.complete(addr);
    }

    /**
     * 解析 {@code host:port} 接入地址。
     *
     * @param address 地址串
     * @return 种子地址；形态非法为 {@code null}
     */
    public static ClientConfig.SeedAddress parse(String address) {
        int colon = address.lastIndexOf(':');
        if (colon <= 0 || colon == address.length() - 1) {
            return null;
        }
        try {
            return new ClientConfig.SeedAddress(address.substring(0, colon),
                    Integer.parseInt(address.substring(colon + 1)));
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /** 发现穷尽（无种子报告可用 Leader）：调用方以快速失败语义处理。 */
    public static final class DiscoveryFailedException extends RuntimeException {
        /** 序列化标识。 */
        private static final long serialVersionUID = 1L;

        /**
         * 以失败摘要构造。
         *
         * @param message 失败摘要
         */
        public DiscoveryFailedException(String message) {
            super(message);
        }
    }
}
