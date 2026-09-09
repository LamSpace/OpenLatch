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

package io.github.lamspace.openlatch.server.admin;

import io.github.lamspace.openlatch.core.CoreEngine;
import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.SystemClock;
import io.github.lamspace.openlatch.core.command.AcquireCommand;
import io.github.lamspace.openlatch.core.command.LatchAwaitCommand;
import io.github.lamspace.openlatch.core.command.LatchCountDownCommand;
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.protocol.AdminKeyDetailResponse;
import io.github.lamspace.openlatch.protocol.AdminKeyInfo;
import io.github.lamspace.openlatch.protocol.AdminListKeysResponse;
import io.github.lamspace.openlatch.protocol.AdminListSessionsResponse;
import io.github.lamspace.openlatch.protocol.AdminSessionInfo;
import io.github.lamspace.openlatch.protocol.AdminSummaryResponse;
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.AdminConfig;
import io.github.lamspace.openlatch.server.OpenLatchServer;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.metrics.ServerMetrics;
import io.github.lamspace.openlatch.server.net.ServerSessionHandler;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §7 T3 管理协议消息级用例（单机档，spec {@code admin-observability}）：
 * EmbeddedChannel 直驱完整接入层（握手 → ADMIN 早退分支 → 管理处理器），
 * 覆盖认证矩阵（对/错/空/未配置/握手前/低版本）、四消息逐字段断言、
 * 分页与过滤边界、管理流量指标零污染与 ADMIN 的在途限额。引擎状态经
 * {@code CoreEngine} 直驱预置（生产系统时钟，仅观察语义不受时钟影响）。
 */
class AdminProtocolTest {

    /** 测试管理令牌。 */
    private static final String TOKEN = "t3-admin-secret";

    /** 被测嵌入通道（完整接入层装配）。 */
    private EmbeddedChannel ch;
    /** 被测核心引擎（状态预置与观察面断言共用）。 */
    private CoreEngine core;
    /** 被测会话注册表。 */
    private ServerSessionRegistry registry;
    /** 被测指标门面（零污染断言的 scrape 来源）。 */
    private ServerMetrics metrics;
    /** 握手后有效会话 id。 */
    private long sessionId;

    @BeforeEach
    void setUp() {
        ServerConfig config = ServerConfig.defaults();
        registry = new ServerSessionRegistry();
        core = new CoreEngine(new CoreConfig(), new SystemClock(), (s, r, k) -> { });
        metrics = new ServerMetrics();
        ch = channelFor(config, new AdminConfig(TOKEN));
        ch.pipeline().fireChannelActive();
        sessionId = hello(1, 3);
    }

    /**
     * 以给定配置与令牌装配一条接入通道（独立于 {@link #ch} 的用例专用）。
     *
     * @param config 服务器配置
     * @param admin  管理配置（令牌）
     * @return 已触发 channelActive 的嵌入通道
     */
    private EmbeddedChannel channelFor(ServerConfig config, AdminConfig admin) {
        CoreEngine engine = this.core;
        ServerSessionHandler handler = new ServerSessionHandler(engine, config, registry,
                new RequestDispatcher(engine, metrics), null,
                new AdminRequestHandler(admin, engine, null, registry, () -> 4242L));
        EmbeddedChannel c = new EmbeddedChannel(handler);
        c.pipeline().fireChannelActive();
        return c;
    }

    /**
     * 完成一次 HELLO 并回读分配的会话 id。
     *
     * @param rid     请求 id
     * @param version 客户端协议版本
     * @return sessionId
     */
    private long helloOn(EmbeddedChannel c, long rid, int version) {
        c.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(version).setType(MessageType.HELLO).setRequestId(rid)
                .setHelloRequest(HelloRequest.newBuilder().setClientProtocolVersion(version)
                        .setClientName("console"))
                .build());
        Envelope resp = outbound(c);
        assertThat(resp.getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);
        return resp.getHelloResponse().getSessionId();
    }

    /** {@link #ch} 上的便捷 HELLO。 */
    private long hello(long rid, int version) {
        return helloOn(ch, rid, version);
    }

    /** ADMIN_SUMMARY 请求信封。 */
    private static Envelope adminSummary(long rid, int version, String token) {
        return Envelope.newBuilder().setProtocolVersion(version)
                .setType(MessageType.ADMIN_SUMMARY).setRequestId(rid)
                .setAdminSummaryRequest(io.github.lamspace.openlatch.protocol
                        .AdminSummaryRequest.newBuilder().setToken(token))
                .build();
    }

    /** ADMIN_LIST_KEYS 请求信封。 */
    private static Envelope adminListKeys(long rid, String token, int page, int pageSize, String prefix) {
        return Envelope.newBuilder().setProtocolVersion(3)
                .setType(MessageType.ADMIN_LIST_KEYS).setRequestId(rid)
                .setAdminListKeysRequest(io.github.lamspace.openlatch.protocol
                        .AdminListKeysRequest.newBuilder()
                        .setToken(token).setPage(page).setPageSize(pageSize).setPrefix(prefix))
                .build();
    }

    /** ADMIN_KEY_DETAIL 请求信封。 */
    private static Envelope adminKeyDetail(long rid, String token, String key) {
        return Envelope.newBuilder().setProtocolVersion(3)
                .setType(MessageType.ADMIN_KEY_DETAIL).setRequestId(rid)
                .setAdminKeyDetailRequest(io.github.lamspace.openlatch.protocol
                        .AdminKeyDetailRequest.newBuilder().setToken(token).setKey(key))
                .build();
    }

    /** ADMIN_LIST_SESSIONS 请求信封。 */
    private static Envelope adminListSessions(long rid, String token) {
        return Envelope.newBuilder().setProtocolVersion(3)
                .setType(MessageType.ADMIN_LIST_SESSIONS).setRequestId(rid)
                .setAdminListSessionsRequest(io.github.lamspace.openlatch.protocol
                        .AdminListSessionsRequest.newBuilder().setToken(token))
                .build();
    }

    /** 读一条出站信封。 */
    private static Envelope outbound(EmbeddedChannel c) {
        Object out = c.readOutbound();
        assertThat(out).isInstanceOf(Envelope.class);
        return (Envelope) out;
    }

    /** 经 {@link #ch} 发送 ADMIN 请求并读应答（默认正确令牌）。 */
    private Envelope admin(Envelope msg) {
        ch.writeInbound(msg);
        return outbound(ch);
    }

    // ===================== 认证矩阵（spec"管理令牌认证"） =====================

    @Test
    void wrongToken_rejected_andDisconnected() {
        Envelope resp = admin(adminSummary(9, 3, "wrong"));
        assertThat(resp.getType()).isEqualTo(MessageType.ADMIN_SUMMARY);
        assertThat(resp.getRequestId()).isEqualTo(9);
        assertThat(resp.getProtocolVersion()).isEqualTo(3);
        assertThat(resp.getAdminSummaryResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isFalse();
    }

    @Test
    void emptyToken_rejected_andDisconnected() {
        admin(adminSummary(9, 3, ""));
        assertThat(ch.isOpen()).isFalse();
    }

    @Test
    void unconfiguredAdmin_rejectsEveryToken() {
        EmbeddedChannel c = channelFor(ServerConfig.defaults(), AdminConfig.unconfigured());
        helloOn(c, 2, 3); // helloOn 内部已消费并断言 HELLO 应答
        c.writeInbound(adminSummary(3, 3, TOKEN));
        Envelope r = outbound(c);
        assertThat(r.getAdminSummaryResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(c.isOpen()).isFalse();
    }

    @Test
    void adminBeforeHandshake_rejected_withoutDisconnect() {
        EmbeddedChannel c = channelFor(ServerConfig.defaults(), new AdminConfig(TOKEN));
        c.writeInbound(adminSummary(4, 3, TOKEN));
        Envelope r = outbound(c);
        // 握手门闩既有规则覆盖 ADMIN：INVALID_REQUEST（v3-T3 起状态码随
        // 类型化应答在线路可见）、不断连、可补发 HELLO。
        assertThat(r.getRequestId()).isEqualTo(4);
        assertThat(r.getAdminSummaryResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(c.isOpen()).isTrue();
        helloOn(c, 5, 3); // 补发合法 HELLO 成功（门闩语义：拒绝不断连）
    }

    @Test
    void v2Session_adminRejected_messageLevel_notDisconnected() {
        EmbeddedChannel c = channelFor(ServerConfig.defaults(), new AdminConfig(TOKEN));
        long sid = helloOn(c, 1, 2);
        c.writeInbound(adminSummary(6, 2, TOKEN));
        Envelope r = outbound(c);
        assertThat(r.getAdminSummaryResponse().getStatus())
                .isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(c.isOpen()).isTrue();
        // 该会话既有 v2 业务路径照常服务（v1/v2 行为不变，验收 §8-6）。
        c.writeInbound(Envelope.newBuilder().setProtocolVersion(2)
                .setType(MessageType.LOCK_ACQUIRE).setRequestId(7)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey("biz").setThreadId(1))
                .build());
        Envelope biz = outbound(c);
        assertThat(biz.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(sid).isPositive();
    }

    // ===================== ADMIN_SUMMARY =====================

    @Test
    void summary_reportsStandaloneAggregateAndIdentity() {
        long other = core.sessionOpened();
        core.acquire(new AcquireCommand(sessionId, 100, "lk", LockType.REENTRANT, 11, 30_000, true));
        core.acquire(new AcquireCommand(other, 101, "lk", LockType.REENTRANT, 12, 30_000, true));
        core.acquire(new AcquireCommand(other, 102, "sem", LockType.SEMAPHORE,
                12, 30_000, true, 1, 2));
        core.latchAwait(new LatchAwaitCommand(other, 103, "gate", 2));

        AdminSummaryResponse s = admin(adminSummary(8, 3, TOKEN)).getAdminSummaryResponse();
        assertThat(s.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(s.getHeldLocks()).isEqualTo(1);
        assertThat(s.getHeldSemaphores()).isEqualTo(1);
        assertThat(s.getLatchEntries()).isEqualTo(1);
        assertThat(s.getTotalWaiters()).isEqualTo(2); // lk 一个锁等待 + gate 一个 awaiter
        assertThat(s.getSessionCount()).isEqualTo(1); // 本节点接入会话（不含裸引擎会话）
        assertThat(s.getNodeRole()).isEqualTo("SINGLE");
        assertThat(s.getUptimeMs()).isEqualTo(4242L);
        assertThat(s.getVersion()).isEqualTo(OpenLatchServer.serverVersion());
    }

    // ===================== ADMIN_LIST_KEYS =====================

    @Test
    void listKeys_sortedFilteredPaged() {
        seedKeySet();
        AdminListKeysResponse all = admin(adminListKeys(11, TOKEN, 0, 10, ""))
                .getAdminListKeysResponse();
        assertThat(all.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(all.getTotalMatched()).isEqualTo(3);
        assertThat(all.getItemsList()).extracting(AdminKeyInfo::getKey)
                .containsExactly("job:a", "order:b", "order:c");
        // 行级字段：持有者计数与等待者数按引擎状态如实。
        AdminKeyInfo lkOrderB = all.getItemsList().stream()
                .filter(i -> i.getKey().equals("order:b")).findFirst().orElseThrow();
        assertThat(lkOrderB.getFamily()).isEqualTo("lock");
        assertThat(lkOrderB.getHolders()).isEqualTo(1);
        assertThat(lkOrderB.getWaiterCount()).isEqualTo(1);
        assertThat(lkOrderB.getRemainingLeaseMs()).isBetween(1L, 30_000L);

        AdminListKeysResponse prefixed = admin(adminListKeys(12, TOKEN, 0, 10, "order:"))
                .getAdminListKeysResponse();
        assertThat(prefixed.getTotalMatched()).isEqualTo(2);
        assertThat(prefixed.getItemsList()).extracting(AdminKeyInfo::getKey)
                .containsExactly("order:b", "order:c");

        // 分页遍历：末页不满、超页空集，字典序全局一致无重叠。
        AdminListKeysResponse p0 = admin(adminListKeys(13, TOKEN, 0, 2, ""))
                .getAdminListKeysResponse();
        assertThat(p0.getItemsList()).hasSize(2);
        AdminListKeysResponse p1 = admin(adminListKeys(14, TOKEN, 1, 2, ""))
                .getAdminListKeysResponse();
        assertThat(p1.getItemsList()).extracting(AdminKeyInfo::getKey).containsExactly("order:c");
        AdminListKeysResponse p9 = admin(adminListKeys(15, TOKEN, 9, 2, ""))
                .getAdminListKeysResponse();
        assertThat(p9.getItemsList()).isEmpty();
        assertThat(p9.getTotalMatched()).isEqualTo(3);
    }

    @Test
    void listKeys_invalidPagingParams_messageLevelReject() {
        Envelope r1 = admin(adminListKeys(16, TOKEN, -1, 10, ""));
        assertThat(r1.getAdminListKeysResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        Envelope r2 = admin(adminListKeys(17, TOKEN, 0, 0, ""));
        assertThat(r2.getAdminListKeysResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        Envelope r3 = admin(adminListKeys(18, TOKEN, 0, AdminRequestHandler.MAX_PAGE_SIZE + 1, ""));
        assertThat(r3.getAdminListKeysResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue(); // 分页参数非法只拒消息，不断连（区别于令牌失败）
    }

    /** 预置三个 key（字典序：job:a < order:b < order:c），order:b 带一个等待者。 */
    private void seedKeySet() {
        core.acquire(new AcquireCommand(sessionId, 200, "order:b", LockType.REENTRANT, 21, 30_000, true));
        long w = core.sessionOpened();
        core.acquire(new AcquireCommand(w, 201, "order:b", LockType.REENTRANT, 22, 30_000, true));
        core.acquire(new AcquireCommand(sessionId, 202, "order:c", LockType.SIMPLE, 21, 30_000, true));
        core.acquire(new AcquireCommand(sessionId, 203, "job:a", LockType.READ, 21, 30_000, true));
    }

    // ===================== ADMIN_KEY_DETAIL =====================

    @Test
    void keyDetail_reentrantWithWaiter_allFields() {
        core.acquire(new AcquireCommand(sessionId, 210, "lk", LockType.REENTRANT, 31, 30_000, true));
        core.acquire(new AcquireCommand(sessionId, 211, "lk", LockType.REENTRANT, 31, 30_000, true));
        long waiter = core.sessionOpened();
        core.acquire(new AcquireCommand(waiter, 212, "lk", LockType.WRITE, 32, 30_000, true));

        AdminKeyDetailResponse d = admin(adminKeyDetail(19, TOKEN, "lk")).getAdminKeyDetailResponse();
        assertThat(d.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(d.getFamily()).isEqualTo("lock");
        assertThat(d.getHoldersList()).singleElement().satisfies(h -> {
            assertThat(h.getSessionId()).isEqualTo(sessionId);
            assertThat(h.getThreadId()).isEqualTo(31);
            assertThat(h.getCount()).isEqualTo(2);
            assertThat(h.getRole()).isEqualTo("writer");
        });
        assertThat(d.getWaitersList()).singleElement().satisfies(w -> {
            assertThat(w.getPosition()).isEqualTo(1);
            assertThat(w.getSessionId()).isEqualTo(waiter);
            assertThat(w.getRequestId()).isEqualTo(212);
            assertThat(w.getPermits()).isEqualTo(1);
            assertThat(w.getWaitedMs()).isBetween(0L, 30_000L);
        });
        assertThat(d.getLeaseExpiresAtMs()).isPositive();
        assertThat(d.getRemainingLeaseMs()).isBetween(1L, 30_000L);
        assertThat(d.getWaitQueueLeaderOnly()).isFalse(); // 单机等待队列恒可见
    }

    @Test
    void keyDetail_semaphoreAndLatchAndMissing() {
        core.acquire(new AcquireCommand(sessionId, 220, "sem", LockType.SEMAPHORE,
                41, 30_000, true, 2, 5));
        AdminKeyDetailResponse sem = admin(adminKeyDetail(20, TOKEN, "sem"))
                .getAdminKeyDetailResponse();
        assertThat(sem.getFamily()).isEqualTo("semaphore");
        assertThat(sem.getPermitsTotal()).isEqualTo(5);
        assertThat(sem.getPermitsAvailable()).isEqualTo(3);
        assertThat(sem.getHoldersList()).singleElement().satisfies(h -> {
            assertThat(h.getRole()).isEqualTo("holder");
            assertThat(h.getCount()).isEqualTo(2);
        });

        long p = core.sessionOpened();
        core.latchAwait(new LatchAwaitCommand(p, 221, "gate", 3));
        core.countDown(new LatchCountDownCommand(p, "gate", 1, 0));
        AdminKeyDetailResponse gate = admin(adminKeyDetail(21, TOKEN, "gate"))
                .getAdminKeyDetailResponse();
        assertThat(gate.getFamily()).isEqualTo("latch");
        assertThat(gate.getLatchTotal()).isEqualTo(3);
        assertThat(gate.getLatchRemaining()).isEqualTo(2);
        assertThat(gate.getLeaseExpiresAtMs()).isZero();
        assertThat(gate.getWaitersList()).singleElement().satisfies(w -> {
            assertThat(w.getSessionId()).isEqualTo(p);
            assertThat(w.getWaitedMs()).isBetween(0L, 30_000L);
        });

        // 未命中：明确 NOT_HELD 的完整明细应答（非空壳错误帧），不断连。
        Envelope miss = admin(adminKeyDetail(22, TOKEN, "nope"));
        assertThat(miss.getType()).isEqualTo(MessageType.ADMIN_KEY_DETAIL);
        assertThat(miss.getAdminKeyDetailResponse().getStatus()).isEqualTo(StatusCode.NOT_HELD);
        assertThat(ch.isOpen()).isTrue();
    }

    // ===================== ADMIN_LIST_SESSIONS =====================

    @Test
    void listSessions_coversOnlyLocallyConnected() {
        long rawSession = core.sessionOpened(); // 裸引擎会话：无连接，不得出现
        core.acquire(new AcquireCommand(sessionId, 230, "own", LockType.REENTRANT, 51, 30_000, true));
        core.acquire(new AcquireCommand(rawSession, 231, "own", LockType.REENTRANT, 52, 30_000, true));

        List<AdminSessionInfo> list = admin(adminListSessions(23, TOKEN))
                .getAdminListSessionsResponse().getSessionsList();
        assertThat(list).singleElement().satisfies(s -> {
            assertThat(s.getSessionId()).isEqualTo(sessionId);
            assertThat(s.getNodeId()).isZero(); // 单机无集群身份
            assertThat(s.getConnectedAtMs()).isPositive().isLessThanOrEqualTo(System.currentTimeMillis());
            assertThat(s.getHeldKeys()).isEqualTo(1);
            assertThat(s.getWaitingKeys()).isZero();
        });
    }

    @Test
    void listSessions_multipleConnectionsOnSameNode() {
        EmbeddedChannel c2 = channelFor(ServerConfig.defaults(), new AdminConfig(TOKEN));
        long sid2 = helloOn(c2, 30, 3);
        core.acquire(new AcquireCommand(sid2, 240, "two", LockType.REENTRANT, 61, 30_000, true));
        // 经第二条连接发 ADMIN（c2 与 ch 共享 registry）。
        c2.writeInbound(adminListSessions(31, TOKEN));
        AdminListSessionsResponse r = outbound(c2).getAdminListSessionsResponse();
        assertThat(r.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(r.getSessionsList()).hasSize(2);
        assertThat(r.getSessionsList()).extracting(AdminSessionInfo::getSessionId)
                .containsExactlyInAnyOrder(sessionId, sid2);
        AdminSessionInfo s2 = r.getSessionsList().stream()
                .filter(s -> s.getSessionId() == sid2).findFirst().orElseThrow();
        assertThat(s2.getHeldKeys()).isEqualTo(1);
    }

    // ===================== 隔离与自我保护（spec"管理流量与业务面隔离"） =====================

    @Test
    void adminTraffic_doesNotPolluteMetrics() {
        // 业务请求先落指标（scrape 基线含业务计数）。
        ch.writeInbound(Envelope.newBuilder().setProtocolVersion(3)
                .setType(MessageType.LOCK_ACQUIRE).setRequestId(40)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey("biz").setThreadId(1))
                .build());
        assertThat(outbound(ch).getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        String baseline = metrics.registry().scrape();
        assertThat(baseline).contains("openlatch_server_acquire_total");

        admin(adminSummary(41, 3, TOKEN));
        admin(adminListKeys(42, TOKEN, 0, 50, ""));
        admin(adminKeyDetail(43, TOKEN, "biz"));
        admin(adminListSessions(44, TOKEN));
        EmbeddedChannel bad = channelFor(ServerConfig.defaults(), new AdminConfig(TOKEN));
        helloOn(bad, 45, 3);
        bad.writeInbound(adminSummary(46, 3, "wrong")); // 被拒路径同样零污染
        outbound(bad);

        assertThat(metrics.registry().scrape()).isEqualTo(baseline);
    }

    @Test
    void inflightLimit_protectsAdminChannel() {
        ServerConfig tight = new ServerConfig(0, 1, 60_000L, 30_000L, 1_000L, 3_600_000L,
                500L, 5_000L, 512, 4096, 1); // maxInflightPerConnection = 1
        HangingWriter hanging = new HangingWriter();
        EmbeddedChannel c = new EmbeddedChannel(hanging, new ServerSessionHandler(
                core, tight, registry, new RequestDispatcher(core, metrics), null,
                new AdminRequestHandler(new AdminConfig(TOKEN), core, null, registry, () -> 1L)));
        c.pipeline().fireChannelActive();
        c.writeInbound(Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.HELLO)
                .setRequestId(50)
                .setHelloRequest(HelloRequest.newBuilder().setClientProtocolVersion(3))
                .build());
        assertThat(hanging.messages).hasSize(1); // 握手应答不占在途记账
        c.writeInbound(adminSummary(51, 3, TOKEN)); // 应答滞留未完成 → 在途保持 1
        c.writeInbound(adminSummary(52, 3, TOKEN)); // 超限
        assertThat(hanging.messages).hasSize(3);
        Envelope over = (Envelope) hanging.messages.peekLast();
        assertThat(over.getRequestId()).isEqualTo(52);
        assertThat(over.getAdminSummaryResponse().getStatus()).isEqualTo(StatusCode.OVERLOADED);
        Envelope first = (Envelope) hanging.messages.toArray()[1];
        assertThat(first.getRequestId()).isEqualTo(51);
        assertThat(first.getAdminSummaryResponse().getStatus()).isEqualTo(StatusCode.OK);
        assertThat(c.isOpen()).isTrue();
    }

    /**
     * 出站滞留器（与 {@code InflightOverloadTest} 同法）：收下写请求但不完成
     * promise，使在途记账保持——ADMIN 的限额保护由此可观测。
     */
    private static final class HangingWriter
            extends io.netty.channel.ChannelOutboundHandlerAdapter {
        /** 已滞留的出站消息。 */
        final java.util.Deque<Object> messages = new java.util.ArrayDeque<>();

        @Override
        public void write(io.netty.channel.ChannelHandlerContext ctx, Object msg,
                          io.netty.channel.ChannelPromise promise) {
            messages.add(msg); // 故意不透传：promise 永不完成
        }
    }

    @Test
    void adminDoesNotDisturbBusinessState() {
        long token = core.acquire(new AcquireCommand(sessionId, 250, "k",
                LockType.REENTRANT, 71, 30_000, true)).leaseToken();
        long beforeExpires = core.stats().heldLocks();
        admin(adminSummary(60, 3, TOKEN));
        admin(adminKeyDetail(61, TOKEN, "k"));
        // 观察不刷新租约、不换手凭证：业务释放照常成功。
        assertThat(core.stats().heldLocks()).isEqualTo(beforeExpires);
        core.release(new ReleaseCommand(sessionId, "k", token, 71));
        assertThat(core.stats().heldLocks()).isZero();
    }
}
