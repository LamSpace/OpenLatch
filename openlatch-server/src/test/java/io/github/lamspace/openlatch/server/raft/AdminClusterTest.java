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

package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.protocol.AdminKeyDetailResponse;
import io.github.lamspace.openlatch.protocol.AdminListKeysResponse;
import io.github.lamspace.openlatch.protocol.AdminListSessionsResponse;
import io.github.lamspace.openlatch.protocol.AdminSummaryRequest;
import io.github.lamspace.openlatch.protocol.AdminSummaryResponse;
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.AdminConfig;
import io.github.lamspace.openlatch.server.admin.AdminRequestHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §7 T3 管理协议集群档消息级用例（spec"双形态数据源与集群视角口径"）：
 * 三节点 {@link ClusterHarness} 基座上直驱 {@link AdminRequestHandler}
 * （每节点一个处理器实例，数据源为该节点运行时）——覆盖复制态镜像跨
 * 副本可查、等待队列 Leader 专属标注、会话列表按接入节点分治、SUMMARY
 * 角色如实，并钉住"管理查询零日志条目"（关闭探针后比对各节点
 * lastApplied 不动点）。
 */
class AdminClusterTest {

    /** 测试管理令牌。 */
    private static final String TOKEN = "cluster-admin-t3";

    /** 三节点集群基座。 */
    private ClusterHarness harness;

    @BeforeEach
    void setUp() throws IOException {
        harness = ClusterHarness.start(3);
    }

    @AfterEach
    void tearDown() {
        harness.close();
    }

    /** 为节点装配管理处理器（集群数据源形态）。 */
    private AdminRequestHandler handlerFor(ClusterHarness.Node node) {
        return new AdminRequestHandler(new AdminConfig(TOKEN), null, node.runtime,
                node.registry, () -> 77L);
    }

    /** 同步发一条 ADMIN 请求并取回应答（经测试连接的直驱通道）。 */
    private Envelope admin(ClusterHarness.Node node, AdminRequestHandler handler,
                           ClusterHarness.TestConn conn, Envelope msg) {
        handler.handle(conn.ctx, conn.session, msg);
        return conn.awaitOutbound(10_000);
    }

    /** ADMIN_SUMMARY 请求。 */
    private static Envelope summary(long rid) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.ADMIN_SUMMARY)
                .setRequestId(rid)
                .setAdminSummaryRequest(AdminSummaryRequest.newBuilder().setToken(TOKEN))
                .build();
    }

    /** ADMIN_LIST_KEYS 请求。 */
    private static Envelope listKeys(long rid, String prefix) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.ADMIN_LIST_KEYS)
                .setRequestId(rid)
                .setAdminListKeysRequest(io.github.lamspace.openlatch.protocol
                        .AdminListKeysRequest.newBuilder()
                        .setToken(TOKEN).setPage(0).setPageSize(100).setPrefix(prefix))
                .build();
    }

    /** ADMIN_KEY_DETAIL 请求。 */
    private static Envelope keyDetail(long rid, String key) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.ADMIN_KEY_DETAIL)
                .setRequestId(rid)
                .setAdminKeyDetailRequest(io.github.lamspace.openlatch.protocol
                        .AdminKeyDetailRequest.newBuilder().setToken(TOKEN).setKey(key))
                .build();
    }

    /** ADMIN_LIST_SESSIONS 请求。 */
    private static Envelope listSessions(long rid) {
        return Envelope.newBuilder().setProtocolVersion(3)
                .setType(MessageType.ADMIN_LIST_SESSIONS).setRequestId(rid)
                .setAdminListSessionsRequest(io.github.lamspace.openlatch.protocol
                        .AdminListSessionsRequest.newBuilder().setToken(TOKEN))
                .build();
    }

    /** 排队式 ACQUIRE 信封。 */
    private static Envelope acquire(long rid, String key, long threadId, long waitMs) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey(key).setThreadId(threadId).setWaitMs(waitMs))
                .build();
    }

    @Test
    void clusterAdmin_queriesAnsweredLocallyWithLogicalSessionIds() throws Exception {
        ClusterHarness.Node leader = harness.leader();
        ClusterHarness.Node follower = harness.nodes().stream()
                .filter(n -> !n.isLeader()).findFirst().orElseThrow();

        // Leader 上建立连接并授予；同 key 第二个连接排队。
        ClusterHarness.TestConn holder = harness.connect(leader);
        long holderSid = holder.hello(1, 3).getHelloResponse().getSessionId();
        Envelope granted = holder.request(acquire(2, "ck", 11, -1));
        assertThat(granted.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
        ClusterHarness.TestConn waiter = harness.connect(leader);
        long waiterSid = waiter.hello(3, 3).getHelloResponse().getSessionId();
        Envelope queued = waiter.request(acquire(4, "ck", 12, -1));
        assertThat(queued.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);

        AdminRequestHandler leaderHandler = handlerFor(leader);
        // Leader SUMMARY：角色与聚合如实。
        AdminSummaryResponse s = admin(leader, leaderHandler, holder, summary(5))
                .getAdminSummaryResponse();
        assertThat(s.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(s.getNodeRole()).isEqualTo("LEADER");
        assertThat(s.getHeldLocks()).isEqualTo(1);
        assertThat(s.getTotalWaiters()).isEqualTo(1);
        assertThat(s.getSessionCount()).isEqualTo(2);
        assertThat(s.getUptimeMs()).isEqualTo(77L);

        // LIST_KEYS：逻辑会话 id 口径（高位 = Leader nodeId）。
        AdminListKeysResponse lk = admin(leader, leaderHandler, holder, listKeys(6, ""))
                .getAdminListKeysResponse();
        assertThat(lk.getTotalMatched()).isEqualTo(1);
        assertThat(lk.getItemsList().get(0).getKey()).isEqualTo("ck");
        assertThat(lk.getItemsList().get(0).getWaiterCount()).isEqualTo(1);

        // KEY_DETAIL Leader：等待位次可见、归属为逻辑会话。
        AdminKeyDetailResponse kd = admin(leader, leaderHandler, holder, keyDetail(7, "ck"))
                .getAdminKeyDetailResponse();
        assertThat(kd.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(kd.getWaitQueueLeaderOnly()).isFalse();
        assertThat(kd.getHoldersList().get(0).getSessionId()).isEqualTo(holderSid);
        assertThat(kd.getWaitersList().get(0).getSessionId()).isEqualTo(waiterSid);
        assertThat(kd.getWaitersList().get(0).getPosition()).isEqualTo(1);
        // 逻辑会话高位 = 接入节点 id（spec"接入节点由会话 id 高位导出"）。
        assertThat(holderSid >>> 32).isEqualTo(leader.id);

        // 会话列表分治：Leader 列 2、Follower 列 0。
        AdminListSessionsResponse ls = admin(leader, leaderHandler, holder, listSessions(8))
                .getAdminListSessionsResponse();
        assertThat(ls.getSessionsList()).hasSize(2);
        assertThat(ls.getSessionsList()).allSatisfy(x ->
                assertThat(x.getNodeId()).isEqualTo(leader.id));

        // Follower：镜像收敛后可查、等待区标注"仅 Leader 可见"、角色如实。
        ClusterHarness.TestConn fConn = harness.connect(follower);
        fConn.hello(9, 3);
        AdminRequestHandler followerHandler = handlerFor(follower);
        harness.awaitTrue(() -> admin(follower, followerHandler, fConn, listKeys(10, ""))
                .getAdminListKeysResponse().getTotalMatched() >= 1, 10_000, "follower 镜像收敛");
        AdminKeyDetailResponse fkd = admin(follower, followerHandler, fConn, keyDetail(11, "ck"))
                .getAdminKeyDetailResponse();
        assertThat(fkd.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(fkd.getHoldersList().get(0).getSessionId()).isEqualTo(holderSid);
        assertThat(fkd.getWaitersList()).isEmpty();
        assertThat(fkd.getWaitQueueLeaderOnly()).isTrue();
        AdminSummaryResponse fs = admin(follower, followerHandler, fConn, summary(12))
                .getAdminSummaryResponse();
        assertThat(fs.getNodeRole()).isEqualTo("FOLLOWER");
        assertThat(fs.getTotalWaiters()).isZero();
        assertThat(admin(follower, followerHandler, fConn, listSessions(13))
                .getAdminListSessionsResponse().getSessionsList()).hasSize(1);
    }

    @Test
    void adminQueries_produceNoLogEntries() {
        harness.setProbesEnabled(false); // 排除 NOOP 探针噪声（harness 既定手法）
        ClusterHarness.Node leader = harness.leader();
        ClusterHarness.TestConn conn = harness.connect(leader);
        conn.hello(1, 3);
        conn.request(acquire(2, "still", 21, -1)); // 授予产生条目（计入基线前）

        AdminRequestHandler handler = handlerFor(leader);
        List<Long> before = harness.nodes().stream().map(ClusterHarness.Node::lastApplied).toList();
        admin(leader, handler, conn, summary(3));
        admin(leader, handler, conn, listKeys(4, ""));
        admin(leader, handler, conn, keyDetail(5, "still"));
        admin(leader, handler, conn, listSessions(6));
        admin(leader, handler, conn, summary(7));
        List<Long> after = harness.nodes().stream().map(ClusterHarness.Node::lastApplied).toList();
        assertThat(after).isEqualTo(before); // spec"管理查询不产生复制日志条目"
        harness.setProbesEnabled(true);
    }
}
