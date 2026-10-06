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
import io.github.lamspace.openlatch.protocol.AdminSummaryResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.PhaserOp;
import io.github.lamspace.openlatch.protocol.PhaserOpRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.AdminConfig;
import io.github.lamspace.openlatch.server.admin.AdminRequestHandler;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * phaser 管理观察夹具（v10）：Leader 视角 SUMMARY {@code phaser_entries}、
 * LIST_KEYS {@code family=phaser} 行三计数与等待数、KEY_DETAIL 三计数+配额
 * 明细+等待明细（expected_phase）；观察零扰动（查询不推进相位/清扫等待）；
 * Follower 视角账簿与配额照常（复制态命中）、等待明细如实零并同口径标注
 * （双速呈现）；换主窗口账簿即时完整而等待明细仅含重挂（过渡如实）。
 * 判例 {@code TopicAdminTest}（单 harness 双视角，控端口周转）。
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class PhaserAdminTest {

    /** 管理令牌。 */
    private static final String TOKEN = "phaser-admin-token";

    /** 三节点基座。 */
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

    /** 同步发一条 ADMIN 请求并取回应答。 */
    private Envelope admin(ClusterHarness.Node node, AdminRequestHandler handler,
                           ClusterHarness.TestConn conn, Envelope msg) {
        handler.handle(conn.ctx, conn.session, msg);
        return conn.awaitOutbound(10_000);
    }

    private static Envelope summaryMsg() {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.ADMIN_SUMMARY)
                .setRequestId(1).setAdminSummaryRequest(
                        io.github.lamspace.openlatch.protocol.AdminSummaryRequest.newBuilder()
                                .setToken(TOKEN)).build();
    }

    private static Envelope listMsg() {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.ADMIN_LIST_KEYS)
                .setRequestId(2).setAdminListKeysRequest(
                        io.github.lamspace.openlatch.protocol.AdminListKeysRequest.newBuilder()
                                .setToken(TOKEN).setPage(0).setPageSize(100).setPrefix(""))
                .build();
    }

    private static Envelope detailMsg(String key) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.ADMIN_KEY_DETAIL)
                .setRequestId(3).setAdminKeyDetailRequest(
                        io.github.lamspace.openlatch.protocol.AdminKeyDetailRequest.newBuilder()
                                .setToken(TOKEN).setKey(key)).build();
    }

    private static Envelope phaser(long rid, PhaserOp op, String key, int parties,
            Long expected) {
        PhaserOpRequest.Builder rb = PhaserOpRequest.newBuilder()
                .setKey(key).setOp(op).setParties(parties);
        if (expected != null) {
            rb.setExpectedPhase(expected);
        }
        return Envelope.newBuilder().setProtocolVersion(10).setType(MessageType.PHASER_OP)
                .setRequestId(rid).setPhaserOpRequest(rb).build();
    }

    private static StatusCode status(Envelope resp) {
        return resp.getPhaserOpResponse().getStatus();
    }

    /** 业务请求：按同 rid 同型过滤先到的 AWAIT_NOTIFY（判例 ClusterPhaserTest）。 */
    private static Envelope send(ClusterHarness.TestConn conn, Envelope msg) {
        Envelope first = conn.request(msg);
        if (first.getType() == msg.getType() && first.getRequestId() == msg.getRequestId()) {
            return first;
        }
        for (int i = 0; i < 16; i++) {
            Envelope next = conn.awaitOutbound(10_000);
            if (next.getType() == msg.getType() && next.getRequestId() == msg.getRequestId()) {
                return next;
            }
        }
        throw new AssertionError("reply for rid " + msg.getRequestId() + " not found");
    }

    @Test
    void leaderViewLedgerQuotaAndWaitersVisible() throws Exception {
        ClusterHarness.Node leader = harness.leader();
        ClusterHarness.TestConn a = harness.connect(leader);
        a.hello(1, 10);
        assertThat(status(send(a, phaser(10, PhaserOp.PHASER_OP_REGISTER, "pa", 2, null))))
                .isEqualTo(StatusCode.OK);
        assertThat(status(send(a, phaser(11, PhaserOp.PHASER_OP_AWAIT_ADVANCE,
                "pa", 0, 0L)))).isEqualTo(StatusCode.QUEUED);

        AdminRequestHandler handler = handlerFor(leader);
        ClusterHarness.TestConn adminConn = harness.connect(leader);
        adminConn.hello(2, 3);
        AdminSummaryResponse sum = admin(leader, handler, adminConn, summaryMsg())
                .getAdminSummaryResponse();
        assertThat(sum.getPhaserEntries()).isEqualTo(1);
        // 等待者合计含 phaser 挂起项（v9 合并口径延伸）。
        assertThat(sum.getTotalWaiters()).isGreaterThanOrEqualTo(1);

        AdminListKeysResponse list = admin(leader, handler, adminConn, listMsg())
                .getAdminListKeysResponse();
        var row = list.getItemsList().stream().filter(i -> i.getKey().equals("pa"))
                .findFirst().orElseThrow();
        assertThat(row.getFamily()).isEqualTo("phaser");
        assertThat(row.getPhaserRegistered()).isEqualTo(2);
        assertThat(row.getPhaserPhase()).isZero();
        assertThat(row.getWaiterCount()).isEqualTo(1); // 簿记口径

        AdminKeyDetailResponse detail = admin(leader, handler, adminConn, detailMsg("pa"))
                .getAdminKeyDetailResponse();
        assertThat(detail.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(detail.getPhaserPartiesInfoList())
                .anySatisfy(p -> assertThat(p.getParties()).isEqualTo(2));
        assertThat(detail.getPhaserWaitersInfoList()).hasSize(1);
        assertThat(detail.getPhaserWaitersInfoList().get(0).getExpectedPhase()).isZero();
        assertThat(detail.getWaitQueueLeaderOnly()).isFalse();

        // 观察零扰动：反复查询后等待仍在——到场两条即合拢并唤醒该 rid。
        admin(leader, handler, adminConn, detailMsg("pa"));
        admin(leader, handler, adminConn, detailMsg("pa"));
        assertThat(status(send(a, phaser(12, PhaserOp.PHASER_OP_ARRIVE, "pa", 0, null))))
                .isEqualTo(StatusCode.OK);
        // 第二发到场合拢（2/2）并唤醒 rid=11——唤醒先于应答派发（判例屏障）。
        Envelope notify = a.request(phaser(13, PhaserOp.PHASER_OP_ARRIVE, "pa", 0, null));
        assertThat(notify.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
        assertThat(notify.getAwaitNotify().getRequestIdRef()).isEqualTo(11);
        assertThat(a.awaitOutbound(10_000).getPhaserOpResponse().getStatus())
                .isEqualTo(StatusCode.OK);
    }

    @Test
    void followerViewLedgerReadableWaitersHonestlyZero() throws Exception {
        ClusterHarness.Node leader = harness.leader();
        ClusterHarness.TestConn a = harness.connect(leader);
        a.hello(1, 10);
        assertThat(status(send(a, phaser(10, PhaserOp.PHASER_OP_REGISTER, "pf", 3, null))))
                .isEqualTo(StatusCode.OK);
        assertThat(status(send(a, phaser(11, PhaserOp.PHASER_OP_AWAIT_ADVANCE,
                "pf", 0, 0L)))).isEqualTo(StatusCode.QUEUED);
        ClusterHarness.Node follower = harness.nodes().stream()
                .filter(n -> n != harness.leader()).findFirst().orElseThrow();
        AdminRequestHandler fHandler = handlerFor(follower);
        ClusterHarness.TestConn adminConn = harness.connect(follower);
        adminConn.hello(2, 3);
        AdminKeyDetailResponse detail = admin(follower, fHandler, adminConn, detailMsg("pf"))
                .getAdminKeyDetailResponse();
        // 复制态命中：三计数与配额照常（与 topic 键的 Follower 未命中分轨）。
        assertThat(detail.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(detail.getPhaserRegistered()).isEqualTo(3);
        assertThat(detail.getPhaserPartiesInfoList()).hasSize(1);
        // 等待簿记 Leader 本地态：如实零 + 同源标注，MUST NOT 伪零壳掩盖。
        assertThat(detail.getPhaserWaitersInfoList()).isEmpty();
        assertThat(detail.getWaitQueueLeaderOnly()).isTrue();
    }

    @Test
    void failoverTransitionLedgerCompleteWaitersPendingRehang() throws Exception {
        ClusterHarness.Node oldLeader = harness.leader();
        ClusterHarness.TestConn a = harness.connect(oldLeader);
        a.hello(1, 10);
        assertThat(status(send(a, phaser(10, PhaserOp.PHASER_OP_REGISTER,
                "pt", 2, null)))).isEqualTo(StatusCode.OK);
        assertThat(status(send(a, phaser(11, PhaserOp.PHASER_OP_AWAIT_ADVANCE,
                "pt", 0, 0L)))).isEqualTo(StatusCode.QUEUED);
        harness.stopNode(oldLeader.id);
        harness.awaitTrue(() -> harness.leader() != null && harness.leader() != oldLeader,
                20_000, "重选主");
        ClusterHarness.Node newLeader = harness.leader();
        AdminRequestHandler handler = handlerFor(newLeader);
        ClusterHarness.TestConn adminConn = harness.connect(newLeader);
        adminConn.hello(2, 3);
        // 双速呈现：账簿即时完整（复制态随 apply 收敛，无过渡窗）……
        AdminKeyDetailResponse mid = admin(newLeader, handler, adminConn, detailMsg("pt"))
                .getAdminKeyDetailResponse();
        assertThat(mid.getPhaserRegistered()).isEqualTo(2);
        // ……等待明细为空或仅含已重挂（新 term 簿记无旧残留）。
        assertThat(mid.getPhaserWaitersInfoList()).isEmpty();
        assertThat(mid.getWaitQueueLeaderOnly()).isFalse();
        // 重挂后恢复可见（纯谓词：未越相位续挂）。
        ClusterHarness.TestConn rehang = harness.connect(newLeader);
        rehang.hello(3, 10);
        assertThat(status(rehang.request(phaser(30, PhaserOp.PHASER_OP_AWAIT_ADVANCE,
                "pt", 0, 0L)))).isEqualTo(StatusCode.QUEUED);
        AdminKeyDetailResponse after = admin(newLeader, handler, adminConn, detailMsg("pt"))
                .getAdminKeyDetailResponse();
        assertThat(after.getPhaserWaitersInfoList()).hasSize(1);
        assertThat(after.getPhaserWaitersInfoList().get(0).getRequestId()).isEqualTo(30);
    }
}
