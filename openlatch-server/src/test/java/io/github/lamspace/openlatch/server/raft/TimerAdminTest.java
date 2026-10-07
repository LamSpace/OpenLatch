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
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.TimerOp;
import io.github.lamspace.openlatch.protocol.TimerOpRequest;
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
 * timer 管理观察夹具（v11）：Leader 视角 SUMMARY {@code timer_entries}、
 * LIST_KEYS {@code family=timer} 行三元组与等待数、KEY_DETAIL 三元组+等待
 * 明细；<b>呈现面时钟无关</b>——两节点各取一次三元组逐字节等（不折算 marked
 * 的呈现纪律，与 Leader 应答线 marked 折算正交）；观察零扰动（查询不推进
 * 代次/装载态/到期判定/清扫）；Follower 视角三元组照常（复制态命中）、等待
 * 明细如实零并同口径标注（双速呈现沿 phaser 型）；换主窗口账簿即时完整而
 * 等待明细仅含重挂（过渡如实）。判例 {@code PhaserAdminTest}（单 harness
 * 双视角，控端口周转）。
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class TimerAdminTest {

    /** 管理令牌。 */
    private static final String TOKEN = "timer-admin-token";

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

    private static Envelope timer(long rid, TimerOp op, String key, Long delay, long awaitRid) {
        TimerOpRequest.Builder rb = TimerOpRequest.newBuilder()
                .setKey(key).setOp(op).setAwaitRequestId(awaitRid);
        if (delay != null) {
            rb.setDelayMs(delay);
        }
        return Envelope.newBuilder().setProtocolVersion(11).setType(MessageType.TIMER_OP)
                .setRequestId(rid).setTimerOpRequest(rb).build();
    }

    private static StatusCode status(Envelope resp) {
        return resp.getTimerOpResponse().getStatus();
    }

    /** 业务请求：按同 rid 同型过滤先到的 AWAIT_NOTIFY（判例 PhaserAdminTest）。 */
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
    void leaderViewLedgerAndWaitersVisibleAndPresentationClockFree() throws Exception {
        ClusterHarness.Node leader = harness.leader();
        ClusterHarness.TestConn a = harness.connect(leader);
        a.hello(1, 11);
        assertThat(status(send(a, timer(10, TimerOp.TIMER_OP_SCHEDULE, "ta", 60_000L, 0))))
                .isEqualTo(StatusCode.OK);
        assertThat(status(send(a, timer(11, TimerOp.TIMER_OP_AWAIT, "ta", null, 0))))
                .isEqualTo(StatusCode.QUEUED);

        AdminRequestHandler handler = handlerFor(leader);
        ClusterHarness.TestConn adminConn = harness.connect(leader);
        adminConn.hello(2, 3);
        AdminSummaryResponse sum = admin(leader, handler, adminConn, summaryMsg())
                .getAdminSummaryResponse();
        assertThat(sum.getTimerEntries()).isEqualTo(1);
        // 等待者合计含 timer 挂起项（v9/v10 合并口径再延伸）。
        assertThat(sum.getTotalWaiters()).isGreaterThanOrEqualTo(1);

        AdminListKeysResponse list = admin(leader, handler, adminConn, listMsg())
                .getAdminListKeysResponse();
        var row = list.getItemsList().stream().filter(i -> i.getKey().equals("ta"))
                .findFirst().orElseThrow();
        assertThat(row.getFamily()).isEqualTo("timer");
        assertThat(row.getTimerGeneration()).isEqualTo(1);
        assertThat(row.getTimerArmed()).isTrue();
        assertThat(row.getTimerFireAtMs()).isGreaterThan(0);
        assertThat(row.getWaiterCount()).isEqualTo(1); // 簿记口径

        AdminKeyDetailResponse detail = admin(leader, handler, adminConn, detailMsg("ta"))
                .getAdminKeyDetailResponse();
        assertThat(detail.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(detail.getTimerWaitersInfoList()).hasSize(1);
        assertThat(detail.getTimerWaitersInfoList().get(0).getRequestId()).isEqualTo(11);
        assertThat(detail.getWaitQueueLeaderOnly()).isFalse();
        // 呈现面时钟无关：同 key 两取逐字节等（无 marked 折算字段可漂移）。
        AdminKeyDetailResponse detail2 = admin(leader, handler, adminConn, detailMsg("ta"))
                .getAdminKeyDetailResponse();
        assertThat(detail2.getTimerFireAtMs()).isEqualTo(detail.getTimerFireAtMs());
        assertThat(detail2.getTimerGeneration()).isEqualTo(detail.getTimerGeneration());

        // 观察零扰动：反复查询后等待仍在——DISARM 即唤醒并以 DENIED 收束。
        admin(leader, handler, adminConn, detailMsg("ta"));
        admin(leader, handler, adminConn, detailMsg("ta"));
        assertThat(status(send(a, timer(12, TimerOp.TIMER_OP_DISARM, "ta", null, 0))))
                .isEqualTo(StatusCode.OK);
        // 撤销唤醒与应答到达序不锁定（notify 可能先于/后于 DISARM 应答）：
        // 以同 rid 重发的终态为断言面——簿记已被 apply 排空，重发即刻 DENIED。
        Envelope denied = send(a, timer(11, TimerOp.TIMER_OP_AWAIT, "ta", null, 0));
        assertThat(status(denied)).isEqualTo(StatusCode.DENIED);
        // 三元组如实：代终结 armed=false、fire_at 保持历史值、代次不推。
        AdminKeyDetailResponse after = admin(leader, handler, adminConn, detailMsg("ta"))
                .getAdminKeyDetailResponse();
        assertThat(after.getTimerArmed()).isFalse();
        assertThat(after.getTimerGeneration()).isEqualTo(1);
    }

    @Test
    void followerViewLedgerReadableWaitersHonestlyZero() throws Exception {
        ClusterHarness.Node leader = harness.leader();
        ClusterHarness.TestConn a = harness.connect(leader);
        a.hello(1, 11);
        assertThat(status(send(a, timer(10, TimerOp.TIMER_OP_SCHEDULE,
                "tf", 60_000L, 0)))).isEqualTo(StatusCode.OK);
        assertThat(status(send(a, timer(11, TimerOp.TIMER_OP_AWAIT, "tf", null, 0))))
                .isEqualTo(StatusCode.QUEUED);
        ClusterHarness.Node follower = harness.nodes().stream()
                .filter(n -> n != harness.leader()).findFirst().orElseThrow();
        AdminRequestHandler fHandler = handlerFor(follower);
        ClusterHarness.TestConn adminConn = harness.connect(follower);
        adminConn.hello(2, 3);
        AdminKeyDetailResponse detail = admin(follower, fHandler, adminConn, detailMsg("tf"))
                .getAdminKeyDetailResponse();
        // 复制态命中：三元组照常呈现（与 topic 键的 Follower 未命中分轨），
        // 且读数与 Leader 等值（原始值不随节点时钟漂移）。
        assertThat(detail.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(detail.getTimerGeneration()).isEqualTo(1);
        assertThat(detail.getTimerArmed()).isTrue();
        ClusterHarness.TestConn leaderAdminConn = harness.connect(leader);
        leaderAdminConn.hello(4, 3);
        AdminKeyDetailResponse leaderView = admin(leader, handlerFor(leader),
                leaderAdminConn, detailMsg("tf")).getAdminKeyDetailResponse();
        assertThat(detail.getTimerFireAtMs()).isEqualTo(leaderView.getTimerFireAtMs());
        // 等待簿记 Leader 本地态：如实零 + 同源标注，MUST NOT 伪零壳掩盖。
        assertThat(detail.getTimerWaitersInfoList()).isEmpty();
        assertThat(detail.getWaitQueueLeaderOnly()).isTrue();
    }

    @Test
    void failoverTransitionLedgerCompleteWaitersPendingRehang() throws Exception {
        ClusterHarness.Node oldLeader = harness.leader();
        ClusterHarness.TestConn a = harness.connect(oldLeader);
        a.hello(1, 11);
        assertThat(status(send(a, timer(10, TimerOp.TIMER_OP_SCHEDULE,
                "tt", 60_000L, 0)))).isEqualTo(StatusCode.OK);
        assertThat(status(send(a, timer(11, TimerOp.TIMER_OP_AWAIT, "tt", null, 0))))
                .isEqualTo(StatusCode.QUEUED);
        harness.stopNode(oldLeader.id);
        harness.awaitTrue(() -> harness.leader() != null && harness.leader() != oldLeader,
                20_000, "重选主");
        ClusterHarness.Node newLeader = harness.leader();
        AdminRequestHandler handler = handlerFor(newLeader);
        ClusterHarness.TestConn adminConn = harness.connect(newLeader);
        adminConn.hello(2, 3);
        // 双速呈现：账簿即时完整（复制态随 apply 收敛，无过渡窗）……
        AdminKeyDetailResponse mid = admin(newLeader, handler, adminConn, detailMsg("tt"))
                .getAdminKeyDetailResponse();
        assertThat(mid.getTimerGeneration()).isEqualTo(1);
        assertThat(mid.getTimerArmed()).isTrue();
        // ……等待明细为空或仅含已重挂（新 term 集合无旧残留）。
        assertThat(mid.getTimerWaitersInfoList()).isEmpty();
        assertThat(mid.getWaitQueueLeaderOnly()).isFalse();
        // 重挂后恢复可见（谓词未到期续挂）。
        ClusterHarness.TestConn rehang = harness.connect(newLeader);
        rehang.hello(3, 11);
        assertThat(status(rehang.request(timer(30, TimerOp.TIMER_OP_AWAIT,
                "tt", null, 0)))).isEqualTo(StatusCode.QUEUED);
        AdminKeyDetailResponse after = admin(newLeader, handler, adminConn, detailMsg("tt"))
                .getAdminKeyDetailResponse();
        assertThat(after.getTimerWaitersInfoList()).hasSize(1);
        assertThat(after.getTimerWaitersInfoList().get(0).getRequestId()).isEqualTo(30);
    }
}
