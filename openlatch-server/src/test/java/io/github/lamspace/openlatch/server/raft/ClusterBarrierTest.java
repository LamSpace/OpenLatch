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

import io.github.lamspace.openlatch.protocol.BarrierActionDoneRequest;
import io.github.lamspace.openlatch.protocol.BarrierAwaitRequest;
import io.github.lamspace.openlatch.protocol.BarrierLeaveRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LatchAwaitRequest;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 循环屏障集群 E2E（判例 {@code ClusterSemaphoreLatchTest}）：三命令皆经复制
 * 日志合拢、执行者两阶段放行的推送时序、离场即破障（到场者会话死亡经
 * {@code SESSION_CLOSE} 传播）、世代回卷复用与 failover 后状态存续、
 * v5 门控在集群车道生效、副本一致摘要收口。
 */
class ClusterBarrierTest {

    /** 屏障到场信封（v5）。 */
    private static Envelope await(long rid, String key, long parties, boolean action) {
        return Envelope.newBuilder()
                .setProtocolVersion(5)
                .setType(MessageType.BARRIER_AWAIT)
                .setRequestId(rid)
                .setBarrierAwaitRequest(BarrierAwaitRequest.newBuilder()
                        .setKey(key).setParties(parties).setCarriesAction(action))
                .build();
    }

    /** 屏障动作了结信封（v5）。 */
    private static Envelope actionDone(long rid, String key, long generation) {
        return Envelope.newBuilder()
                .setProtocolVersion(5)
                .setType(MessageType.BARRIER_ACTION_DONE)
                .setRequestId(rid)
                .setBarrierActionDoneRequest(BarrierActionDoneRequest.newBuilder()
                        .setKey(key).setGeneration(generation))
                .build();
    }

    /** 屏障离场信封（v5；awaitRid=0 为纯破障）。 */
    private static Envelope leave(long rid, String key, long awaitRid) {
        return Envelope.newBuilder()
                .setProtocolVersion(5)
                .setType(MessageType.BARRIER_LEAVE)
                .setRequestId(rid)
                .setBarrierLeaveRequest(BarrierLeaveRequest.newBuilder()
                        .setKey(key).setAwaitRequestId(awaitRid))
                .build();
    }

    /** Latch await 信封（v3 门控回归用）。 */
    private static Envelope latchAwait(long rid, String key, long total) {
        return Envelope.newBuilder()
                .setProtocolVersion(3)
                .setType(MessageType.LATCH_AWAIT)
                .setRequestId(rid)
                .setLatchAwaitRequest(LatchAwaitRequest.newBuilder().setKey(key).setTotal(total))
                .build();
    }

    /**
     * 请求并消费应答前的挂起推送（同连接既是等待者又发新请求时，Leader
     * 可先投递 {@code AWAIT_NOTIFY} 再回应答；harness 出站队列按写入序读取）。
     *
     * @param conn 连接
     * @param msg  请求信封
     * @return 该请求的应答信封（跳过所有推送帧）
     */
    private static Envelope requestSkippingNotifies(ClusterHarness.TestConn conn, Envelope msg)
            throws Exception {
        Envelope first = conn.request(msg);
        while (first.getType() == MessageType.AWAIT_NOTIFY) {
            first = conn.awaitOutbound(10_000);
        }
        return first;
    }

    /**
     * 静默窗口断言：在给定毫秒内不得收到任何出站推送（推送时序钉死用）。
     */
    private static void quietFor(ClusterHarness.TestConn conn, long ms) throws InterruptedException {
        long deadline = System.currentTimeMillis() + ms;
        while (System.currentTimeMillis() < deadline) {
            assertThat(conn.pollOutbound()).isNull();
            Thread.sleep(50);
        }
    }

    @Test
    void replicatedTripsPushWrapAndActionTwoPhase() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn a = h.connect(leader);
            a.hello(1, 5);
            ClusterHarness.TestConn b = h.connect(leader);
            b.hello(2, 5);
            ClusterHarness.TestConn c = h.connect(leader);
            c.hello(3, 5);

            // 世代 1（无动作）：两方挂起、第三方当回合拢直答，前两位收推送后重发了结。
            Envelope ga = a.request(await(10, "ph", 3, false));
            assertThat(ga.getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            assertThat(ga.getBarrierAwaitResponse().getGeneration()).isEqualTo(1);
            assertThat(ga.getBarrierAwaitResponse().getQueuePosition()).isEqualTo(1);
            assertThat(b.request(await(11, "ph", 0, false)).getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.QUEUED);
            quietFor(a, 500);
            Envelope gc = c.request(await(12, "ph", 0, false));
            assertThat(gc.getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(gc.getBarrierAwaitResponse().getGeneration()).isEqualTo(1);
            Envelope pa = a.awaitOutbound(10_000);
            Envelope pb = b.awaitOutbound(10_000);
            assertThat(pa.getAwaitNotify().getRequestIdRef()).isEqualTo(10);
            assertThat(pb.getAwaitNotify().getRequestIdRef()).isEqualTo(11);
            Envelope ra = a.request(await(10, "ph", 0, false));
            assertThat(ra.getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(ra.getBarrierAwaitResponse().getGeneration()).isEqualTo(1); // 旧世代了结

            // 世代 2（动作两阶段）：执行者=第 parties 次到场者；回报前其余方不放行。
            assertThat(a.request(await(20, "ph", 0, false)).getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.QUEUED);
            assertThat(b.request(await(21, "ph", 0, false)).getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.QUEUED);
            Envelope exec = c.request(await(22, "ph", 0, true));
            assertThat(exec.getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            assertThat(exec.getBarrierAwaitResponse().getExecutor()).isTrue();
            assertThat(exec.getBarrierAwaitResponse().getGeneration()).isEqualTo(2);
            quietFor(a, 800); // 动作未回报：A/B 均不得收放行推送
            assertThat(c.request(actionDone(23, "ph", 2)).getBarrierActionDoneResponse()
                    .getStatus()).isEqualTo(StatusCode.OK);
            assertThat(a.awaitOutbound(10_000).getAwaitNotify().getRequestIdRef()).isEqualTo(20);
            assertThat(b.awaitOutbound(10_000).getAwaitNotify().getRequestIdRef()).isEqualTo(21);
            assertThat(a.request(await(20, "ph", 0, false)).getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            // 已合拢世代的非执行者重复回报被拒（执行者挂账归属断言）。
            assertThat(b.request(actionDone(24, "ph", 2)).getBarrierActionDoneResponse()
                    .getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);

            // 相位复用后重组：世代 3 三方到场合拢（顺序不同，纯回卷复用）。
            assertThat(c.request(await(30, "ph", 0, false)).getBarrierAwaitResponse()
                    .getGeneration()).isEqualTo(3);
            assertThat(b.request(await(31, "ph", 0, false)).getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.QUEUED);
            Envelope trip3 = a.request(await(32, "ph", 0, false));
            assertThat(trip3.getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(trip3.getBarrierAwaitResponse().getGeneration()).isEqualTo(3);
            assertThat(c.awaitOutbound(10_000).getAwaitNotify().getRequestIdRef()).isEqualTo(30);

            // 纯破障主张（breakBarrier 通道）破当前世代：在队者收破障了结。
            assertThat(a.request(await(40, "ph", 0, false)).getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.QUEUED);
            assertThat(b.request(leave(41, "ph", 0)).getBarrierLeaveResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            assertThat(a.awaitOutbound(10_000).getAwaitNotify().getRequestIdRef()).isEqualTo(40);
            Envelope broken = a.request(await(40, "ph", 0, false));
            assertThat(broken.getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.BARRIER_BROKEN);
            assertThat(broken.getBarrierAwaitResponse().getGeneration()).isEqualTo(4);

            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "Barrier 序列后副本一致");
        }
    }

    @Test
    void deathOfArrivedParticipantBreaksGenerationForPeers() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn a = h.connect(leader);
            a.hello(1, 5);
            ClusterHarness.TestConn b = h.connect(leader);
            b.hello(2, 5);
            ClusterHarness.TestConn victim = h.connect(leader);
            victim.hello(3, 5);
            ClusterHarness.TestConn bystander = h.connect(leader);
            bystander.hello(4, 5);

            // parties=4：三方到场，第四方" bystander" 从未到场。
            assertThat(a.request(await(10, "db", 4, false)).getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.QUEUED);
            assertThat(b.request(await(11, "db", 0, false)).getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.QUEUED);
            assertThat(victim.request(await(12, "db", 0, false))
                    .getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.QUEUED);

            // 未到场者死亡：零扰动（bystander 关闭后其余仍无通知）。
            bystander.disconnect();
            quietFor(a, 800);

            // 到场者会话死亡：即时破障——在队同伴收破障广播，重发得在带裁决。
            victim.disconnect();
            Envelope pa = a.awaitOutbound(10_000);
            Envelope pb = b.awaitOutbound(10_000);
            assertThat(pa.getAwaitNotify().getRequestIdRef()).isEqualTo(10);
            assertThat(pb.getAwaitNotify().getRequestIdRef()).isEqualTo(11);
            assertThat(a.request(await(10, "db", 0, false)).getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.BARRIER_BROKEN);
            assertThat(b.request(await(11, "db", 0, false)).getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.BARRIER_BROKEN);

            // 世代局部自愈：破障后新到场进新世代正常挂起（无粘滞）。
            Envelope fresh = a.request(await(20, "db", 0, false));
            assertThat(fresh.getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            assertThat(fresh.getBarrierAwaitResponse().getGeneration()).isEqualTo(2);

            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "破障序列后副本一致");
        }
    }

    @Test
    void barrierLedgerSurvivesLeaderFailover() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3, 800L)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn a = h.connect(leader);
            a.hello(1, 5);
            // 世代 1 合拢（两方，parties=2 纯复制账簿），世代 2 起算。
            assertThat(a.request(await(10, "fo", 2, false)).getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.QUEUED);
            Envelope trip = a.request(await(11, "fo", 0, false));
            assertThat(trip.getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.OK);

            h.stopNode(leader.id);
            h.awaitTrue(() -> h.leader() != null && h.leader() != leader, 20_000, "重选主");
            ClusterHarness.TestConn onNew = h.connect(h.leader());
            onNew.hello(20, 5);

            // 账簿与世代号为复制状态：新 Leader 上 parties 存续、世代不回退。
            Envelope gen2 = requestSkippingNotifies(onNew, await(21, "fo", 0, false));
            assertThat(gen2.getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            assertThat(gen2.getBarrierAwaitResponse().getGeneration()).isEqualTo(2);
            assertThat(gen2.getBarrierAwaitResponse().getParties()).isEqualTo(2);
            // 同连接的第二发：合拢时先派发上一等待项的 AWAIT_NOTIFY，再回本应答。
            Envelope trip2 = requestSkippingNotifies(onNew, await(22, "fo", 0, false));
            assertThat(trip2.getBarrierAwaitResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(trip2.getBarrierAwaitResponse().getGeneration()).isEqualTo(2);
            // 无主张创建被拒（定型断言跨 failover 存续）。
            assertThat(onNew.request(await(23, "fresh", 0, false))
                    .getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.INVALID_REQUEST);

            h.awaitTrue(h::aliveAgreeWithLeader, 10_000, "failover 后副本一致");
        }
    }

    @Test
    void barrierMessagesGatedForV4SessionWithoutDisconnect() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn v4 = h.connect(leader);
            v4.hello(1, 4);
            assertThat(v4.request(await(10, "gt", 2, false)).getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.INVALID_REQUEST);
            assertThat(v4.request(leave(11, "gt", 0)).getBarrierLeaveResponse().getStatus())
                    .isEqualTo(StatusCode.INVALID_REQUEST);
            assertThat(v4.request(actionDone(12, "gt", 1)).getBarrierActionDoneResponse()
                    .getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
            // v4 既有能力回归：Latch 通道照常（定型 + 挂起）。
            assertThat(v4.request(latchAwait(13, "lat", 2)).getLatchAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.QUEUED);
            // Follower 车道角色门前置（与 LATCH_AWAIT 同口径）：v4 会话先得
            // NOT_LEADER 提示——门控与角色门均不断连，后续请求照常裁决。
            ClusterHarness.Node follower = null;
            for (ClusterHarness.Node n : h.nodes()) {
                if (n != h.leader()) {
                    follower = n;
                    break;
                }
            }
            assertThat(follower).isNotNull();
            ClusterHarness.TestConn v4f = h.connect(follower);
            v4f.hello(1, 4);
            assertThat(v4f.request(await(14, "gt", 2, false)).getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.NOT_LEADER);
            // 不断连：同连接再来一发，照常得到 NOT_LEADER 裁决。
            assertThat(v4f.request(await(15, "gt", 2, false)).getBarrierAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.NOT_LEADER);
        }
    }
}
