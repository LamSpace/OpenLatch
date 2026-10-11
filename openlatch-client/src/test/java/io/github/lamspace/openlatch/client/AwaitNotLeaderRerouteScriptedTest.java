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

import io.github.lamspace.openlatch.client.internal.ScriptedServer;
import io.github.lamspace.openlatch.protocol.BarrierAwaitResponse;
import io.github.lamspace.openlatch.protocol.ClusterView;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.LatchAwaitResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.NodeInfo;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 闩/屏障 await 的「`NOT_LEADER` 应答状态零生效重道」脚本化夹具
 * （{@link ScriptedServer} 直驱，不起真集群）。
 *
 * <p>覆盖两侧：**陈旧路由但存在真主**时应重道放行；**无真主**时应以有界
 * 预算显式失败，且不假绿、不悬挂至等待总预算。
 */
@Timeout(value = 40, unit = TimeUnit.SECONDS)
class AwaitNotLeaderRerouteScriptedTest {

    /** HELLO 应答（集群形态：提示非 0，含选举空窗的 -1）。 */
    private static Envelope hello(Envelope req, long sessionId, long leaderHint, String leaderAddr) {
        return req.toBuilder().setHelloResponse(HelloResponse.newBuilder()
                .setStatus(StatusCode.OK).setSessionId(sessionId)
                .setServerProtocolVersion(2).setDefaultLeaseMs(30_000)
                .setLeaderHint(leaderHint).setLeaderAddress(leaderAddr))
                .build();
    }

    /** 集群视图应答（单成员表）。 */
    private static Envelope view(Envelope req, long leaderNodeId, String leaderAddr) {
        return req.toBuilder().setClusterView(ClusterView.newBuilder()
                .setStatus(StatusCode.OK)
                .addNodes(NodeInfo.newBuilder().setNodeId(1)
                        .setAddress("127.0.0.1:1").setIsLeader(false))
                .addNodes(NodeInfo.newBuilder().setNodeId(leaderNodeId)
                        .setAddress(leaderAddr).setIsLeader(true))
                .build()).build();
    }

    /** 无主的集群视图（选举空窗：成员表里没有 is_leader 条目）。 */
    private static Envelope viewWithoutLeader(Envelope req) {
        return req.toBuilder().setClusterView(ClusterView.newBuilder()
                .setStatus(StatusCode.OK)
                .addNodes(NodeInfo.newBuilder().setNodeId(1)
                        .setAddress("127.0.0.1:1").setIsLeader(false))
                .build()).build();
    }

    /** 闩 await 应答（同型码形）。 */
    private static Envelope latchAwait(Envelope req, StatusCode status) {
        return req.toBuilder().setType(MessageType.LATCH_AWAIT)
                .setLatchAwaitResponse(LatchAwaitResponse.newBuilder().setStatus(status))
                .build();
    }

    /** 屏障 await 应答（同型码形）。 */
    private static Envelope barrierAwait(Envelope req, StatusCode status) {
        return req.toBuilder().setType(MessageType.BARRIER_AWAIT)
                .setBarrierAwaitResponse(BarrierAwaitResponse.newBuilder()
                        .setStatus(status).setGeneration(1).setParties(1).setExecutor(false))
                .build();
    }

    @Test
    void latchAwaitReroutesWhenHomeIsStaleButLeaderExists() throws Exception {
        // home 停在非权威节点（对 await 回 NOT_LEADER），真主在另一节点；
        // 驻留期核对会把车道改过去——零生效重道应让这次 await 放行而非失败。
        ScriptedServer leader = new ScriptedServer(req -> switch (req.getType()) {
            case HELLO -> hello(req, 2000, -1, "");
            case LATCH_AWAIT -> latchAwait(req, StatusCode.OK);
            default -> null;
        });
        ScriptedServer home = new ScriptedServer(req -> switch (req.getType()) {
            case HELLO -> hello(req, 1000, -1, "");
            case LATCH_AWAIT -> latchAwait(req, StatusCode.NOT_LEADER);
            case CLUSTER_VIEW -> view(req, 2, leader.address());
            default -> null;
        });
        try (home; leader) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(home.address())
                    .requestTimeout(Duration.ofSeconds(2))
                    .defaultWaitTimeout(Duration.ofSeconds(10))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OCountDownLatch latch = client.newCountDownLatch("rl", 2);
                assertThat(latch.await(10, TimeUnit.SECONDS))
                        .as("陈旧路由但存在真主：await 应在重道预算内归零放行").isTrue();
            }
            assertThat(leader.countType(MessageType.LATCH_AWAIT))
                    .as("重道后的 await 应落在当值 Leader").isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void barrierAwaitReroutesWhenHomeIsStaleButLeaderExists() throws Exception {
        ScriptedServer leader = new ScriptedServer(req -> switch (req.getType()) {
            case HELLO -> hello(req, 2000, -1, "");
            case BARRIER_AWAIT -> barrierAwait(req, StatusCode.OK);
            default -> null;
        });
        ScriptedServer home = new ScriptedServer(req -> switch (req.getType()) {
            case HELLO -> hello(req, 1000, -1, "");
            case BARRIER_AWAIT -> barrierAwait(req, StatusCode.NOT_LEADER);
            case CLUSTER_VIEW -> view(req, 2, leader.address());
            default -> null;
        });
        try (home; leader) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(home.address())
                    .requestTimeout(Duration.ofSeconds(2))
                    .defaultWaitTimeout(Duration.ofSeconds(10))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OBarrier barrier = client.newBarrier("rb", 1);
                assertThat(barrier.await(10, TimeUnit.SECONDS))
                        .as("陈旧路由但存在真主：到场应在重道预算内合拢").isTrue();
            }
            assertThat(leader.countType(MessageType.BARRIER_AWAIT))
                    .as("重道后的到场应落在当值 Leader").isGreaterThanOrEqualTo(1);
        }
    }

    @Test
    void awaitFailsBoundedlyWithoutLeaderAndNeverFalseGreens() throws Exception {
        // 集群形态但始终无当值 Leader：路由不会变化，重道无从谈起。
        // 必须显式失败、不得假绿、且耗时受重道预算上界约束（远小于等待总预算）。
        ScriptedServer home = new ScriptedServer(req -> switch (req.getType()) {
            case HELLO -> hello(req, 1000, -1, "");
            case LATCH_AWAIT -> latchAwait(req, StatusCode.NOT_LEADER);
            case CLUSTER_VIEW -> viewWithoutLeader(req);
            default -> null;
        });
        try (home) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(home.address())
                    .requestTimeout(Duration.ofSeconds(2))
                    .defaultWaitTimeout(Duration.ofSeconds(30))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OCountDownLatch latch = client.newCountDownLatch("rl", 2);
                long t0 = System.currentTimeMillis();
                assertThatThrownBy(() -> latch.await(10, TimeUnit.SECONDS))
                        .as("无真主时必须以 NOT_LEADER 显式失败，MUST NOT 假绿返回 true")
                        .isInstanceOf(OpenLatchException.class)
                        .extracting(e -> ((OpenLatchException) e).status())
                        .isEqualTo(StatusCode.NOT_LEADER);
                long elapsed = System.currentTimeMillis() - t0;
                assertThat(elapsed)
                        .as("失败前应确有重道等待（有界），不是立即放弃")
                        .isGreaterThanOrEqualTo(1_000L);
                assertThat(elapsed)
                        .as("失败上界受重道预算约束，不得悬挂至等待总预算")
                        .isLessThan(5_000L);
            }
        }
    }
}
