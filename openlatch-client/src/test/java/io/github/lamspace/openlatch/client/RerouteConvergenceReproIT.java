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
import io.github.lamspace.openlatch.protocol.QueueOpRequest;
import io.github.lamspace.openlatch.protocol.QueueOpResponse;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 非 ACQUIRE 车道路由收敛缺口的**确定性红先夹具**（启动窗提示滞后形态）。
 *
 * <p><b>红是预期</b>：本类不是回归网，而是该缺口的判据——缺口未修复时用例
 * **必须失败**（在册记录见 {@code WATCHLIST.md} 的 W11 行）。默认构建经
 * {@code @Tag("w11-repro")} 排除，取证命令：
 * <pre>{@code
 * mvn -s /home/lam/repo/settings.xml -Pw11-repro -pl openlatch-client verify
 * }</pre>
 * 缺口修复后本类应**由红转绿**，届时该标签与 profile 一并退役。
 *
 * <p><b>覆盖的根因窗</b>：种子节点的 Leader 视图尚未随当选事件更新——握手
 * 应答给出无效提示（{@code leaderHint=-1}），只有后续查询集群视图才能得知
 * 当值 Leader。而改道决策只挂在握手回调上，home 会话连通期间不再重评估
 * Leader 身份；于是非 ACQUIRE 车道（队列/原子/屏障/闩/…）既无 {@code NOT_LEADER}
 * 提示可依（无提示字段时该车道的重发环只能原地退避），home 又不断连，请求被
 * 消化至等待预算耗尽——**显式超时**。当值 Leader 侧接受应答是修复后唯一
 * 合法的成功来源。
 *
 * <p><b>等待面（闩/屏障）</b>：同一缺口在等待原语上另有一副形态——闩与屏障的
 * {@code await} 对 {@code NOT_LEADER} <b>应答状态</b>是终态抛出（状态分派只有
 * {@code OK}/{@code QUEUED} 可续），于是"home 活着但已非权威"窗内的等待仍立即
 * 失败，尽管该答复恰恰证明这次到场/登记**从未被受理**。本类另锁其二侧：
 * <b>真主可达</b>（视图给出 {@code is_leader}）时须经零生效重道——摘本机登记 →
 * 触发共享路由刷新 → 短预算内轮询路由 → 在新会话重发——并在当值 Leader 上完成
 * 等待；<b>无真主可达</b>（选举空窗，永久 {@code NOT_LEADER}）时失败须<b>有界</b>
 * 且携带 {@code NOT_LEADER}，不得悬挂至等待总预算。后者修复前后皆绿，专防
 * "过修复"把显式失败变成悬挂。
 *
 * <p><b>守卫断言</b>：把已非权威的 home 配成**缺码形应答**（应答 oneof 成员
 * 缺位）——这正是"缺码形被判读为 protobuf 默认实例、默认实例的 OK 被当成
 * 成功"这一静默错值形态的入口。因此本夹具的成功路径另有一条并列断言：
 * 成功必须落在当值 Leader 上，不得是缺码形应答被读作成功。断言只在成功
 * 路径上求值：**显式超时与服务端会话失效不判红**（它们是合法终态），
 * 只有静默错值才是缺陷。
 */
@Tag("w11-repro")
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class RerouteConvergenceReproIT {

    /** 握手应答：{@code leaderHint=-1} 即"提示滞后"形态（当选事件未达该节点）。 */
    private static Envelope helloWithoutHint(Envelope req, long sessionId) {
        return req.toBuilder().setHelloResponse(HelloResponse.newBuilder()
                .setStatus(StatusCode.OK).setSessionId(sessionId)
                .setServerProtocolVersion(2).setDefaultLeaseMs(30_000)
                .setLeaderHint(-1))
                .build();
    }

    /** 集群视图应答：列出两个成员，其中之一为当值 Leader（地址即其真实接入地址）。 */
    private static Envelope clusterView(Envelope req, String leaderAddr) {
        return req.toBuilder().setClusterView(ClusterView.newBuilder()
                .setStatus(StatusCode.OK)
                .addNodes(NodeInfo.newBuilder().setNodeId(1)
                        .setAddress("127.0.0.1:1").setIsLeader(false))
                .addNodes(NodeInfo.newBuilder().setNodeId(2)
                        .setAddress(leaderAddr).setIsLeader(true))
                .build()).build();
    }

    /** 当值 Leader 的队列受理应答（op 回显，形态同队列通道服务端）。 */
    private static Envelope queueAccepted(Envelope req) {
        QueueOpRequest q = req.getQueueOpRequest();
        return req.toBuilder().setType(MessageType.QUEUE_OP)
                .setQueueOpResponse(QueueOpResponse.newBuilder()
                        .setStatus(StatusCode.OK).setOp(q.getOp()))
                .build();
    }

    /**
     * 启动窗提示滞后下，非 ACQUIRE 请求应在等待预算内**收敛**到当值 Leader。
     *
     * <p>未修复时 red：home 不断连、车道永不重建，队列请求耗尽等待预算显式
     * 超时。
     */
    @Test
    void startupHintLagConvergesNonAcquireRequest() throws Exception {
        try (ScriptedServer authoritative = new ScriptedServer(req -> switch (req.getType()) {
                    case HELLO -> helloWithoutHint(req, 2000);
                    case QUEUE_OP -> queueAccepted(req);
                    default -> null;
                });
             ScriptedServer laggingHome = new ScriptedServer(req -> switch (req.getType()) {
                    case HELLO -> helloWithoutHint(req, 1000);
                    case CLUSTER_VIEW -> clusterView(req, authoritative.address());
                    // 已非权威的 home：应答 oneof 成员缺位（缺码形），
                    // 盲读默认实例会把它当成 OK 成功——守卫断言的猎物。
                    case QUEUE_OP -> req.toBuilder().build();
                    default -> null;
                })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(laggingHome.address())
                    .requestTimeout(Duration.ofSeconds(2))
                    .defaultWaitTimeout(Duration.ofSeconds(10))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OBlockingQueue q = client.newBlockingQueue("repro-hint-lag", 4);
                q.put("x"); // 未收敛即在此耗尽等待预算抛超时（红）
            }
            assertThat(authoritative.countType(MessageType.QUEUE_OP))
                    .as("成功必须来自当值 Leader，不得是缺码形应答被读作成功")
                    .isGreaterThanOrEqualTo(1);
        }
    }

    /**
     * 等待面（闩）：路由陈旧而真主存在时，`await` 收到 `NOT_LEADER` 应答状态
     * 应经零生效重道在短预算内于**当值 Leader** 完成等待。
     *
     * <p>未修复时 red：`NOT_LEADER` 是终态抛出，等待立即失败（该答复恰恰证明
     * 本次登记从未被受理，失败并不诚实）。
     */
    @Test
    void staleRouteWithRealLeaderReroutesLatchAwait() throws Exception {
        try (ScriptedServer leader = new ScriptedServer(req -> switch (req.getType()) {
                    case HELLO -> helloWithoutHint(req, 2000);
                    case LATCH_AWAIT -> latchReleased(req);
                    default -> null;
                });
             ScriptedServer staleHome = new ScriptedServer(req -> switch (req.getType()) {
                    case HELLO -> helloWithoutHint(req, 1000);
                    case CLUSTER_VIEW -> clusterView(req, leader.address());
                    case LATCH_AWAIT -> latchNotLeader(req);
                    default -> null;
                });
             OpenLatchClient client = connect(staleHome.address())) {
            OCountDownLatch latch = client.newCountDownLatch("repro-wait-latch", 1);
            assertThat(latch.await(5, TimeUnit.SECONDS))
                    .as("零生效重道须在短预算内于当值 Leader 完成等待")
                    .isTrue();
            assertThat(staleHome.countType(MessageType.LATCH_AWAIT))
                    .as("构造有效性：首次 await 必须真的落在陈旧 home 上，否则本夹具空转")
                    .isGreaterThanOrEqualTo(1);
            assertThat(leader.countType(MessageType.LATCH_AWAIT))
                    .as("成功必须来自当值 Leader")
                    .isGreaterThanOrEqualTo(1);
        }
    }

    /**
     * 等待面（屏障）：同口径——屏障 `await` 收到 `NOT_LEADER` 应答状态时可跨会话
     * 重道（该答复证明到场从未被计入，故不触犯"到场不跨会话重放"）。
     *
     * <p>未修复时 red：终态抛出。
     */
    @Test
    void staleRouteWithRealLeaderReroutesBarrierAwait() throws Exception {
        try (ScriptedServer leader = new ScriptedServer(req -> switch (req.getType()) {
                    case HELLO -> helloWithoutHint(req, 2200);
                    case BARRIER_AWAIT -> barrierTripped(req);
                    default -> null;
                });
             ScriptedServer staleHome = new ScriptedServer(req -> switch (req.getType()) {
                    case HELLO -> helloWithoutHint(req, 1100);
                    case CLUSTER_VIEW -> clusterView(req, leader.address());
                    case BARRIER_AWAIT -> barrierNotLeader(req);
                    default -> null;
                });
             OpenLatchClient client = connect(staleHome.address())) {
            OBarrier barrier = client.newBarrier("repro-wait-barrier", 1);
            assertThat(barrier.await(5, TimeUnit.SECONDS))
                    .as("零生效重道须在短预算内于当值 Leader 合拢")
                    .isTrue();
            assertThat(staleHome.countType(MessageType.BARRIER_AWAIT))
                    .as("构造有效性：首次到场必须真的落在陈旧 home 上")
                    .isGreaterThanOrEqualTo(1);
            assertThat(leader.countType(MessageType.BARRIER_AWAIT))
                    .as("合拢必须来自当值 Leader")
                    .isGreaterThanOrEqualTo(1);
        }
    }

    /**
     * 两侧锁定（闩）：**永久** `NOT_LEADER` 形态（选举空窗，视图无 `is_leader`
     * 条目 ⇒ 刷新无可发布的路由）下，失败须**有界**且携带 `NOT_LEADER`。
     *
     * <p>修复前后皆绿：它锁的是"过修复"——把显式失败变成悬挂至等待总预算
     * （此处总预算 10s，失败上界取远小于它的 4s）。
     */
    @Test
    void permanentNotLeaderLatchAwaitFailsBounded() throws Exception {
        try (ScriptedServer staleHome = new ScriptedServer(req -> switch (req.getType()) {
                    case HELLO -> helloWithoutHint(req, 1200);
                    case CLUSTER_VIEW -> clusterViewWithoutLeader(req);
                    case LATCH_AWAIT -> latchNotLeader(req);
                    default -> null;
                });
             OpenLatchClient client = connect(staleHome.address())) {
            OCountDownLatch latch = client.newCountDownLatch("repro-wait-latch-pinned", 1);
            long t0 = System.currentTimeMillis();
            assertThatThrownBy(() -> latch.await(10, TimeUnit.SECONDS))
                    .as("永久 NOT_LEADER 须显式抛出，MUST NOT 假绿放行")
                    .isInstanceOf(OpenLatchException.class)
                    .extracting(e -> ((OpenLatchException) e).status())
                    .isEqualTo(StatusCode.NOT_LEADER);
            assertThat(System.currentTimeMillis() - t0)
                    .as("失败有界：不得悬挂至 10s 等待总预算")
                    .isLessThan(4_000L);
        }
    }

    /**
     * 两侧锁定（屏障）：同口径——永久 `NOT_LEADER` 下屏障 `await` 亦须有界
     * 显式失败（且不得借道离场把失败粉饰成 `false`）。
     */
    @Test
    void permanentNotLeaderBarrierAwaitFailsBounded() throws Exception {
        try (ScriptedServer staleHome = new ScriptedServer(req -> switch (req.getType()) {
                    case HELLO -> helloWithoutHint(req, 1300);
                    case CLUSTER_VIEW -> clusterViewWithoutLeader(req);
                    case BARRIER_AWAIT -> barrierNotLeader(req);
                    default -> null;
                });
             OpenLatchClient client = connect(staleHome.address())) {
            OBarrier barrier = client.newBarrier("repro-wait-barrier-pinned", 1);
            long t0 = System.currentTimeMillis();
            assertThatThrownBy(() -> barrier.await(10, TimeUnit.SECONDS))
                    .as("永久 NOT_LEADER 须显式抛出，MUST NOT 假绿合拢")
                    .isInstanceOf(OpenLatchException.class)
                    .extracting(e -> ((OpenLatchException) e).status())
                    .isEqualTo(StatusCode.NOT_LEADER);
            assertThat(System.currentTimeMillis() - t0)
                    .as("失败有界：不得悬挂至 10s 等待总预算")
                    .isLessThan(4_000L);
        }
    }

    /** 陈旧 home 对闩 await 的同型 `NOT_LEADER` 应答（角色门拒绝＝零生效）。 */
    private static Envelope latchNotLeader(Envelope req) {
        return req.toBuilder().setType(MessageType.LATCH_AWAIT)
                .setLatchAwaitResponse(LatchAwaitResponse.newBuilder()
                        .setStatus(StatusCode.NOT_LEADER))
                .build();
    }

    /** 当值 Leader 的闩放行应答（计数已归零）。 */
    private static Envelope latchReleased(Envelope req) {
        return req.toBuilder().setType(MessageType.LATCH_AWAIT)
                .setLatchAwaitResponse(LatchAwaitResponse.newBuilder()
                        .setStatus(StatusCode.OK))
                .build();
    }

    /** 陈旧 home 对屏障 await 的同型 `NOT_LEADER` 应答。 */
    private static Envelope barrierNotLeader(Envelope req) {
        return req.toBuilder().setType(MessageType.BARRIER_AWAIT)
                .setBarrierAwaitResponse(BarrierAwaitResponse.newBuilder()
                        .setStatus(StatusCode.NOT_LEADER))
                .build();
    }

    /** 当值 Leader 的屏障合拢应答（非执行者形态，世代 1 已放行）。 */
    private static Envelope barrierTripped(Envelope req) {
        return req.toBuilder().setType(MessageType.BARRIER_AWAIT)
                .setBarrierAwaitResponse(BarrierAwaitResponse.newBuilder()
                        .setStatus(StatusCode.OK).setGeneration(1).setParties(1))
                .build();
    }

    /**
     * 选举空窗的集群视图：无 `is_leader` 条目——刷新探测判为本轮无结果
     * （静默降级），故永久 `NOT_LEADER` 形态下路由无从改变。
     */
    private static Envelope clusterViewWithoutLeader(Envelope req) {
        return req.toBuilder().setClusterView(ClusterView.newBuilder()
                .setStatus(StatusCode.OK)
                .addNodes(NodeInfo.newBuilder().setNodeId(1)
                        .setAddress("127.0.0.1:1").setIsLeader(false))
                .build()).build();
    }

    /**
     * 建连客户端（以陈旧 home 为种子）。
     *
     * @param seedAddress 种子地址
     * @return 已建连的客户端（由调用方关闭）
     * @throws Exception 建连失败或超时
     */
    private static OpenLatchClient connect(String seedAddress) throws Exception {
        OpenLatchClient client = OpenLatchClient.builder()
                .address(seedAddress)
                .requestTimeout(Duration.ofSeconds(2))
                .defaultWaitTimeout(Duration.ofSeconds(10))
                .build();
        client.connectAsync().get(5, TimeUnit.SECONDS);
        return client;
    }
}
