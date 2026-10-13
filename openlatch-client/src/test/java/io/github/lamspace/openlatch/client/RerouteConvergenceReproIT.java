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
import io.github.lamspace.openlatch.protocol.ClusterView;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
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
}
