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

import com.google.protobuf.ByteString;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LatchAwaitRequest;
import io.github.lamspace.openlatch.protocol.LatchCountDownRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.QueueOp;
import io.github.lamspace.openlatch.protocol.QueueOpRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 集群拒绝应答码形同型测试：{@code QUEUE_OP}、{@code LATCH_COUNT_DOWN}、
 * {@code LATCH_AWAIT} 被非当值 Leader 节点拒绝（角色门或提交失败）时，
 * 应答 MUST 携带与请求同型的 Response 载荷并自述 {@code NOT_LEADER}——
 * 拒绝状态码线路可见，客户端 MUST NOT 可能把拒绝读成 protobuf 默认实例
 * 的"成功空应答"（{@code StatusCode.OK} 为枚举零值）。
 *
 * <p><b>判红即根因实证</b>：修复前这三型拒绝落 {@code notLeaderEnvelope}
 * 的 default 分支，应答 type 回显请求类型但 payload 为异型
 * {@code acquire_response}，本文件断言在现网形态下逐条失败。
 *
 * <p><b>路径覆盖</b>：队列两型与 LATCH_AWAIT 走 {@code requireLeader} 角色门
 * 拒绝。判读记录：{@code LATCH_COUNT_DOWN} 不过门，健康 Follower 上的提交经
 * Ratis 客户端重定向<b>正常落值</b>（返回真 {@code latch_count_down_response}），
 * {@code commitFailure} 仅在 Ratis 层失败窗显形、集群基座上不可构造确定性触发；
 * 该路与角色门在 {@code notLeaderEnvelope} 同一私有助手汇合，其码形由编解码器
 * 级表驱动测试（{@code RejectCodecTableTest}）直接实证。
 *
 * <p>基座 {@link ClusterHarness}，判例 {@code LeaderFailoverServerTest}/
 * {@code ClusterQueueTest}。
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ClusterRejectShapeTest {

    /** 构造队列操作信封（v7，形状合法：读 op op_seq 填 0、写 op 填非零）。 */
    private static Envelope queue(long rid, String key, QueueOp op, boolean blocking,
            long capacity, byte[] element, long opSeq) {
        QueueOpRequest.Builder rb = QueueOpRequest.newBuilder()
                .setKey(key).setOp(op).setLockType(LockType.LOCK_TYPE_QUEUE)
                .setBlocking(blocking).setCapacity(capacity).setOpSeq(opSeq);
        if (element != null) {
            rb.setElementBytes(ByteString.copyFrom(element));
        }
        return Envelope.newBuilder().setProtocolVersion(7).setType(MessageType.QUEUE_OP)
                .setRequestId(rid).setQueueOpRequest(rb).build();
    }

    /** 构造 LATCH_COUNT_DOWN 信封（v3，形状合法）。 */
    private static Envelope countDown(long rid, String key) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LATCH_COUNT_DOWN)
                .setRequestId(rid)
                .setLatchCountDownRequest(LatchCountDownRequest.newBuilder()
                        .setKey(key).setCount(1).setTotal(1))
                .build();
    }

    /** 构造 LATCH_AWAIT 信封（v3，形状合法）。 */
    private static Envelope latchAwait(long rid, String key) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.LATCH_AWAIT)
                .setRequestId(rid)
                .setLatchAwaitRequest(LatchAwaitRequest.newBuilder().setKey(key).setTotal(1))
                .build();
    }

    /** 取一个存活 Follower。 */
    private static ClusterHarness.Node oneFollower(ClusterHarness h) {
        return h.nodes().stream().filter(x -> x.alive() && !x.isLeader()).findFirst()
                .orElseThrow();
    }

    /**
     * 1.1：Follower 角色门拒绝 QUEUE_OP（SIZE 与 TAKE 各一）——应答 MUST 为
     * 同型 {@code queue_op_response}、{@code NOT_LEADER}、op 回显。
     * 判红点：修复前 {@code hasQueueOpResponse()==false}（异型 acquire_response）。
     */
    @Test
    void followerRejectsQueueOpWithSameShapeNotLeader() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.TestConn c = h.connect(oneFollower(h));
            assertThat(c.hello(1, 7).getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);

            Envelope sizeResp = c.request(queue(2, "shape-q", QueueOp.QUEUE_OP_SIZE,
                    false, 0, null, 0));
            assertThat(sizeResp.getType()).isEqualTo(MessageType.QUEUE_OP);
            assertThat(sizeResp.hasQueueOpResponse())
                    .as("SIZE 拒绝须携同型 queue_op_response")
                    .isTrue();
            assertThat(sizeResp.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.NOT_LEADER);
            assertThat(sizeResp.getQueueOpResponse().getOp()).isEqualTo(QueueOp.QUEUE_OP_SIZE);

            Envelope takeResp = c.request(queue(3, "shape-q", QueueOp.QUEUE_OP_TAKE,
                    false, 0, null, 1));
            assertThat(takeResp.hasQueueOpResponse())
                    .as("TAKE 拒绝须携同型 queue_op_response")
                    .isTrue();
            assertThat(takeResp.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.NOT_LEADER);
            assertThat(takeResp.getQueueOpResponse().getOp()).isEqualTo(QueueOp.QUEUE_OP_TAKE);
        }
    }

    /**
     * 1.2：LATCH 两型路径判读——LATCH_COUNT_DOWN 不过角色门，健康 Follower
     * 上经 Ratis 重定向透明提交（同型应答、状态非拒绝：判读记录此路径在
     * 集群基座上不可构造 commit-failure，其拒绝码形由 {@code RejectCodecTableTest}
     * 编解码器级实证）；LATCH_AWAIT 走角色门，MUST 携同型
     * {@code latch_await_response} 与 {@code NOT_LEADER}——杜绝客户端把拒绝
     * 读成"已破障"的默认实例假绿。判红点：AWAIT 段修复前
     * {@code hasLatchAwaitResponse()==false}。
     */
    @Test
    void latchCountDownCommitsWhileLatchAwaitRejectedSameShapeOnFollower()
            throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.TestConn c = h.connect(oneFollower(h));
            assertThat(c.hello(1, 3).getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);

            // countDown 经 Ratis 重定向在 Follower 上透明提交：同型应答、真实结果
            // （count 1 / total 1 → remaining 0）。其拒绝码形仅在失败窗显形，
            // 由 RejectCodecTableTest 于编解码器级覆盖（本路径经 commitFailure
            // 与角色门共用 notLeaderEnvelope 单一出口）。
            Envelope cdResp = c.request(countDown(2, "shape-latch"));
            assertThat(cdResp.hasLatchCountDownResponse())
                    .as("countDown 应答须携同型 latch_count_down_response")
                    .isTrue();
            assertThat(cdResp.getLatchCountDownResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            assertThat(cdResp.getLatchCountDownResponse().getRemaining()).isEqualTo(0);

            // await 被角色门拒：同型 NOT_LEADER。
            Envelope awaitResp = c.request(latchAwait(3, "shape-latch"));
            assertThat(awaitResp.hasLatchAwaitResponse())
                    .as("LATCH_AWAIT 拒绝须携同型 latch_await_response")
                    .isTrue();
            assertThat(awaitResp.getLatchAwaitResponse().getStatus())
                    .isEqualTo(StatusCode.NOT_LEADER);
        }
    }

    /**
     * 1.4：确定性 E2E——元素经当值 Leader 真实提交（两元素、副本影子表追平）
     * 后，钉在 Follower 的队列请求 MUST 以同型 {@code NOT_LEADER} 拒之且
     * 影子深度零扰动（拒绝非伪成功、亦不触碰状态）。修复前此处读回的是
     * "OK+0/OK+空"伪成功形态。
     */
    @Test
    void rejectedQueueOpOnFollowerLeavesCommittedElementsUntouched() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.TestConn leader = h.connect(h.leader());
            assertThat(leader.hello(1, 7).getHelloResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            for (int i = 0; i < 2; i++) {
                Envelope put = leader.request(queue(10L + i, "shape-e2e",
                        QueueOp.QUEUE_OP_PUT, false, 4,
                        ("e" + i).getBytes(StandardCharsets.UTF_8), i + 1));
                assertThat(put.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.OK);
            }
            h.awaitTrue(h::aliveAgreeWithLeader, 20_000, "副本影子表追平");

            ClusterHarness.Node followerNode = oneFollower(h);
            ClusterHarness.TestConn c = h.connect(followerNode);
            assertThat(c.hello(2, 7).getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);

            Envelope sizeResp = c.request(queue(3, "shape-e2e", QueueOp.QUEUE_OP_SIZE,
                    false, 0, null, 0));
            assertThat(sizeResp.hasQueueOpResponse()).isTrue();
            assertThat(sizeResp.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.NOT_LEADER);

            Envelope takeResp = c.request(queue(4, "shape-e2e", QueueOp.QUEUE_OP_TAKE,
                    false, 0, null, 1));
            assertThat(takeResp.hasQueueOpResponse()).isTrue();
            assertThat(takeResp.getQueueOpResponse().getStatus()).isEqualTo(StatusCode.NOT_LEADER);

            // 拒绝零状态变更：两元素照常驻留。
            assertThat(followerNode.runtime.core().shadow().queueDepth("shape-e2e"))
                    .isEqualTo(2);
        }
    }

}
