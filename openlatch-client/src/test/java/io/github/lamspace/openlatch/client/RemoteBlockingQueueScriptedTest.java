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
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.QueueOp;
import io.github.lamspace.openlatch.protocol.QueueOpRequest;
import io.github.lamspace.openlatch.protocol.QueueOpResponse;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@link RemoteBlockingQueue} 车道单测（v7，{@link ScriptedServer} 直驱）：
 * 工厂参数守门、请求形状装配（阻塞位/容量主张/元素 presence/op_seq 分配与
 * 读类填 0/UTF-8 两形态等价/drain 与 delay 字段）、QUEUED→推送丢失→同信封
 * 自兜底重发→OK 闭环、DENIED 回弹继续等待、不可重试拒绝显式抛错零重发、
 * NOT_LEADER 同信封同序号重试。
 */
@Timeout(value = 60, unit = TimeUnit.SECONDS)
class RemoteBlockingQueueScriptedTest {

    /** HELLO 应答（v7 服务端形态）。 */
    private static Envelope hello(Envelope req) {
        return req.toBuilder().setHelloResponse(HelloResponse.newBuilder()
                .setStatus(StatusCode.OK).setSessionId(9001L)
                .setServerProtocolVersion(7).setDefaultLeaseMs(30_000))
                .build();
    }

    /** 构造队列应答信封（回挂请求的 op 回显）。 */
    private static Envelope queueResp(Envelope req, StatusCode status) {
        QueueOpRequest q = req.getQueueOpRequest();
        QueueOpResponse.Builder b = QueueOpResponse.newBuilder()
                .setStatus(status).setOp(q.getOp());
        return req.toBuilder().setType(MessageType.QUEUE_OP)
                .setQueueOpResponse(b).build();
    }

    @Test
    void factoryGuardsRejectBadArguments() {
        try (OpenLatchClient client = OpenLatchClient.builder().address("127.0.0.1:1").build()) {
            assertThatThrownBy(() -> client.newBlockingQueue(null, 4))
                    .isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> client.newBlockingQueue("k", 0))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> client.newDelayQueue("k", -1))
                    .isInstanceOf(IllegalArgumentException.class);
            OBlockingQueue plain = client.newBlockingQueue("k", 4);
            assertThat(plain.capacity()).isEqualTo(4);
            assertThat(plain.key()).isEqualTo("k");
            // 元素不可为 null（rejectNull 契约）：本地即拒、不发请求。
            assertThatThrownBy(() -> plain.put((byte[]) null))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> plain.offer((String) null))
                    .isInstanceOf(IllegalArgumentException.class);
            // 负超时与负批量本地拒。
            assertThatThrownBy(() -> plain.offer(new byte[] {1}, -1, TimeUnit.SECONDS))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> plain.drainTo(new ArrayList<byte[]>(), -1))
                    .isInstanceOf(IllegalArgumentException.class);
            // QUEUE 形态句柄调 offerDelayed：本地形态拒（不发请求）。
            assertThatThrownBy(() -> ((ODelayQueue) plain).offerDelayed("x", 1,
                    TimeUnit.SECONDS))
                    .isInstanceOf(IllegalStateException.class);
        }
    }

    @Test
    void shapeAssemblyUtf8AndReadOps() throws Exception {
        List<Envelope> seen = new ArrayList<>();
        try (ScriptedServer server = new ScriptedServer(req -> {
            if (req.getType() == MessageType.HELLO) {
                return hello(req);
            }
            seen.add(req);
            QueueOp op = req.getQueueOpRequest().getOp();
            QueueOpResponse.Builder b = QueueOpResponse.newBuilder()
                    .setStatus(StatusCode.OK).setOp(op);
            if (op == QueueOp.QUEUE_OP_PEEK) {
                b.setElementBytes(com.google.protobuf.ByteString
                        .copyFrom("头".getBytes(StandardCharsets.UTF_8)));
            }
            if (op == QueueOp.QUEUE_OP_SIZE) {
                b.setSize(7).setCapacity(4);
            }
            if (op == QueueOp.QUEUE_OP_DRAIN) {
                b.addDrainedBytes(com.google.protobuf.ByteString
                        .copyFrom("a".getBytes(StandardCharsets.UTF_8)));
                b.addDrainedBytes(com.google.protobuf.ByteString.EMPTY);
            }
            return req.toBuilder().setQueueOpResponse(b).build();
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(5))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                ODelayQueue queue = client.newDelayQueue("q", 4);

                assertThat(queue.offer("任务A")).isTrue();
                QueueOpRequest put = seen.get(0).getQueueOpRequest();
                assertThat(seen.get(0).getType()).isEqualTo(MessageType.QUEUE_OP);
                assertThat(put.getOp()).isEqualTo(QueueOp.QUEUE_OP_PUT);
                assertThat(put.getBlocking()).isFalse();
                assertThat(put.getCapacity()).isEqualTo(4);
                assertThat(put.getLockType()).isEqualTo(LockType.LOCK_TYPE_DELAY_QUEUE);
                assertThat(put.getElementBytes().toString(StandardCharsets.UTF_8))
                        .isEqualTo("任务A");
                assertThat(put.getOpSeq()).isGreaterThanOrEqualTo(1);

                assertThat(queue.offerDelayed("late", 300, TimeUnit.MILLISECONDS)).isTrue();
                assertThat(seen.get(1).getQueueOpRequest().getDelayMs()).isEqualTo(300);

                assertThat(queue.poll()).isNull(); // 服务端 OK 无元素 presence
                assertThat(seen.get(2).getQueueOpRequest().getOp())
                        .isEqualTo(QueueOp.QUEUE_OP_TAKE);
                assertThat(seen.get(2).getQueueOpRequest().getBlocking()).isFalse();

                assertThat(new String(queue.peek(), StandardCharsets.UTF_8)).isEqualTo("头");
                assertThat(seen.get(3).getQueueOpRequest().getOpSeq()).isZero();
                assertThat(queue.size()).isEqualTo(7);
                assertThat(seen.get(4).getQueueOpRequest().getOpSeq()).isZero();

                List<byte[]> sink = new ArrayList<>();
                assertThat(queue.drainTo(sink, 64)).isEqualTo(2);
                QueueOpRequest drain = seen.get(5).getQueueOpRequest();
                assertThat(queue.remainingCapacity()).isZero(); // 4 − 7 下限 0
                assertThat(drain.getMaxElements()).isEqualTo(64);
                assertThat(drain.getOp()).isEqualTo(QueueOp.QUEUE_OP_DRAIN);
                assertThat(new String(sink.get(0), StandardCharsets.UTF_8)).isEqualTo("a");
                assertThat(sink.get(1)).isEmpty(); // 空串元素：零长度非 null
            }
        }
    }

    @Test
    void queuedThenPushLostSelfResendClosesLoop() throws Exception {
        List<Envelope> seen = new ArrayList<>();
        try (ScriptedServer server = new ScriptedServer(req -> {
            if (req.getType() == MessageType.HELLO) {
                return hello(req);
            }
            seen.add(req);
            StatusCode status = seen.size() == 1 ? StatusCode.QUEUED : StatusCode.OK;
            return queueResp(req, status);
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofMillis(300))
                    .defaultWaitTimeout(Duration.ofSeconds(10))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                // 推送不送达——读界兜底自发同信封重发，第二次 OK。
                client.newBlockingQueue("q", 2).put("x".getBytes(StandardCharsets.UTF_8));
            }
        }
        assertThat(seen).hasSizeGreaterThanOrEqualTo(2);
        assertThat(seen.get(0).getQueueOpRequest().getBlocking()).isTrue();
        // 同信封语义：同 requestId、同 op_seq 重发（服务端去重槽的充分条件）。
        assertThat(seen.get(1).getRequestId()).isEqualTo(seen.get(0).getRequestId());
        assertThat(seen.get(1).getQueueOpRequest().getOpSeq())
                .isEqualTo(seen.get(0).getQueueOpRequest().getOpSeq());
    }

    @Test
    void deniedBounceKeepsWaitingUntilDelivery() throws Exception {
        List<Envelope> seen = new ArrayList<>();
        try (ScriptedServer server = new ScriptedServer(req -> {
            if (req.getType() == MessageType.HELLO) {
                return hello(req);
            }
            seen.add(req);
            int n = seen.size();
            if (n == 1) {
                return queueResp(req, StatusCode.QUEUED);
            }
            if (n == 2) {
                return queueResp(req, StatusCode.DENIED); // 回弹缺口的防御兜底
            }
            return req.toBuilder().setQueueOpResponse(QueueOpResponse.newBuilder()
                    .setStatus(StatusCode.OK).setOp(QueueOp.QUEUE_OP_TAKE)
                    .setElementBytes(com.google.protobuf.ByteString
                            .copyFrom("e".getBytes(StandardCharsets.UTF_8)))).build();
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofMillis(200))
                    .defaultWaitTimeout(Duration.ofSeconds(10))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                byte[] got = client.newBlockingQueue("q", 2).take();
                assertThat(new String(got, StandardCharsets.UTF_8)).isEqualTo("e");
            }
        }
        assertThat(seen).hasSize(3);
        // 三轮同 rid 同 op_seq——回弹对调用者透明。
        assertThat(seen.get(1).getRequestId()).isEqualTo(seen.get(0).getRequestId());
        assertThat(seen.get(2).getRequestId()).isEqualTo(seen.get(0).getRequestId());
    }

    @Test
    void explicitRejectionIsTerminalWithoutRetry() throws Exception {
        try (ScriptedServer server = new ScriptedServer(req ->
                req.getType() == MessageType.HELLO ? hello(req) : queueResp(req,
                        StatusCode.INVALID_REQUEST))) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(3))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                OBlockingQueue queue = client.newBlockingQueue("q", 2);
                assertThatThrownBy(() -> queue.offer("x".getBytes(StandardCharsets.UTF_8)))
                        .isInstanceOf(OpenLatchException.class)
                        .hasMessageContaining("INVALID_REQUEST");
                // 恰一次 QUEUE_OP：不可重试分类零重发。
                assertThat(server.countType(MessageType.QUEUE_OP)).isEqualTo(1);
            }
        }
    }

    @Test
    void notLeaderRetriesSameEnvelopeSameOpSeq() throws Exception {
        List<Envelope> seen = new ArrayList<>();
        try (ScriptedServer server = new ScriptedServer(req -> {
            if (req.getType() == MessageType.HELLO) {
                return hello(req);
            }
            seen.add(req);
            return queueResp(req, seen.size() == 1 ? StatusCode.NOT_LEADER : StatusCode.OK);
        })) {
            try (OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .requestTimeout(Duration.ofSeconds(3))
                    .build()) {
                client.connectAsync().get(5, TimeUnit.SECONDS);
                assertThat(client.newBlockingQueue("q", 2)
                        .offer("y".getBytes(StandardCharsets.UTF_8))).isTrue();
            }
        }
        assertThat(seen).hasSize(2);
        assertThat(seen.get(1).getQueueOpRequest().getOpSeq())
                .isEqualTo(seen.get(0).getQueueOpRequest().getOpSeq());
    }
}
