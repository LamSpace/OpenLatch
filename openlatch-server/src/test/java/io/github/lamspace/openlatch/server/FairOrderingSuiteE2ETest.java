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

package io.github.lamspace.openlatch.server;

import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * FairLock 公平性回归套件·服务端端到端档（Phase 3 P3-02，验收 §8-1 常开）：
 * 真实端口与真实协议收发，断言"通知推送顺序 == 排队顺序、授予顺序 == 排队
 * 顺序"贯穿线路层。与 core 直驱档（{@code FairOrderingSuiteTest}）、集群档
 * （{@code FairOrderingSuiteClusterTest}）共享同一场景骨架。
 */
class FairOrderingSuiteE2ETest {

    /** 被测内嵌服务器。 */
    private OpenLatchServer server;

    /** 关停本用例服务器。 */
    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop();
        }
    }

    /** 构造 FAIR 排队式获取信封。 */
    private static Envelope fair(long requestId, String key) {
        return acquire(requestId, key, LockType.LOCK_TYPE_FAIR);
    }

    /** 构造排队式获取信封。 */
    private static Envelope acquire(long requestId, String key, LockType type) {
        return Envelope.newBuilder()
                .setProtocolVersion(OpenLatchServer.PROTOCOL_VERSION)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(requestId)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey(key).setLockType(type).setThreadId(1).setWaitMs(-1))
                .build();
    }

    /** 构造释放信封。 */
    private static Envelope release(long requestId, String key, long token) {
        return Envelope.newBuilder()
                .setProtocolVersion(OpenLatchServer.PROTOCOL_VERSION)
                .setType(MessageType.LOCK_RELEASE)
                .setRequestId(requestId)
                .setReleaseRequest(ReleaseRequest.newBuilder()
                        .setKey(key).setLeaseToken(token).setThreadId(1))
                .build();
    }

    @Test
    @Timeout(30)
    void fairGrantOrderMatchesArrivalOrderOverTheWire() throws Exception {
        server = TestServers.start(TestServers.config(0));
        String key = "fair-e2e";

        TestProtocolClient holder = new TestProtocolClient();
        TestProtocolClient[] waiters = new TestProtocolClient[3];
        try {
            holder.connect("127.0.0.1", server.port());
            holder.hello();
            for (int i = 0; i < 3; i++) {
                waiters[i] = new TestProtocolClient();
                waiters[i].connect("127.0.0.1", server.port());
                waiters[i].hello();
            }

            Envelope g = holder.sendAndAwait(fair(holder.nextRequestId(), key));
            assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            long token = g.getAcquireResponse().getLeaseToken();

            long[] reqIds = new long[3];
            for (int i = 0; i < 3; i++) {
                reqIds[i] = waiters[i].nextRequestId();
                Envelope q = waiters[i].sendAndAwait(fair(reqIds[i], key));
                assertThat(q.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
                assertThat(q.getAcquireResponse().getQueuePosition()).isEqualTo(i + 1);
            }

            // 逐轮释放：通知与授予严格按 W0→W1→W2 的入队序推进。
            long currentToken = token;
            TestProtocolClient currentHolder = holder;
            for (int i = 0; i < 3; i++) {
                Envelope rel = currentHolder.sendAndAwait(release(currentHolder.nextRequestId(), key, currentToken));
                assertThat(rel.getReleaseResponse().getStatus()).isEqualTo(StatusCode.OK);

                Envelope push = waiters[i].awaitPush(5000);
                assertThat(push).isNotNull();
                assertThat(push.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
                assertThat(push.getAwaitNotify().getRequestIdRef()).isEqualTo(reqIds[i]);
                // 其余等待者不得先行收到通知（严格 FIFO；300ms 静默窗内
                // 若有越位推送会立即落入断言）。
                for (int j = i + 1; j < 3; j++) {
                    assertThat(waiters[j].awaitPush(300)).isNull();
                }

                Envelope g2 = waiters[i].sendAndAwait(fair(reqIds[i], key));
                assertThat(g2.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
                currentToken = g2.getAcquireResponse().getLeaseToken();
                currentHolder = waiters[i];
            }
        } finally {
            holder.close();
            for (TestProtocolClient w : waiters) {
                if (w != null) {
                    w.close();
                }
            }
        }
    }

    @Test
    @Timeout(30)
    void lateArrivalCannotOvertakeInNotifyWindowOverTheWire() throws Exception {
        server = TestServers.start(TestServers.config(0));
        String key = "fair-window-e2e";

        try (TestProtocolClient a = new TestProtocolClient();
             TestProtocolClient b = new TestProtocolClient();
             TestProtocolClient c = new TestProtocolClient()) {
            for (TestProtocolClient t : new TestProtocolClient[] {a, b, c}) {
                t.connect("127.0.0.1", server.port());
                t.hello();
            }
            Envelope ga = a.sendAndAwait(fair(a.nextRequestId(), key));
            assertThat(ga.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            long rb = b.nextRequestId();
            assertThat(b.sendAndAwait(fair(rb, key)).getAcquireResponse().getStatus())
                    .isEqualTo(StatusCode.QUEUED);

            a.sendAndAwait(release(a.nextRequestId(), key, ga.getAcquireResponse().getLeaseToken()));
            // b 处于"已通知、待重发"窗口：c 新到必须排队尾（位次 2）。
            Envelope qc = c.sendAndAwait(fair(c.nextRequestId(), key));
            assertThat(qc.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            assertThat(qc.getAcquireResponse().getQueuePosition()).isEqualTo(2);
        }
    }
}
