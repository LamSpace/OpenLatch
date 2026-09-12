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

import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * FairLock 公平性回归套件·集群档（常开）：Leader 侧
 * {@code WaitQueue} 位次经复制路径推进时"通知序 == 排队序 == 授予序"
 * 依旧成立（位次裁决在 Leader 内存、授予裁决经日志，两层不得合谋插队）。
 * {@code FAIR} 与 {@code REENTRANT} 参数化同矩阵。
 */
class FairOrderingSuiteClusterTest {

    /** 构造排队式获取信封。 */
    private static Envelope acquire(long requestId, String key, LockType type) {
        return Envelope.newBuilder()
                .setProtocolVersion(3)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(requestId)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey(key).setLockType(type).setThreadId(1).setWaitMs(-1))
                .build();
    }

    /** 构造释放信封。 */
    private static Envelope release(long requestId, String key, long token) {
        return Envelope.newBuilder()
                .setProtocolVersion(3)
                .setType(MessageType.LOCK_RELEASE)
                .setRequestId(requestId)
                .setReleaseRequest(ReleaseRequest.newBuilder()
                        .setKey(key).setLeaseToken(token).setThreadId(1))
                .build();
    }

    @ParameterizedTest(name = "type={0}")
    @EnumSource(value = LockType.class, names = {"LOCK_TYPE_FAIR", "LOCK_TYPE_REENTRANT"})
    void clusterGrantOrderMatchesEnqueueOrder(LockType type) throws Exception {
        String key = "fair-cluster";
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn holder = h.connect(leader);
            holder.hello(1, 3);

            Envelope g = holder.request(acquire(2, key, type));
            assertThat(g.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            long token = g.getAcquireResponse().getLeaseToken();

            ClusterHarness.TestConn[] waiters = new ClusterHarness.TestConn[3];
            long[] reqIds = new long[3];
            for (int i = 0; i < 3; i++) {
                waiters[i] = h.connect(leader);
                waiters[i].hello(10 + i, 3);
                reqIds[i] = 20 + i;
                Envelope q = waiters[i].request(acquire(reqIds[i], key, type));
                assertThat(q.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
                assertThat(q.getAcquireResponse().getQueuePosition()).isEqualTo(i + 1);
            }

            // 逐轮：持有者释放（经复制提交）→ 恰队首收到通知 → 同 requestId
            // 重发被授予。授予序 == 入队序 W0→W1→W2。
            ClusterHarness.TestConn currentHolder = holder;
            long currentToken = token;
            long currentRequestId = 100;
            for (int i = 0; i < 3; i++) {
                currentHolder.request(release(currentRequestId++, key, currentToken));

                Envelope push = waiters[i].awaitOutbound(10_000);
                assertThat(push.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
                assertThat(push.getAwaitNotify().getRequestIdRef()).isEqualTo(reqIds[i]);

                Envelope g2 = waiters[i].request(acquire(reqIds[i], key, type));
                assertThat(g2.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
                currentHolder = waiters[i];
                currentToken = g2.getAcquireResponse().getLeaseToken();
            }
            currentHolder.request(release(currentRequestId, key, currentToken));
            assertThat(leader.lastApplied()).isPositive();
        }
    }
}
