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
import io.github.lamspace.openlatch.protocol.AcquireResponse;
import io.github.lamspace.openlatch.protocol.ClusterView;
import io.github.lamspace.openlatch.protocol.ConditionOpResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.NodeInfo;
import io.github.lamspace.openlatch.protocol.QueueOpRequest;
import io.github.lamspace.openlatch.protocol.QueueOpResponse;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.TimerOpResponse;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * **会话换代静默续发**的确定性复现夹具（判据，非回归网）。
 *
 * <p><b>要回答的问题</b>：一条变异请求若先经历**不确定窗**（应答丢失、效果不明），
 * 随后路由换代，它是否会在**新会话**上续发？——服务端去重槽按 {@code (会话, op_seq)}
 * 记账，换会话即槽不中，续发等同新写 ⇒ 与"静默重复生效"同族。
 *
 * <p><b>三条用例各证一半，合起来才是完整判据</b>：
 * <ol>
 *   <li>条件族（{@code signal}）与定时器族（{@code SCHEDULE}）：不确定窗后
 *       **根本不重道**——请求以显式失败收场，且**真主一次都没被建道**。
 *       "零 HELLO"是断言本身，不是辅助检查。</li>
 *   <li>队列（**对照**）：同一不确定窗之后收到一次明确零生效拒绝（缺码形）——
 *       该分支会触发共享刷新，车道**确实被建到真主**（真主有 HELLO）；即便如此，
 *       在途写**仍然不续发**。这一条证明的是"守卫挡住了续发"，而非"压根无路可换"。</li>
 * </ol>
 *
 * <p>三条合起来回答：两族在不确定窗后的处置是**放弃**，且该放弃**不是靠"无路可换"**
 * 实现的——队列那条证明换会话的机会确实存在过。
 *
 * <p><b>来历与现状</b>：本类原以 {@code @Tag("w11-repro")} 门控（当时与同族夹具
 * 同处"无修复必红"的判据集）；结论落地后与 profile 一并转正，**现由默认构建执行**，
 * 守的是"变异车道在不确定窗后放弃、不跨会话续发"这条契约——谁把它改成续发，默认
 * CI 即刻见红。
 */
@Timeout(value = 90, unit = TimeUnit.SECONDS)
class SessionSwitchReproIT {

    /**
     * HELLO（指定会话 id：两个节点 = 两个会话）。
     *
     * <p>{@code leader_hint=-1} 是**必要**的：proto3 默认 {@code 0} 会被客户端判为
     * 单机形态，从而静默关闭路由刷新（{@code clusterHintCapable} 门）——该坑已由
     * 构造有效性断言实测抓到过一次，故在此显式记下。
     *
     * @param req 请求信封
     * @param sid 会话 id
     * @return HELLO 应答
     */
    private static Envelope hello(Envelope req, long sid) {
        return req.toBuilder().setHelloResponse(HelloResponse.newBuilder()
                .setStatus(StatusCode.OK).setSessionId(sid)
                .setServerProtocolVersion(9).setDefaultLeaseMs(30_000)
                .setLeaderHint(-1))
                .build();
    }

    /** 集群视图（指向真主，供共享刷新建道）。 */
    private static Envelope view(Envelope req, String leaderAddr) {
        return req.toBuilder().setClusterView(ClusterView.newBuilder()
                .setStatus(StatusCode.OK)
                .addNodes(NodeInfo.newBuilder().setNodeId(1)
                        .setAddress("127.0.0.1:1").setIsLeader(false))
                .addNodes(NodeInfo.newBuilder().setNodeId(2)
                        .setAddress(leaderAddr).setIsLeader(true))
                .build()).build();
    }

    /** 获取授予（条件请求的前置：signal 要求本线程持有锁）。 */
    private static Envelope acquireOk(Envelope req) {
        return req.toBuilder().setType(MessageType.LOCK_ACQUIRE)
                .setAcquireResponse(AcquireResponse.newBuilder()
                        .setStatus(StatusCode.OK).setLeaseToken(11L).setGrantedLeaseMs(30_000)
                        .setLeaseExpiresAtMs(System.currentTimeMillis() + 30_000))
                .build();
    }

    /** 条件应答（op 回显）。 */
    private static Envelope condResp(Envelope req, StatusCode status) {
        return req.toBuilder().setType(MessageType.CONDITION_OP)
                .setConditionOpResponse(ConditionOpResponse.newBuilder()
                        .setStatus(status).setOp(req.getConditionOpRequest().getOp()))
                .build();
    }

    /** 定时器应答（op 回显）。 */
    private static Envelope timerResp(Envelope req, StatusCode status) {
        return req.toBuilder().setType(MessageType.TIMER_OP)
                .setTimerOpResponse(TimerOpResponse.newBuilder()
                        .setStatus(status).setOp(req.getTimerOpRequest().getOp()))
                .build();
    }

    /** 队列应答（op 回显）。 */
    private static Envelope queueResp(Envelope req, StatusCode status) {
        QueueOpRequest q = req.getQueueOpRequest();
        return req.toBuilder().setType(MessageType.QUEUE_OP)
                .setQueueOpResponse(QueueOpResponse.newBuilder()
                        .setStatus(status).setOp(q.getOp()))
                .build();
    }

    /**
     * 条件族：{@code signal} 首发不应答（不确定窗）后，MUST 显式失败且 MUST NOT 重道。
     */
    @Test
    void conditionSignalAbandonsWithoutRerouteAfterUncertainWindow() throws Exception {
        try (ScriptedServer trueLeader = new ScriptedServer(req -> switch (req.getType()) {
                    case HELLO -> hello(req, 2000L);
                    case CONDITION_OP -> condResp(req, StatusCode.OK);
                    default -> null;
                })) {
            AtomicInteger condAttempts = new AtomicInteger();
            try (ScriptedServer home = new ScriptedServer(req -> switch (req.getType()) {
                        case HELLO -> hello(req, 1000L);
                        case LOCK_ACQUIRE -> acquireOk(req);
                        case CLUSTER_VIEW -> view(req, trueLeader.address());
                        case CONDITION_OP -> condAttempts.incrementAndGet() == 1
                                ? null : condResp(req, StatusCode.NOT_LEADER);
                        default -> null;
                    })) {
                AtomicReference<Throwable> outcome = new AtomicReference<>();
                try (OpenLatchClient client = OpenLatchClient.builder()
                        .address(home.address())
                        .requestTimeout(Duration.ofSeconds(1))
                        .defaultWaitTimeout(Duration.ofSeconds(20))
                        .build()) {
                    client.connectAsync().get(5, TimeUnit.SECONDS);
                    OLock lock = client.newReentrantLock("repro-cond-lock");
                    lock.lock();
                    OCondition cond = lock.newCondition("repro-cond");
                    Thread signaller = new Thread(() -> {
                        try {
                            cond.signal();
                        } catch (Throwable t) {
                            outcome.set(t);
                        }
                    });
                    signaller.start();
                    signaller.join(30_000);
                    assertThat(signaller.isAlive()).isFalse();
                }
                assertThat(outcome.get())
                        .as("不确定窗后 MUST 显式失败（不静默续发、不假绿）").isNotNull();
                assertThat(trueLeader.countType(MessageType.CONDITION_OP))
                        .as("MUST NOT 在新会话上续发该 signal").isZero();
                assertThat(trueLeader.countType(MessageType.HELLO))
                        .as("且该路径 MUST NOT 触发重道——真主一次都未被建道").isZero();
            }
        }
    }

    /**
     * 定时器族：{@code SCHEDULE} 首发不应答（不确定窗）后，MUST 显式失败且 MUST NOT 重道。
     */
    @Test
    void timerScheduleAbandonsWithoutRerouteAfterUncertainWindow() throws Exception {
        try (ScriptedServer trueLeader = new ScriptedServer(req -> switch (req.getType()) {
                    case HELLO -> hello(req, 2000L);
                    case TIMER_OP -> timerResp(req, StatusCode.OK);
                    default -> null;
                })) {
            AtomicInteger timerAttempts = new AtomicInteger();
            try (ScriptedServer home = new ScriptedServer(req -> switch (req.getType()) {
                        case HELLO -> hello(req, 1000L);
                        case CLUSTER_VIEW -> view(req, trueLeader.address());
                        case TIMER_OP -> timerAttempts.incrementAndGet() == 1
                                ? null : timerResp(req, StatusCode.NOT_LEADER);
                        default -> null;
                    })) {
                AtomicReference<Throwable> outcome = new AtomicReference<>();
                try (OpenLatchClient client = OpenLatchClient.builder()
                        .address(home.address())
                        .requestTimeout(Duration.ofSeconds(1))
                        .defaultWaitTimeout(Duration.ofSeconds(20))
                        .build()) {
                    client.connectAsync().get(5, TimeUnit.SECONDS);
                    OTimer timer = client.newTimer("repro-timer");
                    Thread scheduler = new Thread(() -> {
                        try {
                            timer.schedule(60_000, TimeUnit.MILLISECONDS);
                        } catch (Throwable t) {
                            outcome.set(t);
                        }
                    });
                    scheduler.start();
                    scheduler.join(30_000);
                    assertThat(scheduler.isAlive()).isFalse();
                }
                assertThat(outcome.get())
                        .as("不确定窗后 MUST 显式失败（不静默续发）").isNotNull();
                assertThat(trueLeader.countType(MessageType.TIMER_OP))
                        .as("MUST NOT 在新会话上续发该 SCHEDULE").isZero();
                assertThat(trueLeader.countType(MessageType.HELLO))
                        .as("且该路径 MUST NOT 触发重道——真主一次都未被建道").isZero();
            }
        }
    }

    /**
     * **对照**：队列同经历不确定窗，但随后收到一次明确零生效拒绝（缺码形）——
     * 该分支会**触发共享刷新并建道**（故真主必有 HELLO）；即便如此，在途写
     * **仍然不续发**。本条证明"守卫挡住了续发"，而非"无路可换"。
     */
    @Test
    void queueWriteIsBlockedEvenWhenRerouteIsAvailable() throws Exception {
        try (ScriptedServer trueLeader = new ScriptedServer(req -> switch (req.getType()) {
                    case HELLO -> hello(req, 2000L);
                    case QUEUE_OP -> queueResp(req, StatusCode.OK);
                    default -> null;
                })) {
            AtomicInteger putAttempts = new AtomicInteger();
            try (ScriptedServer home = new ScriptedServer(req -> switch (req.getType()) {
                        case HELLO -> hello(req, 1000L);
                        case CLUSTER_VIEW -> view(req, trueLeader.address());
                        case QUEUE_OP -> putAttempts.incrementAndGet() == 1
                                ? null : req.toBuilder().build();
                        default -> null;
                    })) {
                try (OpenLatchClient client = OpenLatchClient.builder()
                        .address(home.address())
                        .requestTimeout(Duration.ofSeconds(1))
                        .defaultWaitTimeout(Duration.ofSeconds(20))
                        .build()) {
                    client.connectAsync().get(5, TimeUnit.SECONDS);
                    OBlockingQueue q = client.newBlockingQueue("repro-q", 4);
                    Thread putter = new Thread(() -> {
                        try {
                            q.put("x");
                        } catch (Exception expected) {
                            // 守卫抛 SESSION_EXPIRED 是预期
                        }
                    });
                    putter.start();
                    putter.join(30_000);
                    assertThat(putter.isAlive()).isFalse();
                }
                assertThat(trueLeader.countType(MessageType.HELLO))
                        .as("对照有效性：零生效拒绝确实触发了刷新，车道已指向真主")
                        .isGreaterThanOrEqualTo(1);
                assertThat(trueLeader.countType(MessageType.QUEUE_OP))
                        .as("对照：换会话的机会存在，守卫仍 MUST NOT 续发").isZero();
            }
        }
    }
}
