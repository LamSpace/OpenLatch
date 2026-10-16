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
import io.github.lamspace.openlatch.protocol.AtomicOpResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 换主锚判据的**负向控制**：证明"把判据放宽到契约面"没有变成"删掉判据"。
 *
 * <p>三层：
 * <ol>
 *   <li><b>非法态必拒</b>——双加后的值、被写成第三方载荷、换主后漂移/丢失，
 *       逐一喂给 {@link LeaderKillLedger} 的纯判定，必须 {@link AssertionError}；</li>
 *   <li><b>合法态必过</b>——"未生效"与"已生效一次"两侧都不得误判（挡过严）；</li>
 *   <li><b>端到端</b>——脚本化桩对 {@code ATOMIC_OP} 回一个"双加过的值"
 *       （服务端错值注入），断言 {@code advanceScalar} 必拒：判据不只是纸面规则，
 *       而是真的挂在读数上。</li>
 * </ol>
 */
class LeaderKillLedgerTest {

    /** 等待上限（毫秒）。 */
    private static final long WAIT_MS = 5_000L;

    /** 双加后的值（前置 0、增量 5 ⇒ 合法态为 {0, 5}，10 即静默重复生效）。 */
    private static final long DOUBLE_APPLIED = 10L;

    /** 非法态：静默重复生效（双加）。 */
    @Test
    void doubleAppliedValueIsRejected() {
        assertThatThrownBy(() -> LeaderKillLedger.assertAdvanceResolved("双加", 0, 5, DOUBLE_APPLIED))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("静默重复生效");
    }

    /** 非法态：引用载荷被写成第三方值。 */
    @Test
    void thirdPartyPayloadIsRejected() {
        assertThatThrownBy(() -> LeaderKillLedger.assertAdvanceResolvedRef("第三方", "v1", "v2", "vX"))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("第三方值");
    }

    /** 非法态：换主后已提交态漂移（值丢、值多算、版本漂移三项）。 */
    @Test
    void driftAndLossAfterFailoverAreRejected() {
        assertThatThrownBy(() -> LeaderKillLedger.assertDurable("丢值", 0, 0, 6, 2))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("漂移");
        assertThatThrownBy(() -> LeaderKillLedger.assertDurable("双加", 12, 3, 6, 2))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("漂移");
        assertThatThrownBy(() -> LeaderKillLedger.assertDurable("版本漂", 6, 3, 6, 2))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("漂移");
    }

    /** 合法态两侧都不得误判（挡"过严"一侧）。 */
    @Test
    void legalStatesPassBothSides() {
        assertThatCode(() -> {
            LeaderKillLedger.assertAdvanceResolved("未生效", 0, 5, 0);
            LeaderKillLedger.assertAdvanceResolved("已生效一次", 0, 5, 5);
            LeaderKillLedger.assertAdvanceResolvedRef("未生效", "v1", "v2", "v1");
            LeaderKillLedger.assertAdvanceResolvedRef("已生效一次", "v1", "v2", "v2");
            LeaderKillLedger.assertDurable("原样存续", 6, 2, 6, 2);
        }).doesNotThrowAnyException();
    }

    /**
     * 端到端负向控制：服务端回"双加过的值"时，{@code advanceScalar} 必拒。
     *
     * <p>脚本化桩对每次 {@code ATOMIC_OP} 回 {@code value=10}（前置 0、增量 5 下
     * 合法态只有 {0, 5}），断言推进判据以 {@link AssertionError} 收场——
     * 若判据是空网，这里会静默把 10 当成"推进成功"。
     *
     * @throws Exception 等待或断言失败
     */
    @Test
    void injectedDoubleAppliedServerValueIsRejected() throws Exception {
        try (ScriptedServer server = new ScriptedServer(env -> {
            if (env.getType() == MessageType.HELLO) {
                return Envelope.newBuilder()
                        .setProtocolVersion(env.getProtocolVersion())
                        .setType(MessageType.HELLO)
                        .setRequestId(env.getRequestId())
                        .setHelloResponse(HelloResponse.newBuilder()
                                .setStatus(StatusCode.OK)
                                .setSessionId(7_000L))
                        .build();
            }
            if (env.getType() == MessageType.ATOMIC_OP) {
                // 错值注入：回一个"已被加过两次"的值
                return Envelope.newBuilder()
                        .setProtocolVersion(env.getProtocolVersion())
                        .setType(MessageType.ATOMIC_OP)
                        .setRequestId(env.getRequestId())
                        .setAtomicOpResponse(AtomicOpResponse.newBuilder()
                                .setStatus(StatusCode.OK)
                                .setApplied(true)
                                .setValue(DOUBLE_APPLIED)
                                .setVersion(2))
                        .build();
            }
            return null;
        })) {
            OpenLatchClient client = OpenLatchClient.builder()
                    .address(server.address())
                    .reconnectInitialBackoff(Duration.ofMillis(50))
                    .reconnectMaxBackoff(Duration.ofMillis(200))
                    .build();
            try {
                client.connectAsync().get(WAIT_MS, TimeUnit.MILLISECONDS);
                OAtomicLong a = client.newAtomicLong("ledger-probe");
                assertThatThrownBy(() -> LeaderKillLedger.advanceScalar("注入复核", a, 0, 5))
                        .isInstanceOf(AssertionError.class)
                        .hasMessageContaining("静默重复生效");
            } finally {
                client.shutdown();
            }
        }
    }
}
