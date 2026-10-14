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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link SessionContinuation} 判定矩阵：跨会话续发的许可条件与**单调性**。
 *
 * <p>本测试是队列写车道那套"零生效逃逸"纪律的**判据层**——端到端夹具
 * （`RemoteBlockingQueueScriptedTest` 的不确定窗用例）证明的是同一条规则在真实
 * 循环里的效果，此处证明判定本身逐分支正确，两者互为锚。
 */
class SessionContinuationTest {

    @Test
    void freshOperationWithZeroEffectRejectMayContinue() {
        SessionContinuation c = new SessionContinuation();
        c.onZeroEffectReject();
        assertThat(c.mayContinueOnNewSession())
                .as("首次尝试即被明确判零生效：可换会话续发（既有承诺）")
                .isTrue();
    }

    @Test
    void uncertainAttemptThenRejectMustNotContinue() {
        SessionContinuation c = new SessionContinuation();
        c.onPossiblyApplied();      // 应答丢失：本操作可能已生效
        c.onZeroEffectReject();     // 随后的明确拒绝 MUST NOT 洗白上一条
        assertThat(c.mayContinueOnNewSession())
                .as("曾进入不确定窗后，明确拒绝不得放行跨会话续发")
                .isFalse();
    }

    @Test
    void acceptedAttemptThenRejectMustNotContinue() {
        SessionContinuation c = new SessionContinuation();
        c.onAcceptedAttempt();      // 已受理（QUEUED/DENIED）：已进账簿
        c.onZeroEffectReject();
        assertThat(c.mayContinueOnNewSession())
                .as("已受理过的操作，后续拒绝不得放行跨会话续发")
                .isFalse();
    }

    @Test
    void uncertaintyIsMonotonicAcrossRepeatedRejects() {
        SessionContinuation c = new SessionContinuation();
        c.onPossiblyApplied();
        c.onZeroEffectReject();
        c.onZeroEffectReject();
        c.onZeroEffectReject();
        assertThat(c.mayContinueOnNewSession())
                .as("单调位一经置位不因反复拒绝而回退")
                .isFalse();
    }

    @Test
    void continuationPermitIsSingleUse() {
        SessionContinuation c = new SessionContinuation();
        c.onZeroEffectReject();
        assertThat(c.mayContinueOnNewSession()).isTrue();
        c.consumeContinuation();
        assertThat(c.mayContinueOnNewSession())
                .as("许可为一次性：续发一次后须再次获得明确零生效拒绝")
                .isFalse();
        c.onZeroEffectReject();
        assertThat(c.mayContinueOnNewSession()).isTrue();
    }

    @Test
    void acceptanceAfterZeroEffectRejectKeepsContinuationClosed() {
        SessionContinuation c = new SessionContinuation();
        c.onZeroEffectReject();
        c.onAcceptedAttempt();      // 该次已受理 => 本操作已生效
        assertThat(c.mayContinueOnNewSession())
                .as("受理事实覆盖此前许可，且此后不得再换会话续发")
                .isFalse();
    }
}
