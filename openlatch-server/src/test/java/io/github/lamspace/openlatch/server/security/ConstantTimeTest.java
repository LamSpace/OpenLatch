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

package io.github.lamspace.openlatch.server.security;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ConstantTime} 语义（常量时间比较）：等值/不等/长度差异不早退、
 * null 一律拒绝。常量时间的结构保证（等长补齐后 {@code MessageDigest.isEqual}）
 * 属代码评审项，不做运行时计时断言。
 */
class ConstantTimeTest {

    @Test
    void equalReturnsTrue() {
        assertThat(ConstantTime.matches("secret-token", "secret-token")).isTrue();
        assertThat(ConstantTime.matches("", "")).isTrue();
    }

    @Test
    void unequalReturnsFalse() {
        assertThat(ConstantTime.matches("token-a", "token-b")).isFalse();
        assertThat(ConstantTime.matches("token-a", "token-a ")).isFalse();
    }

    @Test
    void lengthDifferenceStillCompares() {
        // 等长补齐后比较：长度不等 MUST 返回 false 且不抛异常（无"早退"分支）。
        assertThat(ConstantTime.matches("short", "a-much-longer-config-token")).isFalse();
        assertThat(ConstantTime.matches("a-much-longer-config-token", "short")).isFalse();
    }

    @Test
    void nullNeverMatches() {
        assertThat(ConstantTime.matches(null, "configured")).isFalse();
        assertThat(ConstantTime.matches("presented", null)).isFalse();
        assertThat(ConstantTime.matches(null, null)).isFalse();
    }
}
