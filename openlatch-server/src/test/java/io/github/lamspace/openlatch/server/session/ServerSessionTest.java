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

package io.github.lamspace.openlatch.server.session;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * inflight 计数逻辑（design.md D4）：同步分发模型下限额实际不可触发，
 * 计数语义仍经直接调用锁定。
 */
class ServerSessionTest {

    @Test
    void inflight_counts_requests_within_limit() {
        ServerSession session = new ServerSession(null);

        assertThat(session.tryBeginRequest(2)).isTrue();
        assertThat(session.inflight()).isEqualTo(1);
        assertThat(session.tryBeginRequest(2)).isTrue();
        assertThat(session.inflight()).isEqualTo(2);

        session.endRequest();
        assertThat(session.inflight()).isEqualTo(1);
        assertThat(session.tryBeginRequest(2)).isTrue();
        assertThat(session.inflight()).isEqualTo(2);
    }

    @Test
    void request_over_limit_is_rejected_without_counting() {
        ServerSession session = new ServerSession(null);

        assertThat(session.tryBeginRequest(1)).isTrue();
        assertThat(session.tryBeginRequest(1)).isFalse();
        assertThat(session.inflight()).isEqualTo(1);

        session.endRequest();
        assertThat(session.tryBeginRequest(1)).isTrue();
    }

    @Test
    void mark_closed_is_idempotent() {
        ServerSession session = new ServerSession(null);

        assertThat(session.markClosed()).isTrue();
        assertThat(session.markClosed()).isFalse();
    }
}
