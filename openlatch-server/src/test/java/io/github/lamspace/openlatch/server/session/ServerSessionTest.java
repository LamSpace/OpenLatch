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
