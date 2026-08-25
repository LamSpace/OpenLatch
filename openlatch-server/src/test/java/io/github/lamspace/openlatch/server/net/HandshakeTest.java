package io.github.lamspace.openlatch.server.net;

import io.github.lamspace.openlatch.core.CoreEngine;
import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.core.SystemClock;
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.HelloRequest;
import io.github.lamspace.openlatch.protocol.HelloResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.ServerConfig;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §3.2.1 握手规则（design.md D8）：握手前拒绝、版本/令牌校验、重复 HELLO、requestId 回显。
 */
class HandshakeTest {

    private EmbeddedChannel ch;
    private ServerSessionRegistry registry;
    private ServerConfig config;

    @BeforeEach
    void setUp() {
        config = ServerConfig.defaults();
        registry = new ServerSessionRegistry();
        CoreEngine core = new CoreEngine(new CoreConfig(), new SystemClock(), (s, r, k) -> { });
        ch = new EmbeddedChannel(
                new ServerSessionHandler(core, config, registry, new RequestDispatcher(core)));
        ch.pipeline().fireChannelActive();
    }

    private static Envelope hello(long requestId, int version, String authToken) {
        return Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.HELLO)
                .setRequestId(requestId)
                .setHelloRequest(HelloRequest.newBuilder()
                        .setClientProtocolVersion(version)
                        .setAuthToken(authToken))
                .build();
    }

    private static Envelope acquire(long requestId) {
        return Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(requestId)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey("k"))
                .build();
    }

    private Envelope readOutboundEnvelope() {
        Object out = ch.readOutbound();
        assertThat(out).isInstanceOf(Envelope.class);
        return (Envelope) out;
    }

    @Test
    void valid_hello_establishes_session_and_echoes_request_id() {
        ch.writeInbound(hello(42, 1, ""));

        Envelope resp = readOutboundEnvelope();
        assertThat(resp.getType()).isEqualTo(MessageType.HELLO);
        assertThat(resp.getRequestId()).isEqualTo(42);
        HelloResponse hr = resp.getHelloResponse();
        assertThat(hr.getStatus()).isEqualTo(StatusCode.OK);
        assertThat(hr.getSessionId()).isPositive();
        assertThat(hr.getServerProtocolVersion()).isEqualTo(1);
        assertThat(hr.getDefaultLeaseMs()).isEqualTo(config.defaultLeaseMs());
        assertThat(ch.isOpen()).isTrue();
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void business_request_before_handshake_rejected_without_disconnect() {
        ch.writeInbound(acquire(7));

        Envelope resp = readOutboundEnvelope();
        assertThat(resp.getType()).isEqualTo(MessageType.LOCK_ACQUIRE);
        assertThat(resp.getRequestId()).isEqualTo(7);
        assertThat(resp.getAcquireResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
        assertThat(registry.size()).isZero();
    }

    @Test
    void ping_before_handshake_rejected_without_disconnect() {
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(1).setType(MessageType.PING).setRequestId(3).build());

        Envelope resp = readOutboundEnvelope();
        assertThat(resp.getRequestId()).isEqualTo(3);
        assertThat(ch.isOpen()).isTrue();
    }

    @Test
    void wrong_protocol_version_rejected_and_disconnected() {
        ch.writeInbound(hello(1, 2, ""));

        Envelope resp = readOutboundEnvelope();
        assertThat(resp.getHelloResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isFalse();
        assertThat(registry.size()).isZero();
    }

    @Test
    void non_empty_auth_token_rejected_and_disconnected() {
        ch.writeInbound(hello(1, 1, "secret"));

        Envelope resp = readOutboundEnvelope();
        assertThat(resp.getHelloResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isFalse();
    }

    @Test
    void duplicate_hello_rejected_but_session_kept() {
        ch.writeInbound(hello(1, 1, ""));
        Envelope first = readOutboundEnvelope();
        assertThat(first.getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);

        ch.writeInbound(hello(2, 1, ""));
        Envelope second = readOutboundEnvelope();
        assertThat(second.getType()).isEqualTo(MessageType.HELLO);
        assertThat(second.getHelloResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
        assertThat(registry.size()).isEqualTo(1);
    }

    @Test
    void hello_without_payload_rejected_without_disconnect() {
        ch.writeInbound(Envelope.newBuilder()
                .setProtocolVersion(1).setType(MessageType.HELLO).setRequestId(9).build());

        Envelope resp = readOutboundEnvelope();
        assertThat(resp.getHelloResponse().getStatus()).isEqualTo(StatusCode.INVALID_REQUEST);
        assertThat(ch.isOpen()).isTrue();
    }
}
