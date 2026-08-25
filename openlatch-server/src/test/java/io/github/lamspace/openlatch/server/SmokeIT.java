package io.github.lamspace.openlatch.server;

import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LeaseRenewRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 冒烟（规格"可执行交付形态"，设计说明书 §13.2 P1-17）：
 * {@code java -jar} 独立启动后完整执行 HELLO → ACQUIRE → LEASE_RENEW → RELEASE。
 * 由 failsafe 在 verify 阶段（shade 打包之后）运行。
 */
class SmokeIT {

    @Test
    @Timeout(60)
    void executable_jar_serves_full_protocol_sequence() throws Exception {
        Path jar = findShadedJar();
        int port = findFreePort();
        Path configFile = Files.createTempFile("openlatch-smoke", ".properties");
        Files.writeString(configFile, "openlatch.server.port=" + port + "\n");
        Path logFile = Files.createTempFile("openlatch-smoke", ".log");

        Process server = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-Dopenlatch.config=" + configFile,
                "-jar", jar.toString())
                .redirectOutput(logFile.toFile())
                .redirectError(logFile.toFile())
                .start();
        try {
            awaitListening(port, 15_000);

            try (TestProtocolClient client = new TestProtocolClient()) {
                client.connect("127.0.0.1", port);
                assertThat(client.hello()).isPositive();

                // ACQUIRE：授予携带凭证与生效租约。
                Envelope granted = client.sendAndAwait(acquire(client.nextRequestId(), "smoke"));
                assertThat(granted.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
                long token = granted.getAcquireResponse().getLeaseToken();
                assertThat(token).isPositive();
                assertThat(granted.getAcquireResponse().getGrantedLeaseMs()).isPositive();

                // LEASE_RENEW：凭证有效，返回新到期时刻。
                Envelope renewed = client.sendAndAwait(Envelope.newBuilder()
                        .setProtocolVersion(OpenLatchServer.PROTOCOL_VERSION)
                        .setType(MessageType.LEASE_RENEW)
                        .setRequestId(client.nextRequestId())
                        .setLeaseRenewRequest(LeaseRenewRequest.newBuilder()
                                .setKey("smoke").setLeaseToken(token).setLeaseMs(30_000))
                        .build());
                assertThat(renewed.getLeaseRenewResponse().getStatus()).isEqualTo(StatusCode.OK);
                assertThat(renewed.getLeaseRenewResponse().getLeaseExpiresAtMs()).isPositive();

                // RELEASE：完全释放。
                Envelope released = client.sendAndAwait(Envelope.newBuilder()
                        .setProtocolVersion(OpenLatchServer.PROTOCOL_VERSION)
                        .setType(MessageType.LOCK_RELEASE)
                        .setRequestId(client.nextRequestId())
                        .setReleaseRequest(ReleaseRequest.newBuilder()
                                .setKey("smoke").setLeaseToken(token).setThreadId(1))
                        .build());
                assertThat(released.getReleaseResponse().getStatus()).isEqualTo(StatusCode.OK);
                assertThat(released.getReleaseResponse().getFullyReleased()).isTrue();
            }
        } finally {
            server.destroy();
            assertThat(server.waitFor(10, TimeUnit.SECONDS))
                    .as("server process exits within bounded time")
                    .isTrue();
        }

        String log = Files.readString(logFile);
        assertThat(log).contains("OpenLatch server started")
                .contains("port=" + port)
                .contains("protocolVersion=" + OpenLatchServer.PROTOCOL_VERSION);
    }

    private static Envelope acquire(long requestId, String key) {
        return Envelope.newBuilder()
                .setProtocolVersion(OpenLatchServer.PROTOCOL_VERSION)
                .setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(requestId)
                .setAcquireRequest(AcquireRequest.newBuilder()
                        .setKey(key)
                        .setLockType(LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1)
                        .setLeaseMs(0)
                        .setWaitMs(0))
                .build();
    }

    private static Path findShadedJar() throws IOException {
        Path target = Path.of("target");
        try (var stream = Files.list(target)) {
            return stream.filter(p -> {
                        String name = p.getFileName().toString();
                        return name.startsWith("openlatch-server-") && name.endsWith(".jar");
                    })
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("shaded jar not found in " + target.toAbsolutePath()));
        }
    }

    private static int findFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void awaitListening(int port, long timeoutMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            try (Socket probe = new Socket()) {
                probe.connect(new InetSocketAddress("127.0.0.1", port), 500);
                return;
            } catch (IOException e) {
                Thread.sleep(100);
            }
        }
        throw new AssertionError("server did not start listening on port " + port
                + " within " + timeoutMs + "ms");
    }
}
