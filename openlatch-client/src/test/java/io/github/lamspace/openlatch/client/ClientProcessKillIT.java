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
import org.junit.jupiter.api.Assumptions;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 9.3 杀服务端进程（§10.4）：进程级终止后客户端持锁失锁回调、重启后自动
 * 重连并恢复服务。依赖 M2 可执行 shade jar——未构建时不静默跳过：向
 * stderr 打印显式警告后再按 Assumption 跳过，提示 CI 需先
 * {@code mvn -pl openlatch-server -am package}（或全仓 verify/package）
 * 方可真正执行本用例（变更 phase1-audit-remediation，§11 标准 3 证据闭环）。
 */
class ClientProcessKillIT {

    /** 等待服务器端口就绪 / 回调事件 / 重连恢复的统一时限。 */
    private static final long WAIT_SECONDS = 10;

    /**
     * 启动服务器进程 → 客户端持锁 → 强杀 → 失锁回调 → 重启 → 自动重连恢复。
     */
    @Test
    void serverProcessKillDetectedAndRecovered() throws Exception {
        Path jar = locateServerJar();
        if (jar == null) {
            // 显式警告而非静默跳过：确保 CI 中"§10.4 杀进程用例从未执行"可见。
            System.err.println("[WARN] ClientProcessKillIT SKIPPED: openlatch-server "
                    + "executable shade jar not found; run "
                    + "'mvn -s <settings> -pl openlatch-server -am package' before "
                    + "'mvn verify' to make this §10.4 fault-injection case effective.");
        }
        Assumptions.assumeTrue(jar != null, "openlatch-server shaded jar not built; run 'mvn install' first");

        int port = freePort();
        Process first = startServer(jar, port);
        OpenLatchClient client = null;
        try {
            client = OpenLatchClient.builder()
                    .address("127.0.0.1:" + port)
                    .reconnectInitialBackoff(Duration.ofMillis(100))
                    .reconnectMaxBackoff(Duration.ofMillis(500))
                    .build();
            BlockingQueue<String> lostKeys = new LinkedBlockingQueue<>();
            client.addLockLostListener((key, cause) -> lostKeys.offer(key));
            client.connectAsync().get(WAIT_SECONDS, TimeUnit.SECONDS);

            // 短租约持锁（失锁时刻 ≈ 1s，使杀进程后的失锁裁决快速可观测）
            client.acquireAsync(new AcquireSpec("kill-me", LockType.REENTRANT,
                    Thread.currentThread().threadId(), 1000, 0)).get(3, TimeUnit.SECONDS);

            first.destroyForcibly();
            first.waitFor(10, TimeUnit.SECONDS);

            // 断连 + 租约耗尽：客户端应在失锁时刻触发锁丢失回调
            assertThat(lostKeys.poll(WAIT_SECONDS, TimeUnit.SECONDS)).isEqualTo("kill-me");

            // 重启服务器：客户端自动重连后恢复服务
            Process second = startServer(jar, port);
            try {
                waitFor(client::isActive);
                assertThat(client.newReentrantLock("recovered").tryLock(3, TimeUnit.SECONDS))
                        .isTrue();
                client.newReentrantLock("recovered").unlock();
            } finally {
                second.destroyForcibly();
                second.waitFor(10, TimeUnit.SECONDS);
            }
        } finally {
            if (client != null) {
                client.shutdown();
            }
            if (first.isAlive()) {
                first.destroyForcibly();
            }
        }
    }

    /**
     * 以指定端口启动服务器子进程，并等待端口就绪。
     *
     * @param jar  服务器可执行 jar 路径
     * @param port 监听端口
     * @return 已就绪的进程
     * @throws IOException 配置写入或进程启动失败
     */
    private static Process startServer(Path jar, int port) throws IOException {
        Path config = Files.createTempFile("openlatch-test", ".properties");
        Files.writeString(config, "openlatch.server.port=" + port + "\n");
        Process process = new ProcessBuilder(
                "java", "-Dopenlatch.config=" + config, "-jar", jar.toAbsolutePath().toString())
                .redirectErrorStream(true)
                .start();
        waitForPort(port, WAIT_SECONDS);
        return process;
    }

    /**
     * 轮询等待端口接受连接。
     *
     * @param port        端口
     * @param timeoutSecs 超时（秒）
     */
    private static void waitForPort(int port, long timeoutSecs) {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(timeoutSecs);
        while (System.currentTimeMillis() < deadline) {
            try (Socket ignored = new Socket()) {
                ignored.connect(new InetSocketAddress("127.0.0.1", port), 200);
                return;
            } catch (IOException e) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }
        throw new AssertionError("server did not start on port " + port + " within " + timeoutSecs + "s");
    }

    /**
     * 轮询等待条件满足。
     *
     * @param condition 条件
     */
    private static void waitFor(java.util.function.BooleanSupplier condition) {
        long deadline = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(WAIT_SECONDS);
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new AssertionError("condition not met within " + WAIT_SECONDS + "s");
    }

    /**
     * 定位服务器可执行 uber-jar（shade {@code -executable} 分类器）：
     * 依次尝试 reactor 布局下的候选路径。
     *
     * @return jar 路径；未找到返回 {@code null}
     */
    private static Path locateServerJar() {
        String[] candidates = {
                "../openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar",
                "openlatch-server/target/openlatch-server-1.0-SNAPSHOT-executable.jar",
        };
        for (String candidate : candidates) {
            Path path = Path.of(candidate);
            if (Files.exists(path)) {
                return path;
            }
        }
        return null;
    }

    /**
     * 取一个当前空闲的端口（取后立即释放）。
     *
     * @return 空闲端口
     * @throws IOException 套接字操作失败
     */
    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
