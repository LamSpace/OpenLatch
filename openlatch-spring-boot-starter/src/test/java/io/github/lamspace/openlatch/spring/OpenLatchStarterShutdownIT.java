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

package io.github.lamspace.openlatch.spring;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import io.github.lamspace.openlatch.client.AcquireSpec;
import io.github.lamspace.openlatch.client.LockGrant;
import io.github.lamspace.openlatch.client.LockType;
import io.github.lamspace.openlatch.client.OpenLatchClient;
import io.github.lamspace.openlatch.server.OpenLatchServer;
import io.github.lamspace.openlatch.server.ServerConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * spec 场景"关停后锁即时可竞争"：上下文关闭触发 Bean destroy 回调
 * （{@code shutdown()} 尽力释放持锁），另一客户端应在远小于租约的窗口内
 * 取得该锁（服务端默认租约 30s，2s 窗口即可区分"即时释放"与"租约到期"）。
 *
 * <p>上下文生命周期交由 {@link ApplicationContextRunner} 自管
 * （run 块结束即关闭并执行 destroy），不使用共享缓存的测试上下文。
 */
class OpenLatchStarterShutdownIT {

    /** 临时端口内嵌服务器。 */
    private static final OpenLatchServer SERVER = startServer();

    /** 锁键。 */
    private static final String KEY = "shutdown:held";

    /**
     * 启动端口 0 的内嵌服务器。
     *
     * @return 已启动服务器
     */
    private static OpenLatchServer startServer() {
        ServerConfig d = ServerConfig.defaults();
        OpenLatchServer server = new OpenLatchServer(new ServerConfig(0,
                d.workerThreads(), d.idleTimeoutMs(), d.defaultLeaseMs(), d.minLeaseMs(),
                d.maxLeaseMs(), d.leaseTickIntervalMs(), d.headReplyTimeoutMs(),
                d.maxKeyLength(), d.maxQueueDepthPerKey(), d.maxInflightPerConnection()));
        server.start();
        return server;
    }

    /**
     * 关停服务器。
     */
    @AfterAll
    static void tearDown() {
        SERVER.stop();
    }

    /**
     * runner 上下文内持锁 → 上下文关闭（destroy 释放）→ 观察者 2s 内获取成功。
     */
    @Test
    void contextCloseReleasesHeldLocks() throws Exception {
        new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(
                        OpenLatchAutoConfiguration.class,
                        OpenLatchAspectConfiguration.class))
                .withPropertyValues("openlatch.server-port=" + SERVER.port())
                .run(context -> {
                    OpenLatchClient client = context.getBean(OpenLatchClient.class);
                    client.connectAsync().get(10, TimeUnit.SECONDS);
                    LockGrant grant = client.acquireAsync(new AcquireSpec(
                            KEY, LockType.REENTRANT, -555L, 0, -1))
                            .get(10, TimeUnit.SECONDS);
                    assertThat(grant.leaseToken()).isNotZero();
                });
        // run 块结束：上下文已关闭，destroy 回调应已释放 KEY

        try (OpenLatchClient observer = OpenLatchClient.builder()
                .address("127.0.0.1:" + SERVER.port())
                .requestTimeout(Duration.ofSeconds(1))
                .build()) {
            observer.connectAsync().get(10, TimeUnit.SECONDS);
            LockGrant next = observer.acquireAsync(
                    new AcquireSpec(KEY, LockType.REENTRANT, -556L, 0, -1))
                    .get(2, TimeUnit.SECONDS);
            assertThat(next.leaseToken()).isNotZero();
            observer.releaseAsync(KEY, next.leaseToken(), -556L).get(10, TimeUnit.SECONDS);
        }
    }
}
