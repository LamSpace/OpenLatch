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

import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.context.ApplicationContext;

import io.github.lamspace.openlatch.client.AcquireSpec;
import io.github.lamspace.openlatch.client.LockGrant;
import io.github.lamspace.openlatch.client.LockType;
import io.github.lamspace.openlatch.client.OpenLatchClient;
import io.github.lamspace.openlatch.server.OpenLatchServer;
import io.github.lamspace.openlatch.server.ServerConfig;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * enabled=false 的端到端用例（4.5）：真实服务器上切面不注册，
 * 带注解方法按无注解原样执行，客户端 Bean 仍可用（design D4）。
 */
@SpringBootTest(classes = StarterITApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "openlatch.enabled=false")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OpenLatchStarterDisabledIT {

    /** 临时端口内嵌服务器。 */
    private static final OpenLatchServer SERVER = startServer();

    @Autowired
    private OpenLatchClient client;
    @Autowired
    private AnnotatedService service;
    @Autowired
    private ApplicationContext context;

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
     * 注入实际端口。
     *
     * @param registry 动态属性注册器
     */
    @DynamicPropertySource
    static void openlatchProperties(DynamicPropertyRegistry registry) {
        registry.add("openlatch.server-port", SERVER::port);
    }

    /**
     * 连接就绪门闩。
     */
    @BeforeAll
    void awaitConnection() throws Exception {
        client.connectAsync().get(10, TimeUnit.SECONDS);
    }

    /**
     * 关停服务器。
     */
    @AfterAll
    void tearDown() {
        SERVER.stop();
    }

    /**
     * 切面 Bean 不存在（enabled=false），客户端 Bean 仍在。
     */
    @Test
    void aspectNotRegisteredClientRemains() {
        assertThat(context.getBeanNamesForType(OpenLatchAspect.class)).isEmpty();
        assertThat(context.getBean(OpenLatchClient.class)).isNotNull();
    }

    /**
     * 锁被外部持有时注解方法仍直接执行（不发生锁获取）。
     */
    @Test
    void annotatedMethodRunsWhileLockExternallyHeld() throws Exception {
        LockGrant held = client.acquireAsync(
                new AcquireSpec("held", LockType.REENTRANT, -888L, 5_000L, -1))
                .get(10, TimeUnit.SECONDS);

        assertThat(service.whileExternalHeld()).isEqualTo("ran");

        client.releaseAsync("held", held.leaseToken(), -888L).get(10, TimeUnit.SECONDS);
    }
}
