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

import io.github.lamspace.openlatch.client.OpenLatchClient;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * P1-28 条件装配测试（design D9）：不起服务器，验证客户端 Bean 的
 * 存在性、默认值绑定、属性覆盖与"用户 Bean 让位"，以及服务器不可达时
 * 上下文照常启动（异步连接契约）。
 */
class OpenLatchAutoConfigurationTest {

    /** 每次测试独立构建的 runner，装配本模块全部自动配置。 */
    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    OpenLatchAutoConfiguration.class,
                    OpenLatchAspectConfiguration.class));

    /**
     * 零配置：Bean 存在且 §8.2 默认值逐项落到 client.config()。
     */
    @Test
    void defaultsBindPerSpec() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(OpenLatchClient.class);
            var config = context.getBean(OpenLatchClient.class).config();
            assertThat(config.host()).isEqualTo("127.0.0.1");
            assertThat(config.port()).isEqualTo(9410);
            assertThat(config.requestTimeout()).isEqualTo(Duration.ofSeconds(5));
            assertThat(config.defaultWaitTimeout()).isEqualTo(Duration.ofSeconds(30));
            assertThat(config.reconnectInitialBackoff()).isEqualTo(Duration.ofMillis(200));
            assertThat(config.reconnectMaxBackoff()).isEqualTo(Duration.ofSeconds(10));
        });
    }

    /**
     * 属性覆盖：Duration 简写语法绑定并透传到客户端配置。
     */
    @Test
    void propertyOverridesReachClientConfig() {
        runner.withPropertyValues(
                "openlatch.server-host=10.20.30.40",
                "openlatch.server-port=1234",
                "openlatch.request-timeout=2s",
                "openlatch.default-wait-timeout=1m",
                "openlatch.reconnect-initial-backoff=500ms",
                "openlatch.reconnect-max-backoff=30s")
                .run(context -> {
                    var config = context.getBean(OpenLatchClient.class).config();
                    assertThat(config.host()).isEqualTo("10.20.30.40");
                    assertThat(config.port()).isEqualTo(1234);
                    assertThat(config.requestTimeout()).isEqualTo(Duration.ofSeconds(2));
                    assertThat(config.defaultWaitTimeout()).isEqualTo(Duration.ofMinutes(1));
                    assertThat(config.reconnectInitialBackoff()).isEqualTo(Duration.ofMillis(500));
                    assertThat(config.reconnectMaxBackoff()).isEqualTo(Duration.ofSeconds(30));
                });
    }

    /**
     * 应用自行定义客户端 Bean 时自动装配让位，上下文中唯一实例为用户实例。
     */
    @Test
    void userProvidedClientBeanWins() {
        AtomicReference<OpenLatchClient> custom = new AtomicReference<>();
        runner.withBean(OpenLatchClient.class, () -> {
            OpenLatchClient client = OpenLatchClient.builder()
                    .address("127.0.0.1:1")
                    .build();
            custom.set(client);
            return client;
        }).run(context -> {
            assertThat(context).hasSingleBean(OpenLatchClient.class);
            assertThat(context.getBean(OpenLatchClient.class)).isSameAs(custom.get());
        });
    }

    /**
     * 服务器不可达（端口 1 必然拒绝连接）不阻塞、不失败上下文启动：
     * 首次连接异步发起，失败进入后台退避重连。
     */
    @Test
    void contextStartsWhileServerUnreachable() {
        runner.withPropertyValues("openlatch.server-port=1")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(OpenLatchClient.class);
                });
    }

    /**
     * 默认状态：锁切面 Bean 注册（spec"enabled 开关"默认生效侧）。
     */
    @Test
    void aspectRegisteredByDefault() {
        runner.run(context ->
                assertThat(context).hasSingleBean(OpenLatchAspect.class));
    }

    /**
     * enabled=false：切面不注册，客户端 Bean 照常装配（design D4——
     * 开关只关注解面，编程式路径不受影响）。
     */
    @Test
    void disabledSwitchRemovesAspectKeepsClient() {
        runner.withPropertyValues("openlatch.enabled=false")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(OpenLatchAspect.class);
                    assertThat(context).hasSingleBean(OpenLatchClient.class);
                });
    }
}
