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

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * {@code openlatch.*} 配置属性（详设 §8.2 全表）。
 *
 * <p><b>职责</b>：绑定并承载 starter 的全部可配置项，缺省值即详设 §8.2
 * 默认列；由 {@link OpenLatchAutoConfiguration} 映射到
 * {@link io.github.lamspace.openlatch.client.OpenLatchClient.Builder}。
 *
 * <p><b>契约边界</b>：仅承载配置，不做校验——时长为正、退避上限不小于初始值等
 * 约束由客户端 Builder 单一事实源校验（违例在 Bean 创建期以
 * {@link IllegalArgumentException} 失败，应用上下文启动中止）。
 * {@code enabled=false} 仅关闭 {@code @OpenLatch} 切面注册，客户端 Bean 照常
 * 装配（design D4）。时长类属性支持标准 Duration 写法（如 {@code 5s}、
 * {@code 100ms}）。
 *
 * @param enabled                 注解与切面总开关，默认 {@code true}；关闭后
 *                                {@code @OpenLatch} 不生效，客户端 Bean 仍装配
 * @param serverHost              服务器主机名或地址，默认 {@code 127.0.0.1}
 * @param serverPort              服务器端口，默认 {@code 9410}
 * @param requestTimeout          单个请求（获取/释放/续租）超时，默认 5s
 * @param defaultWaitTimeout      {@code lock()} 总等待兜底超时，默认 30s
 * @param reconnectInitialBackoff 重连指数退避初始值，默认 200ms
 * @param reconnectMaxBackoff     重连指数退避上限，默认 10s
 */
@ConfigurationProperties(prefix = "openlatch")
public record OpenLatchProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("127.0.0.1") String serverHost,
        @DefaultValue("9410") int serverPort,
        @DefaultValue("5s") Duration requestTimeout,
        @DefaultValue("30s") Duration defaultWaitTimeout,
        @DefaultValue("200ms") Duration reconnectInitialBackoff,
        @DefaultValue("10s") Duration reconnectMaxBackoff) {
}
