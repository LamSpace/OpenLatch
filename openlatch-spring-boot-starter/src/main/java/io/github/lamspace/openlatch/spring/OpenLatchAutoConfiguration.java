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
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * OpenLatch 自动装配入口（详设 §8.1）。
 *
 * <p><b>职责</b>：经 {@code META-INF/spring/…AutoConfiguration.imports} 注册；
 * 绑定 {@link OpenLatchProperties} 并装配单例 {@link OpenLatchClient} Bean，
 * 应用关闭时经 Bean destroy 回调执行优雅关停（尽力释放持锁后断连）。
 * {@code @OpenLatch} 锁切面由 {@link OpenLatchAspectConfiguration} 条件注册，
 * 受 {@code openlatch.enabled} 开关控制。
 *
 * <p><b>线程模型</b>：Bean 工厂方法在上下文刷新线程执行一次；客户端自身的
 * 首次连接为异步发起并自动重连——服务器暂不可达不阻塞、不失败上下文启动，
 * 应用在后台退避重连中照常运行。
 *
 * <p><b>契约边界</b>：应用自行定义 {@link OpenLatchClient} Bean 时本装配让位
 * （用户实例唯一），这是"仅加依赖与注解"与"高级用户自定义"两条路径的
 * 会合点；参数校验（时长为正、退避序）委托客户端 Builder 单一事实源完成。
 */
@AutoConfiguration
@EnableConfigurationProperties(OpenLatchProperties.class)
public class OpenLatchAutoConfiguration {

    /**
     * 公开无参构造：Spring 实例化配置类所需，无额外装配语义。
     */
    public OpenLatchAutoConfiguration() {
    }

    /**
     * 装配共享的 {@link OpenLatchClient} 单例。
     *
     * <p>属性映射：{@code server-host}/{@code server-port} 拼为 Builder 的
     * {@code address}；四类超时/退避直传。{@code destroyMethod = "shutdown"}
     * 使上下文关闭时客户端先尽力释放本地持有的全部锁（至多一个请求超时），
     * 再停止重连与网络资源。应用已自行定义客户端 Bean 时不创建。
     *
     * @param properties 已绑定的 {@code openlatch.*} 属性
     * @return 已发起首次异步连接的客户端实例
     */
    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    public OpenLatchClient openLatchClient(OpenLatchProperties properties) {
        return OpenLatchClient.builder()
                .address(properties.serverHost() + ":" + properties.serverPort())
                .requestTimeout(properties.requestTimeout())
                .defaultWaitTimeout(properties.defaultWaitTimeout())
                .reconnectInitialBackoff(properties.reconnectInitialBackoff())
                .reconnectMaxBackoff(properties.reconnectMaxBackoff())
                .build();
    }
}
