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
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
 *
 * <p><b>客户端指标（Phase 3 T2）</b>：上下文存在 {@code MeterRegistry} Bean
 * 时经内嵌守卫配置把注册表注入客户端 Builder（启用可选指标），不存在时
 * 静默跳过、Micrometer 缺席亦不影响本类加载——细节见
 * {@link MetricsInjectionConfiguration}。
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
     * {@code address}；{@code openlatch.*} 表内四类时长直传，
     * {@code connectTimeout}/{@code workerThreads} 不在 §8.2 属性表内，
     * 取客户端 Builder 默认值。{@code destroyMethod = "shutdown"}
     * 使上下文关闭时客户端先尽力释放本地持有的全部锁（至多一个请求超时），
     * 再停止重连与网络资源。应用已自行定义客户端 Bean 时不创建。
     *
     * @param properties  已绑定的 {@code openlatch.*} 属性
     * @param customizers 构建期定制器（按 {@code @Order} 升序应用；T2 度量
     *                    注册表注入即经此挂接，无实现 Bean 时零操作）
     * @return 已发起首次异步连接的客户端实例
     */
    @Bean(destroyMethod = "shutdown")
    @ConditionalOnMissingBean
    public OpenLatchClient openLatchClient(OpenLatchProperties properties,
            ObjectProvider<OpenLatchClientBuilderCustomizer> customizers) {
        OpenLatchClient.Builder builder = OpenLatchClient.builder()
                .address(properties.serverHost() + ":" + properties.serverPort())
                .requestTimeout(properties.requestTimeout())
                .defaultWaitTimeout(properties.defaultWaitTimeout())
                .reconnectInitialBackoff(properties.reconnectInitialBackoff())
                .reconnectMaxBackoff(properties.reconnectMaxBackoff())
                .tlsEnabled(properties.tlsEnabled());
        // TLS/认证属性（Phase 3 T4，spec spring-boot-starter"配置属性绑定与默认值"）：
        // 仅在显式配置时透传，未配置（null）保持客户端默认（TLS 关 / 无令牌）。
        if (isNotBlank(properties.tlsTrustStore())) {
            builder.tlsTrustStore(properties.tlsTrustStore());
        }
        if (isNotBlank(properties.tlsClientCert())) {
            builder.tlsClientCert(properties.tlsClientCert());
        }
        if (isNotBlank(properties.tlsClientKey())) {
            builder.tlsClientKey(properties.tlsClientKey());
        }
        if (isNotBlank(properties.authToken())) {
            builder.authToken(properties.authToken());
        }
        customizers.orderedStream().forEach(c -> c.customize(builder));
        return builder.build();
    }

    /**
     * 字符串是否非空白（透传判据）。
     *
     * @param value 值
     * @return 非空白返回 true
     */
    private static boolean isNotBlank(String value) {
        return value != null && !value.isBlank();
    }

    /**
     * 度量注册表注入分支（T2，spec"度量注册表自动注入"）：类级
     * {@code @ConditionalOnClass} 守卫使 Micrometer 缺席时本分支整体
     * 不加载（主配置类不受牵连）；存在时经 {@link ObjectProvider} 延迟
     * 解算——上下文有 {@code MeterRegistry} Bean 即注入客户端 Builder
     * （启用客户端可选指标），一个也没有则静默跳过（默认关闭，spec
     * "无注册表不受扰"）。多注册表取主 Bean（{@code getIfAvailable}
     * 语义：唯一或 {@code @Primary}）。
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(io.micrometer.core.instrument.MeterRegistry.class)
    static class MetricsInjectionConfiguration {

        /**
         * 公开无参构造：Spring 实例化配置类所需，无额外装配语义。
         */
        MetricsInjectionConfiguration() {
        }

        /**
         * 注册表定制器：把宿主度量注册表接到客户端 Builder 上。
         *
         * @param registries 宿主注册表的延迟提供者
         * @return 定制器
         */
        @Bean
        @ConditionalOnMissingBean
        OpenLatchClientBuilderCustomizer meterRegistryCustomizer(
                ObjectProvider<io.micrometer.core.instrument.MeterRegistry> registries) {
            return builder -> {
                io.micrometer.core.instrument.MeterRegistry registry = registries.getIfAvailable();
                if (registry != null) {
                    builder.meterRegistry(registry);
                }
            };
        }
    }
}
