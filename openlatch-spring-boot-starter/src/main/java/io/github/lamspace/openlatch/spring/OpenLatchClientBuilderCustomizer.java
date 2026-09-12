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

/**
 * {@link OpenLatchClient.Builder} 的装配期扩展点。
 *
 * <p><b>存在动机</b>：把"宿主 classpath 上才有的类型"（如
 * {@code MeterRegistry}）与自动配置主类的加载解耦——主配置类只依赖本
 * 接口（永不因 Micrometer 缺席而 {@code NoClassDefFoundError}），可选
 * 能力由带 {@code @ConditionalOnClass} 守卫的内嵌配置注册实现 Bean，
 * 装配时按序应用（Spring Boot 各 starter 的 {@code *BuilderCustomizer}
 * 同纪律）。
 *
 * <p><b>契约</b>：{@link #customize} 在 {@code build()} 之前、属性绑定
 * 之后被调用；实现 MUST NOT 缓存或跨上下文复用 builder。多个实现按
 * {@code @Order} 升序应用，后应用者覆盖同名设置。
 */
@FunctionalInterface
public interface OpenLatchClientBuilderCustomizer {

    /**
     * 定制即将构建的客户端构建器。
     *
     * @param builder 已应用 {@code openlatch.*} 属性的构建器
     */
    void customize(OpenLatchClient.Builder builder);
}
