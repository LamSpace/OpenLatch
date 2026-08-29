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

import org.aspectj.lang.annotation.Aspect;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;

import io.github.lamspace.openlatch.client.OpenLatchClient;

/**
 * {@link OpenLatchAspect} 切面的条件装配（详设 §8.1，design D4/D7）。
 *
 * <p><b>条件矩阵</b>：类级 {@code @ConditionalOnClass(Aspect)} 保证缺少
 * AspectJ 依赖时整类跳过（自动装配经 ASM 元数据判定，不触发类加载）；
 * {@code openlatch.enabled=false} 时不注册切面 Bean——注解方法按无注解
 * 原样执行，而 {@link OpenLatchClient} Bean 照常装配（开关只关注解面，
 * design D4）；{@code @ConditionalOnBean(OpenLatchClient)} 覆盖"客户端
 * Bean 缺失"的病态场景（如自动装配被 exclude）。
 *
 * <p><b>装配次序</b>：{@code after = OpenLatchAutoConfiguration} 确保切面
 * 的条件判定发生在客户端 Bean 定义注册之后（用户自定义客户端 Bean 的
 * 场景本就在自动装配之前完成注册）。代理基建由 Boot 的
 * {@code AopAutoConfiguration} 提供，本类不重复声明
 * {@code @EnableAspectJAutoProxy}。
 */
@AutoConfiguration(after = OpenLatchAutoConfiguration.class)
@ConditionalOnClass(Aspect.class)
class OpenLatchAspectConfiguration {

    /**
     * 公开无参构造：Spring 实例化配置类所需，无额外装配语义。
     */
    OpenLatchAspectConfiguration() {
    }

    /**
     * 注册锁切面。
     *
     * @param client 上下文中唯一的客户端 Bean（自动装配或用户定义）
     * @return 锁切面实例
     */
    @Bean
    @ConditionalOnBean(OpenLatchClient.class)
    @ConditionalOnProperty(prefix = "openlatch", name = "enabled", matchIfMissing = true)
    public OpenLatchAspect openLatchAspect(OpenLatchClient client) {
        return new OpenLatchAspect(client);
    }
}
