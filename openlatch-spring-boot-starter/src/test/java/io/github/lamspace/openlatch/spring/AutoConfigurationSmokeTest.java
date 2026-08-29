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

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * P1-27 兼容性冒烟：验证 Java 25 × Spring Boot 4.0.3 的自动装配链路。
 *
 * <p>覆盖三段风险路径：{@code AutoConfiguration.imports} 注册文件被解析、
 * 本模块自动装配类被选中并实例化、{@code @Configuration} 类经 CGLIB 增强
 * （Framework 7 字节码栈对 Java 25 的支持点）。定案结论回写详设 §8.4。
 */
class AutoConfigurationSmokeTest {

    /**
     * 冒烟应用：仅开启自动装配，驱动 imports 注册文件与 CGLIB 代理链。
     * 刻意使用默认 full 模式（proxyBeanMethods = true），强制对 Java 25
     * 字节码的 @Configuration 类做 CGLIB 增强——这正是 Framework 7 × Java 25
     * 的兼容性风险点。
     */
    @Configuration
    @EnableAutoConfiguration
    static class SmokeApp {
    }

    /**
     * 上下文刷新成功且 OpenLatch 自动装配类被注册为 Bean，即证明装配链路在
     * Java 25 × Boot 4.0.3 上可用。
     */
    @Test
    void openLatchAutoConfigurationLoadsOnJava25Boot4() {
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(SmokeApp.class)) {
            assertThat(context.getBean(OpenLatchAutoConfiguration.class)).isNotNull();
        }
    }
}
