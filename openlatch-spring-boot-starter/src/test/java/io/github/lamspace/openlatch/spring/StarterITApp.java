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

import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;

/**
 * starter 集成测试的 Boot 应用：仅"加依赖"（自动装配经 imports 生效）
 * + 一个被注解服务 Bean——验收标准 4 的最小形态。
 */
@SpringBootConfiguration
@EnableAutoConfiguration
class StarterITApp {

    /**
     * 被注解业务服务。
     *
     * @return 服务实例
     */
    @Bean
    AnnotatedService annotatedService() {
        return new AnnotatedService();
    }
}
