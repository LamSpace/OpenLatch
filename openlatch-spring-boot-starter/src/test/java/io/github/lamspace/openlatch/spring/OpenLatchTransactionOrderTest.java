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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.SimpleTransactionStatus;

import io.github.lamspace.openlatch.client.LockGrant;
import io.github.lamspace.openlatch.client.OpenLatchClient;
import io.github.lamspace.openlatch.client.internal.ClientConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 锁定"锁在事务外层"的默认顺序（详设 §8.3 事务交互；spec 场景
 * "提交后才释放锁"）。以真实 Spring 上下文装配切面与事务通知：
 * 切面 {@code @Order(0)}、事务通知默认 {@code LOWEST_PRECEDENCE}，
 * 事件序断言 acquire → begin → business → commit → release。
 */
class OpenLatchTransactionOrderTest {

    /**
     * 同时标注事务与锁注解的被测服务。
     */
    static class TxService {

        /** 事件记录表。 */
        private final List<String> events;

        /**
         * 构造服务。
         *
         * @param events 共享事件记录表
         */
        TxService(List<String> events) {
            this.events = events;
        }

        /**
         * 事务 + 声明式锁方法。
         *
         * @return 固定值
         */
        @Transactional
        @OpenLatch(key = "'tx'")
        public String run() {
            events.add("business");
            return "ok";
        }
    }

    /**
     * 只记录 begin/commit/rollback 事件的事务管理器桩（无真实资源）。
     */
    static class RecordingTxManager implements PlatformTransactionManager {

        /** 事件记录表。 */
        private final List<String> events;

        /**
         * 构造事务管理器桩。
         *
         * @param events 共享事件记录表
         */
        RecordingTxManager(List<String> events) {
            this.events = events;
        }

        /**
         * 记录 begin。
         *
         * @param definition 事务定义
         * @return 简单事务状态
         */
        @Override
        public TransactionStatus getTransaction(TransactionDefinition definition) {
            events.add("begin");
            return new SimpleTransactionStatus();
        }

        /**
         * 记录 commit。
         *
         * @param status 事务状态
         */
        @Override
        public void commit(TransactionStatus status) {
            events.add("commit");
        }

        /**
         * 记录 rollback。
         *
         * @param status 事务状态
         */
        @Override
        public void rollback(TransactionStatus status) {
            events.add("rollback");
        }
    }

    /**
     * 测试装配：CGLIB 代理 + 事务管理 + mock 客户端（获取/释放记事件）。
     */
    @Configuration(proxyBeanMethods = false)
    @EnableAspectJAutoProxy(proxyTargetClass = true)
    @EnableTransactionManagement
    static class TestConfig {

        /**
         * 共享事件记录表。
         *
         * @return 同步事件表
         */
        @Bean
        List<String> events() {
            return Collections.synchronizedList(new ArrayList<>());
        }

        /**
         * mock 客户端：acquire/release 记事件并即时完成，配置给默认超时。
         *
         * @param events 事件表
         * @return mock 客户端
         */
        @Bean
        OpenLatchClient client(List<String> events) {
            OpenLatchClient client = mock(OpenLatchClient.class);
            when(client.config()).thenReturn(new ClientConfig("127.0.0.1", 9410,
                    Duration.ofSeconds(5), Duration.ofSeconds(30), Duration.ofSeconds(3),
                    Duration.ofMillis(200), Duration.ofSeconds(10), 1));
            when(client.acquireAsync(any())).thenAnswer(inv -> {
                events.add("acquire");
                return CompletableFuture.completedFuture(new LockGrant(1L, 30_000L));
            });
            when(client.releaseAsync(anyString(), anyLong(), anyLong())).thenAnswer(inv -> {
                events.add("release");
                return CompletableFuture.completedFuture(null);
            });
            return client;
        }

        /**
         * 锁切面 Bean（由自动代理创建器发现）。
         *
         * @param client 客户端
         * @return 切面实例
         */
        @Bean
        OpenLatchAspect openLatchAspect(OpenLatchClient client) {
            return new OpenLatchAspect(client);
        }

        /**
         * 记录型事务管理器。
         *
         * @param events 事件表
         * @return 事务管理器
         */
        @Bean
        PlatformTransactionManager transactionManager(List<String> events) {
            return new RecordingTxManager(events);
        }

        /**
         * 被测服务。
         *
         * @param events 事件表
         * @return 服务实例
         */
        @Bean
        TxService txService(List<String> events) {
            return new TxService(events);
        }
    }

    /**
     * 事件序 acquire → begin → business → commit → release：
     * 锁获取先于事务开启、释放晚于提交。
     */
    @Test
    void lockWrapsTransactionByDefault() {
        try (AnnotationConfigApplicationContext context =
                     new AnnotationConfigApplicationContext(TestConfig.class)) {
            TxService service = context.getBean(TxService.class);
            assertThat(service.run()).isEqualTo("ok");

            List<String> events = context.getBean("events", List.class);
            assertThat(events).containsExactly(
                    "acquire", "begin", "business", "commit", "release");
        }
    }
}
