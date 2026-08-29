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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.aop.aspectj.annotation.AspectJProxyFactory;

import io.github.lamspace.openlatch.client.AcquireSpec;
import io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException;
import io.github.lamspace.openlatch.client.LockGrant;
import io.github.lamspace.openlatch.client.LockType;
import io.github.lamspace.openlatch.client.OpenLatchClient;
import io.github.lamspace.openlatch.client.OpenLatchException;
import io.github.lamspace.openlatch.client.ServerUnavailableException;
import io.github.lamspace.openlatch.client.internal.ClientConfig;
import io.github.lamspace.openlatch.protocol.StatusCode;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * P1-29 切面行为单测（design D9）：mock 客户端异步内核，不起服务器。
 * 覆盖 waitTime 三分支映射、leaseTime 换算、DENIED/总超时异常语义、
 * 业务异常后释放、锁丢失释放守卫（design D3）、SpEL 求值与缓存。
 */
class OpenLatchAspectTest {

    /** 被切面拦截的样例业务类。 */
    static class DemoService {

        /** 业务是否实际执行的标记。 */
        final AtomicBoolean ran = new AtomicBoolean();

        /**
         * 默认排队获取。
         *
         * @return 固定返回值
         */
        @OpenLatch(key = "'fixed'")
        public String queued() {
            ran.set(true);
            return "ok";
        }

        /**
         * 立即式获取。
         *
         * @return 固定返回值
         */
        @OpenLatch(key = "'fixed'", waitTime = 0)
        public String immediate() {
            ran.set(true);
            return "ok";
        }

        /**
         * 限时等待 + 自定义租约（单位分钟）。
         *
         * @return 固定返回值
         */
        @OpenLatch(key = "'fixed'", waitTime = 2, leaseTime = 10,
                timeUnit = java.util.concurrent.TimeUnit.MINUTES)
        public String timedLease() {
            ran.set(true);
            return "ok";
        }

        /**
         * SpEL 按参数名求值。
         *
         * @param id 锁键来源参数
         * @return 锁键
         */
        @OpenLatch(key = "#id")
        public String byParam(String id) {
            ran.set(true);
            return id;
        }

        /**
         * SpEL 位置引用。
         *
         * @param id 锁键来源参数
         * @return 锁键
         */
        @OpenLatch(key = "#p0")
        public String byPosition(String id) {
            ran.set(true);
            return id;
        }

        /**
         * 读锁类型。
         *
         * @return 固定返回值
         */
        @OpenLatch(key = "'rw'", type = LockType.READ)
        public String read() {
            ran.set(true);
            return "r";
        }

        /**
         * 业务抛异常路径。
         *
         * @return 不会返回
         * @throws IllegalStateException 恒定抛出
         */
        @OpenLatch(key = "'fixed'")
        public String failing() {
            ran.set(true);
            throw new IllegalStateException("business");
        }

        /**
         * 表达式对未定义变量调方法（求值期失败）。
         *
         * @return 不会返回
         */
        @OpenLatch(key = "#nope.length()")
        public String badExpression() {
            ran.set(true);
            return "never";
        }
    }

    /** mock 的客户端。 */
    private OpenLatchClient client;
    /** 被测切面。 */
    private OpenLatchAspect aspect;
    /** 织入了切面的代理服务对象。 */
    private DemoService service;
    /** 最近一次释放调用收到的 token，-1 表示未释放。 */
    private final AtomicReference<Long> releasedToken = new AtomicReference<>(-1L);
    /** 代理背后的目标实例（业务标记须在目标上观察，代理字段是另一份拷贝）。 */
    private DemoService target;

    /**
     * 每个用例重建 mock 客户端与代理：acquire 默认授予 token 7、
     * release 默认成功。
     */
    @BeforeEach
    void setUp() {
        client = mock(OpenLatchClient.class);
        when(client.config()).thenReturn(new ClientConfig("127.0.0.1", 9410,
                Duration.ofSeconds(5), Duration.ofSeconds(30), Duration.ofSeconds(3),
                Duration.ofMillis(200), Duration.ofSeconds(10), 1));
        when(client.acquireAsync(any())).thenReturn(
                CompletableFuture.completedFuture(new LockGrant(7L, 30_000L)));
        when(client.releaseAsync(anyString(), anyLong(), anyLong()))
                .thenAnswer(inv -> {
                    releasedToken.set(inv.getArgument(1));
                    return CompletableFuture.completedFuture(null);
                });
        aspect = new OpenLatchAspect(client);
        target = new DemoService();
        AspectJProxyFactory factory = new AspectJProxyFactory(target);
        factory.addAspect(aspect);
        service = factory.getProxy();
    }

    /**
     * 默认排队：waitMs=-1、leaseMs=0、类型 REENTRANT，业务执行并释放。
     */
    @Test
    void queuedAcquireMapsAndWaitTimeAndReleases() {
        assertThat(service.queued()).isEqualTo("ok");
        assertThat(target.ran).isTrue();

        ArgumentCaptor<AcquireSpec> spec = ArgumentCaptor.forClass(AcquireSpec.class);
        verify(client).acquireAsync(spec.capture());
        assertThat(spec.getValue().waitMs()).isEqualTo(-1);
        assertThat(spec.getValue().leaseMs()).isZero();
        assertThat(spec.getValue().lockType()).isEqualTo(LockType.REENTRANT);
        assertThat(spec.getValue().key()).isEqualTo("fixed");
        assertThat(releasedToken.get()).isEqualTo(7L);
    }

    /**
     * 立即式 DENIED → LockAcquisitionTimeoutException，业务不执行、不释放。
     */
    @Test
    void immediateDeniedThrowsAndSkipsBusiness() {
        when(client.acquireAsync(any())).thenReturn(CompletableFuture.failedFuture(
                new OpenLatchException(StatusCode.DENIED, "denied")));

        assertThatThrownBy(() -> service.immediate())
                .isInstanceOf(LockAcquisitionTimeoutException.class);
        assertThat(target.ran).isFalse();
        verify(client, never()).releaseAsync(anyString(), anyLong(), anyLong());
    }

    /**
     * 总超时异常原样传播（客户端 future 以 LATEx 失败）。
     */
    @Test
    void totalTimeoutPropagatesAsAcquisitionTimeout() {
        when(client.acquireAsync(any())).thenReturn(CompletableFuture.failedFuture(
                new LockAcquisitionTimeoutException("queued timeout")));

        assertThatThrownBy(() -> service.queued())
                .isInstanceOf(LockAcquisitionTimeoutException.class)
                .hasMessage("queued timeout");
        assertThat(target.ran).isFalse();
    }

    /**
     * 限时与租约按 timeUnit 换算进 spec：waitTime=2min→120000、leaseTime=10min→600000。
     */
    @Test
    void waitAndLeaseConvertThroughTimeUnit() {
        service.timedLease();
        ArgumentCaptor<AcquireSpec> spec = ArgumentCaptor.forClass(AcquireSpec.class);
        verify(client).acquireAsync(spec.capture());
        assertThat(spec.getValue().waitMs()).isEqualTo(Duration.ofMinutes(2).toMillis());
        assertThat(spec.getValue().leaseMs()).isEqualTo(Duration.ofMinutes(10).toMillis());
    }

    /**
     * 业务异常仍释放且原样传播。
     */
    @Test
    void businessExceptionStillReleasesAndPropagates() {
        assertThatThrownBy(() -> service.failing())
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("business");
        assertThat(releasedToken.get()).isEqualTo(7L);
    }

    /**
     * 释放遇"锁已丢失"（NOT_HELD）：业务已抛异常时不掩盖、不替换。
     */
    @Test
    void lostLockReleaseDoesNotMaskBusinessError() {
        when(client.releaseAsync(anyString(), anyLong(), anyLong()))
                .thenReturn(CompletableFuture.failedFuture(
                        new OpenLatchException(StatusCode.NOT_HELD, "lost")));

        assertThatThrownBy(() -> service.failing())
                .isInstanceOf(IllegalStateException.class);
    }

    /**
     * 释放遇断连不可达（ServerUnavailable）且业务成功：静默跳过，正常返回。
     */
    @Test
    void disconnectOnReleaseIsSwallowedOnSuccess() {
        when(client.releaseAsync(anyString(), anyLong(), anyLong()))
                .thenReturn(CompletableFuture.failedFuture(
                        new ServerUnavailableException("disconnected")));

        assertThat(service.queued()).isEqualTo("ok");
    }

    /**
     * 释放真实失败（INTERNAL_ERROR）且业务成功：抛 OpenLatchException 暴露。
     */
    @Test
    void realReleaseFailureSurfacesOnSuccessPath() {
        when(client.releaseAsync(anyString(), anyLong(), anyLong()))
                .thenReturn(CompletableFuture.failedFuture(
                        new OpenLatchException(StatusCode.INTERNAL_ERROR, "boom")));

        assertThatThrownBy(() -> service.queued())
                .isInstanceOf(OpenLatchException.class);
    }

    /**
     * SpEL：#参数名 与 #p0 位置引用均可求值，键透传进 spec。
     */
    @Test
    void spelResolvesParamNameAndPosition() {
        service.byParam("order-1");
        service.byPosition("order-2");

        ArgumentCaptor<AcquireSpec> spec = ArgumentCaptor.forClass(AcquireSpec.class);
        verify(client, times(2)).acquireAsync(spec.capture());
        List<AcquireSpec> specs = spec.getAllValues();
        assertThat(specs.get(0).key()).isEqualTo("order-1");
        assertThat(specs.get(1).key()).isEqualTo("order-2");
    }

    /**
     * SpEL 空/无值求值：null 参数 → OpenLatchException，业务不执行。
     */
    @Test
    void spelEmptyResultThrowsBeforeBusiness() {
        assertThatThrownBy(() -> service.byParam(null))
                .isInstanceOf(OpenLatchException.class)
                .hasMessageContaining("non-empty");
        assertThat(target.ran).isFalse();
    }

    /**
     * 表达式引用未定义变量（求值期失败）→ OpenLatchException，业务不执行。
     */
    @Test
    void spelUnknownVariableFailsExplicitly() {
        assertThatThrownBy(() -> service.badExpression())
                .isInstanceOf(OpenLatchException.class)
                .hasMessageContaining("-parameters");
        assertThat(target.ran).isFalse();
    }

    /**
     * 缓存复用：同方法重复调用只解析一次表达式。
     */
    @Test
    void expressionCacheReusedAcrossCalls() {
        service.byParam("a");
        service.byParam("b");
        assertThat(aspect.cachedExpressionCount()).isEqualTo(1);
    }

    /**
     * READ 类型直接进 spec。
     */
    @Test
    void readTypePassesThrough() {
        service.read();
        ArgumentCaptor<AcquireSpec> spec = ArgumentCaptor.forClass(AcquireSpec.class);
        verify(client).acquireAsync(spec.capture());
        assertThat(spec.getValue().lockType()).isEqualTo(LockType.READ);
    }

}
