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

package io.github.lamspace.openlatch.client;

import io.github.lamspace.openlatch.client.internal.ClientConfig;
import io.netty.util.HashedWheelTimer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link RouteRefresh} 的纪律矩阵：单飞、限频、在飞合并、以及**静默降级**。
 *
 * <p>本测试与端到端无关——它证明的是刷新器自身的节流与静默契约（failures 不上浮、
 * 不发布），即"一次故障不会被放大成探测风暴"这条设计前提。
 */
class RouteRefreshTest {

    /** 测试用共享定时器（与被测实例同生命周期）。 */
    private final HashedWheelTimer timer = new HashedWheelTimer();

    /** 每条用例结束关闭定时器。 */
    @AfterEach
    void tearDown() {
        timer.stop();
    }

    /** 可控探测：手工决定何时完成、以何结果完成。 */
    private static final class ManualProbe implements RouteRefresh.Probe {
        /** 每次调用的完成源（按序记下，供用例逐个完成）。 */
        final List<CompletableFuture<ClientConfig.SeedAddress>> calls =
                new CopyOnWriteArrayList<>();

        @Override
        public CompletableFuture<ClientConfig.SeedAddress> probe() {
            CompletableFuture<ClientConfig.SeedAddress> f = new CompletableFuture<>();
            calls.add(f);
            return f;
        }
    }

    /** 地址夹具。 */
    private static ClientConfig.SeedAddress addr(String host, int port) {
        return new ClientConfig.SeedAddress(host, port);
    }

    @Test
    void concurrentRequestsCoalesceIntoOneProbe() {
        ManualProbe probe = new ManualProbe();
        List<ClientConfig.SeedAddress> published = new CopyOnWriteArrayList<>();
        RouteRefresh refresh = new RouteRefresh(probe, published::add, timer, 50L);

        refresh.request();
        refresh.request();
        refresh.request();
        assertThat(probe.calls)
                .as("单飞：同刻并发触发只发起一次探测").hasSize(1);

        probe.calls.get(0).complete(addr("127.0.0.1", 9));
        assertThat(published)
                .as("探测有结果即发布期望路由").containsExactly(addr("127.0.0.1", 9));
    }

    @Test
    void rateLimitedTriggersDoNotProbeImmediately() {
        ManualProbe probe = new ManualProbe();
        RouteRefresh refresh = new RouteRefresh(probe, a -> { }, timer, 10_000L);

        refresh.request();
        assertThat(probe.calls).hasSize(1);
        probe.calls.get(0).complete(null);
        refresh.request(); // 限频窗内：不得立即发起
        assertThat(probe.calls)
                .as("限频：窗内触发被抑制（转为补探待办）").hasSize(1);
    }

    @Test
    void failedProbeIsSilentAndDoesNotPublish() {
        ManualProbe probe = new ManualProbe();
        AtomicInteger publishes = new AtomicInteger();
        RouteRefresh refresh = new RouteRefresh(probe, a -> publishes.incrementAndGet(), timer, 10L);

        refresh.request();
        probe.calls.get(0).completeExceptionally(new IllegalStateException("probe failed"));
        assertThat(publishes)
                .as("探测失败：不发布、不上浮").hasValue(0);
    }

    @Test
    void probeThrowingIsAlsoSilent() {
        AtomicInteger publishes = new AtomicInteger();
        RouteRefresh refresh = new RouteRefresh(() -> {
            throw new IllegalStateException("probe exploded");
        }, a -> publishes.incrementAndGet(), timer, 10L);

        refresh.request(); // MUST NOT 抛出
        assertThat(publishes).hasValue(0);
    }

    @Test
    void closedRefreshStopsProbing() {
        ManualProbe probe = new ManualProbe();
        RouteRefresh refresh = new RouteRefresh(probe, a -> { }, timer, 10L);
        refresh.close();
        refresh.request();
        assertThat(probe.calls)
                .as("关停后不再发起探测").isEmpty();
    }

    @Test
    void coalescedTriggerLeadsToFollowUpProbeAfterWindow() throws Exception {
        ManualProbe probe = new ManualProbe();
        RouteRefresh refresh = new RouteRefresh(probe, a -> { }, timer, 100L);

        refresh.request();                       // 探测 #1 在飞
        refresh.request();                       // 在飞期间触发 → 合并为补探
        probe.calls.get(0).completeExceptionally(new IllegalStateException("no leader"));

        long deadline = System.currentTimeMillis() + 5_000;
        while (probe.calls.size() < 2 && System.currentTimeMillis() < deadline) {
            Thread.sleep(20);
        }
        assertThat(probe.calls)
                .as("合并的触发在限频窗后补探一次（不丢触发）").hasSizeGreaterThanOrEqualTo(2);
        probe.calls.get(1).complete(null);
    }

    @Test
    void publishFailureDoesNotPropagate() {
        ManualProbe probe = new ManualProbe();
        RouteRefresh refresh = new RouteRefresh(probe, a -> {
            throw new IllegalStateException("publish exploded");
        }, timer, 10L);
        refresh.request();
        // 发布抛出（如目标不可达的建连失败）MUST NOT 上浮：完成链照常收口
        probe.calls.get(0).complete(addr("127.0.0.1", 9));
        assertThat(probe.calls).hasSize(1);
    }
}
