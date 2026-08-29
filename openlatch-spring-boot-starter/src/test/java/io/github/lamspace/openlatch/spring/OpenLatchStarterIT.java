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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import io.github.lamspace.openlatch.client.AcquireSpec;
import io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException;
import io.github.lamspace.openlatch.client.LockGrant;
import io.github.lamspace.openlatch.client.LockType;
import io.github.lamspace.openlatch.client.OpenLatchClient;
import io.github.lamspace.openlatch.server.OpenLatchServer;
import io.github.lamspace.openlatch.server.ServerConfig;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * P1-30 starter 集成测试（§10.3 starter 部分，design D9）：
 * 进程内真实服务器 + 真实 Boot 上下文（验收标准 4 形态），覆盖
 * 并发互斥、SpEL 参数隔离、获取失败抛异常、READ/WRITE 矩阵。
 */
@SpringBootTest(classes = StarterITApp.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OpenLatchStarterIT {

    /** 临时端口内嵌服务器（上下文创建前就绪，经动态属性注入端口）。 */
    private static final OpenLatchServer SERVER = startServer();

    /** 共享工作线程池。 */
    private final ExecutorService pool = Executors.newFixedThreadPool(16);

    @Autowired
    private OpenLatchClient client;
    @Autowired
    private AnnotatedService service;

    /** 目标实例上的探针（代理字段不可读，见 AnnotatedService 注释）。 */
    private AnnotatedService.Probe probe;

    /**
     * 启动端口 0 的内嵌服务器（其余取服务端默认配置）。
     *
     * @return 已启动服务器
     */
    private static OpenLatchServer startServer() {
        ServerConfig d = ServerConfig.defaults();
        OpenLatchServer server = new OpenLatchServer(new ServerConfig(0,
                d.workerThreads(), d.idleTimeoutMs(), d.defaultLeaseMs(), d.minLeaseMs(),
                d.maxLeaseMs(), d.leaseTickIntervalMs(), d.headReplyTimeoutMs(),
                d.maxKeyLength(), d.maxQueueDepthPerKey(), d.maxInflightPerConnection()));
        server.start();
        return server;
    }

    /**
     * 把实际监听端口注入 {@code openlatch.server-port}。
     *
     * @param registry 动态属性注册器
     */
    @DynamicPropertySource
    static void openlatchProperties(DynamicPropertyRegistry registry) {
        registry.add("openlatch.server-port", SERVER::port);
    }

    /**
     * 连接就绪门闩：异步首连完成后用例才开始（design D9，不 sleep）。
     */
    @BeforeAll
    void awaitConnection() throws Exception {
        probe = service.probe();
        client.connectAsync().get(10, TimeUnit.SECONDS);
    }

    /**
     * 每个用例前复位探针，避免用例间串扰。
     */
    @BeforeEach
    void resetProbe() {
        probe.resetCounters();
    }

    /**
     * 关停服务器与线程池。
     */
    @AfterAll
    void tearDown() {
        pool.shutdownNow();
        SERVER.stop();
    }

    /**
     * 16 线程 × 5 轮并发执行注解互斥方法：无并发违例，
     * 且非原子读-改-写计数器不丢更新（终值 == 80）。
     */
    @Test
    void concurrentAnnotationCallsAreMutuallyExclusive() throws Exception {
        List<Future<?>> futures = new ArrayList<>();
        for (int t = 0; t < 16; t++) {
            futures.add(pool.submit(() -> {
                for (int i = 0; i < 5; i++) {
                    try {
                        service.sharedCritical();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(e);
                    }
                }
            }));
        }
        for (Future<?> f : futures) {
            f.get(60, TimeUnit.SECONDS);
        }
        assertThat(probe.violations.get()).isZero();
        assertThat(probe.plainCounter).isEqualTo(80);
    }

    /**
     * SpEL：不同参数求出不同 key，两调用并发穿过屏障（互不阻塞）。
     */
    @Test
    void differentSpelKeysRunConcurrently() throws Exception {
        CyclicBarrier gate = new CyclicBarrier(2);
        Future<String> a = pool.submit(() -> service.keyedWithBarrier("order-A", gate));
        Future<String> b = pool.submit(() -> service.keyedWithBarrier("order-B", gate));
        assertThat(a.get(10, TimeUnit.SECONDS)).isEqualTo("order-A");
        assertThat(b.get(10, TimeUnit.SECONDS)).isEqualTo("order-B");
    }

    /**
     * SpEL：相同参数求出同一 key，第二个调用必须排队——先持有者独自在
     * 屏障内等到超时，证明第二次没有并发进入。
     */
    @Test
    void sameSpelKeySerializesCalls() throws Exception {
        CyclicBarrier gate = new CyclicBarrier(2);
        Future<String> first = pool.submit(() -> service.keyedWithBarrier("order-X", gate));
        Thread.sleep(150);
        Future<String> second = pool.submit(() -> service.keyedWithBarrier("order-X", gate));

        // 第一个：屏障等不到第二个体内（第二个在锁外排队）→ await 超时并置断
        assertThatThrownBy(() -> first.get(10, TimeUnit.SECONDS))
                .hasCauseInstanceOf(TimeoutException.class);
        // 第二个随后被授予并独自超时
        assertThatThrownBy(() -> second.get(15, TimeUnit.SECONDS))
                .hasCauseInstanceOf(BrokenBarrierException.class);
        // 全程无并发进入违例
        assertThat(probe.violations.get()).isZero();
    }

    /**
     * 锁被外部裸客户端持有时，立即式注解调用抛
     * {@link LockAcquisitionTimeoutException}；外部释放后可正常获取执行。
     */
    @Test
    void acquisitionFailureThrowsAndBusinessSkipped() throws Exception {
        LockGrant held = client.acquireAsync(
                new AcquireSpec("held", LockType.REENTRANT, -777L, 5_000L, -1))
                .get(10, TimeUnit.SECONDS);

        assertThatThrownBy(() -> service.whileExternalHeld())
                .isInstanceOf(LockAcquisitionTimeoutException.class);

        client.releaseAsync("held", held.leaseToken(), -777L).get(10, TimeUnit.SECONDS);
        assertThat(service.whileExternalHeld()).isEqualTo("ran");
    }

    /**
     * 双读者共享持有：两个 READ 注解方法并发穿过屏障。
     */
    @Test
    void twoReadersHoldConcurrently() throws Exception {
        CyclicBarrier gate = new CyclicBarrier(2);
        Future<String> r1 = pool.submit(() -> service.readWithBarrier(gate));
        Future<String> r2 = pool.submit(() -> service.readWithBarrier(gate));
        assertThat(r1.get(10, TimeUnit.SECONDS)).isEqualTo("read");
        assertThat(r2.get(10, TimeUnit.SECONDS)).isEqualTo("read");
    }

    /**
     * 写者持有期间读者排队：读者进入时刻不早于写者离开时刻，且无违例。
     */
    @Test
    void writerExcludesReader() throws Exception {
        Future<String> w = pool.submit(service::writeDoc);
        assertThat(probe.writerEntered.await(10, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(100);
        Future<String> r = pool.submit(service::readDoc);

        assertThat(w.get(20, TimeUnit.SECONDS)).isEqualTo("write");
        assertThat(r.get(20, TimeUnit.SECONDS)).isEqualTo("read");
        assertThat(probe.readerEntryNanos)
                .isGreaterThanOrEqualTo(probe.writerExitNanos);
        assertThat(probe.violations.get()).isZero();
    }
}
