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

import io.github.lamspace.openlatch.server.OpenLatchServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * 延时触发 SDK 端到端（v11，单机服务端）：跨客户端一次到期全体共见（装载者
 * 与等待者分属两客户端）、粘滞标记对后到者的即刻通过、重装载开新轮、DISARM
 * 唤醒以异常收束（与超时 false 两形态不混读）、timed await 超时自救、
 * horizon 越界的 {@code INVALID_REQUEST} 参数线映射。判例
 * {@code ClientPhaserIT}；换主自愈与 kill 钟照响面由集群
 * {@code ClusterTimerTest} 与演练负载段承载。
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ClientTimerIT {

    /** 被测服务器（单机）。 */
    private OpenLatchServer server;
    /** 甲方客户端（装载者）。 */
    private OpenLatchClient alpha;
    /** 乙方客户端（旁观者）。 */
    private OpenLatchClient beta;

    @BeforeEach
    void setUp() throws Exception {
        server = ClientTestServers.start(ClientTestServers.config(0));
        alpha = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        beta = OpenLatchClient.builder().address("127.0.0.1:" + server.port()).build();
        alpha.connectAsync().get(5, TimeUnit.SECONDS);
        beta.connectAsync().get(5, TimeUnit.SECONDS);
    }

    @AfterEach
    void tearDown() {
        alpha.close();
        beta.close();
        server.stop();
    }

    @Test
    void crossClientFireOnceAllSeeStickyMarkAndRoundReset() throws Exception {
        OTimer a = alpha.newTimer("it:alarm");
        OTimer b = beta.newTimer("it:alarm");
        assertThat(a.schedule(1_200, TimeUnit.MILLISECONDS)).isEqualTo(1L);
        assertThat(b.isFired()).isFalse();
        assertThat(b.isArmed()).isTrue();
        AtomicReference<Throwable> err = new AtomicReference<>();
        AtomicReference<Boolean> seen = new AtomicReference<>();
        Thread w = new Thread(() -> {
            try {
                seen.set(b.await(20, TimeUnit.SECONDS));
            } catch (Throwable e) {
                err.set(e);
            }
        });
        w.setDaemon(true);
        w.start();
        w.join(30_000);
        assertThat(err.get()).isNull();
        assertThat(seen.get()).isTrue();
        // 粘滞共见：后到的限时等待不经挂即刻 true；读数同为真。
        assertThat(b.await(100, TimeUnit.MILLISECONDS)).isTrue();
        assertThat(a.isFired()).isTrue();
        // 重装载开新轮：粘滞归伪，新等待者落入阻塞→限时预算以 false 自救。
        assertThat(a.schedule(60_000, TimeUnit.MILLISECONDS)).isEqualTo(2L);
        assertThat(b.isFired()).isFalse();
        assertThat(b.await(300, TimeUnit.MILLISECONDS)).isFalse();
    }

    @Test
    void disarmSurfacesAsExceptionAcrossClients() throws Exception {
        OTimer a = alpha.newTimer("it:gone");
        OTimer b = beta.newTimer("it:gone");
        a.schedule(60_000, TimeUnit.MILLISECONDS);
        AtomicReference<Throwable> err = new AtomicReference<>();
        Thread w = new Thread(() -> {
            try {
                b.await();
            } catch (Throwable e) {
                err.set(e);
            }
        });
        w.setDaemon(true);
        w.start();
        Thread.sleep(300);
        assertThat(w.isAlive()).as("未到期应挂起").isTrue();
        a.disarm();
        w.join(20_000);
        assertThat(w.isAlive()).isFalse();
        // 代终结收束为显式异常（与被撤销的等待不可满足语义一致，非超时 false）。
        assertThat(err.get()).isInstanceOf(OpenLatchException.class)
                .hasMessageContaining("unsatisfiable");
        assertThat(b.isArmed()).isFalse();
        assertThat(b.isFired()).isFalse();
    }

    @Test
    void horizonAndFamilyRejectionsMapToExplicitExceptions() throws Exception {
        OTimer a = alpha.newTimer("it:cap");
        // horizon 越界（默认 24h）：参数线 INVALID_REQUEST 显式异常。
        assertThatThrownBy(() -> a.schedule(8, TimeUnit.DAYS))
                .isInstanceOf(OpenLatchException.class)
                .hasMessageContaining("INVALID_REQUEST");
        // （家族冲突矩阵由 TimerGatingTest/CoreEngineTimerTest 在协议与引擎面钉死。）
        // 无条目操作：INVALID_REQUEST 显式（无隐式建钟）。
        assertThatThrownBy(() -> alpha.newTimer("it:none").disarm())
                .isInstanceOf(OpenLatchException.class)
                .hasMessageContaining("INVALID_REQUEST");
    }
}
