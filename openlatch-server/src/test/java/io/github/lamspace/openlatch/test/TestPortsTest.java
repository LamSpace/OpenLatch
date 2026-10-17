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

package io.github.lamspace.openlatch.test;

import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link TestPorts} 的两条判据：①取样恒落在文档化的非临时带内；②**内核自动分配的口
 * 不得落在该带内**。
 *
 * <p>第②条是本策略的**有效性判据**，刻意不读 {@code /proc/sys/net/ipv4/ip_local_port_range}
 * （沙箱下该文件对 JVM 不可读，实测读到 {@code "3"}）——改为实测内核的临时池样本：
 * 若某环境的临时池与取样带相交，本测试即红，把"策略失效"从偶发 flake 变成显式红灯。
 */
class TestPortsTest {

    /** 取样带有效性所需的最小样本数（相交时命中概率已远高于可忽略水平）。 */
    private static final int KERNEL_SAMPLES = 50;
    /** 取样器自身采样次数。 */
    private static final int SAMPLES = 20;

    /**
     * 取样恒落在文档化的非临时带内（旧取口法 {@code new ServerSocket(0)} 在此为红：
     * 实测 50 次采样全部落在内核临时带 32869–46657，带内计数 0）。
     */
    @Test
    void samplesStayInsideDocumentedNonEphemeralBand() throws Exception {
        for (int i = 0; i < SAMPLES; i++) {
            assertThat(TestPorts.freeNonEphemeral())
                    .as("取样必须落在非临时带内，否则仍可能被并发 connect 抢走")
                    .isBetween(TestPorts.BAND_LO, TestPorts.BAND_HI);
        }
    }

    /**
     * 同一 JVM 内不重复发放：多节点装配会在任何服务端 bind 之前连续取样，重号会让
     * 后一个节点 bind 失败（实测：{@code n1} 与 {@code n3} 拿到同一 Raft 口 ⇒
     * {@code Address already in use}）。本测试是概率性护栏（200 次取样在 10k 带内
     * 无预留时重复概率约 86%）；结构性保证由 {@link TestPorts} 的预留集合承担。
     */
    @Test
    void handedOutPortsAreNeverRepeated() throws Exception {
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            int port = TestPorts.freeNonEphemeral();
            assertThat(seen.add(port))
                    .as("同一 JVM 内端口不得重复发放: " + port)
                    .isTrue();
        }
    }

    /**
     * 内核自动分配的口不落在取样带内——本策略成立的前提。
     */
    @Test
    void kernelAllocatedPortsDoNotFallInsideSamplingBand() throws Exception {
        for (int i = 0; i < KERNEL_SAMPLES; i++) {
            try (ServerSocket probe = new ServerSocket(0)) {
                int port = probe.getLocalPort();
                assertThat(port >= TestPorts.BAND_LO && port <= TestPorts.BAND_HI)
                        .as("内核临时口落入取样带 ⇒ 取样带与本机临时池相交，"
                                + "带外取样失效（须换带）；落带的口=" + port)
                        .isFalse();
            }
        }
    }
}
