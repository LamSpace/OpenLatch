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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 测试侧端口取样器：为"探针取口 → 稍后真 bind"式装配提供**不会被并发 connect 抢走**的口。
 *
 * <p><b>要解决的问题（探针-重绑窗口不可消除）</b>：装配一个多节点集群时，端口必须在
 * 启动前先选好（要填 {@code client-addresses}、要组 peer 列表），而"选"只能靠探针
 * ——绑一下、拿到口、立刻关掉。探针一关，这个口就**回到内核的临时端口池**；随后服务端
 * 真 bind 之前的那段窗口里，同机任何一次 {@code connect}（本仓测试 JVM 里最不缺这个：
 * 客户端后台重连、路由刷新探测、获取车道建连……）都可能被内核正好派到**这同一个刚回收的
 * 口**——于是真 bind 撞 {@code EADDRINUSE}。这段窗口无法消除，只能让窗口里**没有竞争者**。
 *
 * <p><b>治法：把取样带挪出临时端口池</b>。竞争者就是内核的临时口分配器本身，所以只要
 * 取样口**不在临时带内**，它就绝不会被 {@code connect} 拿走，窗口随之失效。本类取
 * {@code [20000, 29999]}——与演练侧（{@code LeaderKillDrillIT} 的私有实现，同一策略的
 * 既有判例）同带。带的选择有一条**显式有效性判据**：内核自动分配的口不得落在该带内
 * （由 {@link TestPortsTest} 断言）——某环境临时池若与该带相交，测试即红，提示换带，
 * 而不是把它留给偶发 flake。
 *
 * <p><b>为什么 {@code setReuseAddress(false)}</b>：探针必须**真的**能绑住候选口，
 * 否则残留在 {@code TIME_WAIT} 的口会被探针误判为可用（关掉 reuse 后绑定失败，
 * 探针自然会换下一个候选）。探针至多换 {@value #MAX_ATTEMPTS} 次候选，容忍带内的
 * 偶发占用。
 *
 * <p><b>线程模型</b>：无状态、可并发调用；候选随机取样（{@link ThreadLocalRandom}）。
 */
final class TestPorts {

    /** 取样带下界（含）：刻意位于 OS 临时端口带之外，见类契约。 */
    static final int BAND_LO = 20_000;
    /** 取样带上界（含）。 */
    static final int BAND_HI = 29_999;
    /** 带内换候选的最大次数（容忍带内偶发占用）。 */
    private static final int MAX_ATTEMPTS = 100;

    /**
     * 工具类，不实例化。
     */
    private TestPorts() {
    }

    /**
     * 取一个"当前可绑定、且不在 OS 临时端口带内"的口。
     *
     * <p>语义：返回的口在调用时刻**确可绑定**（探针实测过），且因位于取样带内而
     * **不可能被内核的临时口分配器派给并发 {@code connect}**——这正是探针-重绑窗口
     * 安全的前提。返回值只保证"此刻"，调用方仍应在 bind 失败时按业务语义处置
     * （本仓既有纪律：装配失败即测试失败，不靠重试掩盖）。
     *
     * @return 取样带内的可绑定端口号
     * @throws IOException 带内连续 {@value #MAX_ATTEMPTS} 次候选均被占用（异常信息含带范围）
     */
    static int freeNonEphemeral() throws IOException {
        for (int attempt = 0; attempt < MAX_ATTEMPTS; attempt++) {
            int candidate = BAND_LO + ThreadLocalRandom.current()
                    .nextInt(BAND_HI - BAND_LO + 1);
            try (ServerSocket probe = new ServerSocket()) {
                probe.setReuseAddress(false);
                probe.bind(new InetSocketAddress(candidate));
                return candidate;
            } catch (IOException busy) {
                // 被占或 TIME_WAIT 残留：换下一个候选
            }
        }
        throw new IOException("no free non-ephemeral port in ["
                + BAND_LO + ", " + BAND_HI + "]");
    }
}
