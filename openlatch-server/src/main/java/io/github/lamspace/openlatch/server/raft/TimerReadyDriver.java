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

package io.github.lamspace.openlatch.server.raft;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * timer 就绪驱动（v11）：Leader 侧延时触发的到期唤醒扫描器。
 *
 * <p><b>职责边界</b>：到期是<b>派生谓词</b>而非复制状态迁移——本驱动只做
 * 两件事：按周期调用 {@link ReplicationGateway#sweepTimerReady(long)} 对
 * "有挂起等待者且当代 {@code armed ∧ now ≥ fire_at_ms}"的 timer 键推送
 * {@code AWAIT_NOTIFY}；MUST NOT 提交任何日志条目、MUST NOT 改变复制状态
 * （时钟走过到期点日志恒零新增——"到期误入日志即红"反向守卫的驱动侧对偶，
 * 唤醒是纯提示，误唤醒与漏唤醒均由等待方重发时的谓词重评自愈）。与
 * {@link LeaseExpiryDriver} 的同与异（判例原文沿用）：同——Leader-only
 * 角色短路、当选立即首扫（换任窗口漏扫补偿）、单守护线程固定周期；异——
 * 租约到期是状态迁移故必须进日志，timer 到期是可见性判定故零日志
 * （{@code QueueReadyDriver} 同款分界）。与 {@code QueueReadyDriver} 的
 * 差异仅在扫描对象：队列 tick 挂元素可见性、本 tick 挂标记唤醒，两周期
 * 分名分值（{@code timer-ready-tick-ms} 独立可调、互不耦合调参）。
 *
 * <p><b>线程模型</b>：扫描线程只读影子表与等待簿记（均为跨线程弱一致
 * 读面），推送投递经 Netty EventLoop 异步完成；驱动本身无锁状态，
 * {@code start}/{@code close} 幂等。
 */
public final class TimerReadyDriver implements AutoCloseable {

    /** 日志器：扫描兜底记 WARN（单次异常不中断调度）。 */
    private static final Logger log = LoggerFactory.getLogger(TimerReadyDriver.class);

    /** Raft 子系统（角色查询）。 */
    private final RaftSubsystem subsystem;
    /** 复制网关（到期扫描与唤醒投递宿主）。 */
    private final ReplicationGateway gateway;
    /** 扫描周期（毫秒，{@code timer-ready-tick-ms}）。 */
    private final long tickMs;
    /** 指标门面（到期唤醒集合事件计数；{@code null}=不埋点）。 */
    private final io.github.lamspace.openlatch.server.metrics.ServerMetrics metrics;
    /** 调度器（{@link #start} 后非空；volatile 保证 close 可见）。 */
    private volatile ScheduledExecutorService scheduler;

    /**
     * 构造就绪驱动（不启动，不埋点——测试夹具形态）。
     *
     * @param subsystem Raft 子系统
     * @param gateway   复制网关
     * @param tickMs    扫描周期（毫秒，{@code >=} 配置下限）
     */
    public TimerReadyDriver(RaftSubsystem subsystem, ReplicationGateway gateway, long tickMs) {
        this(subsystem, gateway, tickMs, null);
    }

    /**
     * 构造就绪驱动（不启动；生产形态经 {@code metrics} 记到期唤醒集合事件）。
     *
     * @param subsystem Raft 子系统
     * @param gateway   复制网关
     * @param tickMs    扫描周期（毫秒）
     * @param metrics   指标门面，可为 {@code null}（不埋点）
     */
    public TimerReadyDriver(RaftSubsystem subsystem, ReplicationGateway gateway, long tickMs,
            io.github.lamspace.openlatch.server.metrics.ServerMetrics metrics) {
        this.subsystem = subsystem;
        this.gateway = gateway;
        this.tickMs = tickMs;
        this.metrics = metrics;
    }

    /**
     * 启动周期扫描（幂等；仅注册调度，Leader 角色检查在每次 tick 内）。
     */
    public void start() {
        if (scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "openlatch-timer-ready-driver");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::scanSafely, tickMs, tickMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 单次扫描：非 Leader 短路（判定在 gateway 内再核一道，防角色事件与
     * 调度时序的窄窗竞态）。
     */
    private void scanSafely() {
        try {
            if (!subsystem.isLeader()) {
                return;
            }
            int events = gateway.sweepTimerReady(System.currentTimeMillis());
            if (metrics != null) {
                // 唤醒面计数（每键每轮至多一事件；无等待者的静默到期零计——
                // fired 口径与"到期发生于谓词"的计数面证据，判例到期计数
                // 走扫描返回值）。
                metrics.recordTimerFired(events);
            }
        } catch (RuntimeException e) {
            log.error("timer ready scan failed", e);
        }
    }

    /**
     * 新任期开始：立即触发一次首扫——挂起者随换主清零重挂，其"挂起时
     * 已到期"的存量状态需要首扫补偿；复制账簿的到期判定与本地时钟无关，
     * 首扫只补投递、不补状态（判例队列/到期驱动同句）。
     */
    public void onLeadershipGained() {
        scanSafely();
    }

    /**
     * 关停扫描线程（子系统关停前调用）。幂等。
     */
    @Override
    public void close() {
        ScheduledExecutorService s = scheduler;
        scheduler = null;
        if (s != null) {
            s.shutdownNow();
        }
    }
}
