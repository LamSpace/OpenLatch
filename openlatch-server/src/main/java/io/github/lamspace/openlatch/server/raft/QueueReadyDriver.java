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
 * 队列就绪驱动（v7）：Leader 侧延时形态的唤醒扫描器。
 *
 * <p><b>职责边界</b>：到期是<b>可见性判定</b>而非复制状态迁移——本驱动
 * 只做两件事：按周期调用 {@link ReplicationGateway#sweepQueueReady(long)}
 * 对"有挂起等待者且队首已可消费/容量已空位"的队列键推送 {@code AWAIT_NOTIFY}；
 * MUST NOT 提交任何日志条目、MUST NOT 改变复制状态（消费仍经提交路径在
 * 应用点终判，误唤醒由回执 DENIED/重挂自愈）。与 {@link LeaseExpiryDriver}
 * 的同与异：同——Leader-only 角色短路、当选立即首扫（换任窗口漏扫补偿）、
 * 单守护线程固定周期；异——租约到期是状态迁移故必须进日志，延时唤醒是
 * 提示故零日志。
 *
 * <p><b>线程模型</b>：扫描线程只读影子表与等待队列（均为跨线程弱一致
 * 读面），推送投递经 Netty EventLoop 异步完成；驱动本身无锁状态，
 * {@code start}/{@code close} 幂等。
 */
public final class QueueReadyDriver implements AutoCloseable {

    /** 日志器：扫描兜底记 WARN（单次异常不中断调度）。 */
    private static final Logger log = LoggerFactory.getLogger(QueueReadyDriver.class);

    /** Raft 子系统（角色查询）。 */
    private final RaftSubsystem subsystem;
    /** 复制网关（就绪扫描与推送宿主）。 */
    private final ReplicationGateway gateway;
    /** 扫描周期（毫秒，{@code ready-tick-ms}）。 */
    private final long tickMs;
    /** 调度器（{@link #start} 后非空；volatile 保证 close 可见）。 */
    private volatile ScheduledExecutorService scheduler;

    /**
     * 构造就绪驱动（不启动）。
     *
     * @param subsystem Raft 子系统
     * @param gateway   复制网关
     * @param tickMs    扫描周期（毫秒，{@code >=} 配置下限）
     */
    public QueueReadyDriver(RaftSubsystem subsystem, ReplicationGateway gateway, long tickMs) {
        this.subsystem = subsystem;
        this.gateway = gateway;
        this.tickMs = tickMs;
    }

    /**
     * 启动周期扫描（幂等；仅注册调度，Leader 角色检查在每次 tick 内）。
     */
    public void start() {
        if (scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "openlatch-queue-ready-driver");
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
            gateway.sweepQueueReady(System.currentTimeMillis());
        } catch (RuntimeException e) {
            log.error("queue ready scan failed", e);
        }
    }

    /**
     * 新任期开始：立即触发一次首扫——挂起者随换主清零重挂，其"挂起时
     * 已到期/已空位"的存量就绪状态需要首扫补偿（判例到期驱动）。
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
