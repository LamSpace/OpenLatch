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

package io.github.lamspace.openlatch.server.metrics;

import io.github.lamspace.openlatch.core.CoreEngine;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.raft.LeaderTracker;
import io.github.lamspace.openlatch.server.raft.ShadowTable;
import io.github.lamspace.openlatch.server.raft.WaitQueue;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

/**
 * 服务端指标词表与埋点门面。
 *
 * <p><b>单一命名点</b>：全部指标的逻辑名/标签常量收编于本类，
 * {@code RequestDispatcher}（单机）与
 * {@code ClusterRequestHandler}（集群）都经本类记录，同一指标在两种装配下
 * 名称、标签、计数口径一致。Prometheus 线路名由 Micrometer 命名翻译生成
 * （点转下划线；counter 的 {@code .total} 逻辑名翻译为 {@code _total} 尾且
 * 不重复追加；Timer 输出 {@code _seconds_bucket/_count/_sum}），映射表以
 * {@code ServerMetricsVocabularyTest} 为唯一权威断言点。
 *
 * <p><b>消息 → 指标映射口径</b>：
 * {@code LOCK_ACQUIRE}/{@code LATCH_AWAIT} 应答计入 {@code acquire.total{status}}
 * 与 {@code acquire.duration{result}}（屏障等待同为"获取-排队"形态，result 按
 * 应答状态码折算 {@code granted/queued/denied}）；{@code LOCK_RELEASE}/
 * {@code LATCH_COUNT_DOWN} 计入 {@code release.total{status}}；
 * {@code LEASE_RENEW} 计入 {@code renew.total{status}}；v4 起 {@code ATOMIC_OP}
 * 单独计入 {@code atomic.total{kind,op,status}}（记于分发/受理点而非应答收口——
 * kind/op 维度只在请求侧存在，形状非法的请求不计数）。无应答 payload 的信封
 * （PING 回包、未知类型）不计。
 *
 * <p><b>启停语义</b>：{@code metrics.enabled=false} 仅关闭 {@code /metrics}
 * 对外服务，埋点照常累积进内存注册表（"关闭即不抓取"而非
 * "关闭即无数据"）；本类永不为 {@code null} 使用，装配点可判空跳过
 * （直接构造的测试夹具允许不挂指标）。
 *
 * <p><b>线程模型</b>：注册表线程安全（Micrometer 契约）；记录方法由连接
 * EventLoop、租约扫描线程、Raft 应用/回调线程并发调用，counter 为
 * LongAdder 底、timer 为无锁直方图，无跨指标聚合写入。
 */
public final class ServerMetrics {

    /** 当前持有中的条目数（按家族标签），gauge。 */
    public static final String LOCKS_HELD = "openlatch.server.locks.held";
    /** 全部等待队列条目总数，gauge。 */
    public static final String WAITERS = "openlatch.server.waiters";
    /** 本节点活跃会话数，gauge。 */
    public static final String SESSIONS = "openlatch.server.sessions";
    /** 获取请求计数（按应答状态码），counter。 */
    public static final String ACQUIRE_TOTAL = "openlatch.server.acquire.total";
    /** 获取处理耗时（granted/queued/denied 三结果），timer。 */
    public static final String ACQUIRE_DURATION = "openlatch.server.acquire.duration";
    /** 释放请求计数（含屏障倒计数），counter。 */
    public static final String RELEASE_TOTAL = "openlatch.server.release.total";
    /** 续租请求计数（失败续租是锁丢失前兆），counter。 */
    public static final String RENEW_TOTAL = "openlatch.server.renew.total";
    /** 租约到期强制释放次数，counter。 */
    public static final String LEASE_EXPIRED_TOTAL = "openlatch.server.lease.expired.total";
    /** 单 key 队列深度最大值（抓取时刻采样），gauge。 */
    public static final String QUEUE_DEPTH_MAX = "openlatch.server.queue.depth.max";
    /** 本节点是否 Leader（仅集群启用注册），gauge。 */
    public static final String CLUSTER_IS_LEADER = "openlatch.cluster.is_leader";
    /** v4：原子变量操作计数（按形态/操作/应答状态码维度），counter。 */
    public static final String ATOMIC_TOTAL = "openlatch.server.atomic.total";

    /** 锁家族 held 线的 type 标签值。 */
    public static final String TYPE_LOCK = "lock";
    /** Semaphore 家族 held 线的 type 标签值。 */
    public static final String TYPE_SEMAPHORE = "semaphore";
    /** 原子形态标签值：long。 */
    public static final String ATOMIC_KIND_LONG = "long";
    /** 原子形态标签值：integer。 */
    public static final String ATOMIC_KIND_INTEGER = "integer";
    /** 原子形态标签值：boolean。 */
    public static final String ATOMIC_KIND_BOOLEAN = "boolean";

    /** 耗时 result 标签值：授予。 */
    public static final String RESULT_GRANTED = "granted";
    /** 耗时 result 标签值：入队挂起。 */
    public static final String RESULT_QUEUED = "queued";
    /** 耗时 result 标签值：拒绝（含非法请求与提交失败）。 */
    public static final String RESULT_DENIED = "denied";

    /** Prometheus 注册表（scrape 数据源），生命周期随服务器。 */
    private final PrometheusMeterRegistry registry;
    /** 耗时计时器按 result 标签的惰性缓存（三档封顶，注册即定形）。 */
    private final ConcurrentHashMap<String, Timer> durationTimers = new ConcurrentHashMap<>();
    /**
     * gauge 绑定对象的强引用保活表：Micrometer gauge 对 obj 仅持弱引用，
     * 无人保活的 supplier（如 {@code is_leader} 的角色判定 lambda）会被 GC
     * 回收并连带摘除仪表。服务器存续期内绑定一次、不清理。
     */
    private final java.util.List<Object> gaugeBindKeepAlive =
            new java.util.concurrent.CopyOnWriteArrayList<>();

    /**
     * 构造空词表注册表（生产形态，Prometheus 默认配置）。
     */
    public ServerMetrics() {
        this(new PrometheusMeterRegistry(PrometheusConfig.DEFAULT));
    }

    /**
     * 以既有注册表构造（测试注入 {@code SimpleMeterRegistry} 场景经
     * {@link #ServerMetrics(PrometheusMeterRegistry)} 的父类收窄替代——
     * 本门面固定 Prometheus 类型以便管理端点直接 scrape）。
     *
     * @param registry 指标注册表
     */
    public ServerMetrics(PrometheusMeterRegistry registry) {
        this.registry = registry;
    }

    /**
     * 底层注册表（管理端点 scrape 与测试断言入口）。
     *
     * @return Prometheus 注册表
     */
    public PrometheusMeterRegistry registry() {
        return registry;
    }

    /**
     * 单机路径收口：按分发产出的应答信封记录一次计数（获取类消息附带
     * 耗时样本）。{@code resp} 为 {@code null}（PING）不计。
     *
     * @param resp         分发产出的应答信封
     * @param elapsedNanos 分发处理耗时（纳秒，单机口径为 dispatch 起止）
     */
    public void recordDispatch(Envelope resp, long elapsedNanos) {
        if (resp == null) {
            return;
        }
        switch (resp.getType()) {
            case LOCK_ACQUIRE, LATCH_AWAIT -> {
                StatusCode st = resp.getType() == io.github.lamspace.openlatch.protocol.MessageType.LOCK_ACQUIRE
                        ? resp.getAcquireResponse().getStatus()
                        : resp.getLatchAwaitResponse().getStatus();
                recordAcquire(st, elapsedNanos);
            }
            case LOCK_RELEASE, LATCH_COUNT_DOWN -> {
                StatusCode st = resp.getType() == io.github.lamspace.openlatch.protocol.MessageType.LOCK_RELEASE
                        ? resp.getReleaseResponse().getStatus()
                        : resp.getLatchCountDownResponse().getStatus();
                recordRelease(st);
            }
            case LEASE_RENEW -> recordRenew(resp.getLeaseRenewResponse().getStatus());
            default -> {
                // 无 payload 或无对应指标族的信封（未知类型、ATOMIC_OP 等）不计——
                // v4 原子计数记于分发/受理点（kind/op 维度只存在于请求侧，
                // 见 recordAtomic），且形状非法的请求不计数，杜绝维度伪造。
            }
        }
    }

    /**
     * 记录一次获取应答（含屏障 await）：计数 + 耗时样本。
     *
     * @param status       应答协议状态码
     * @param elapsedNanos 请求受理至应答生成（单机为 dispatch 起止；集群为
     *                     受理至写回，含 Raft 提交等待）
     */
    public void recordAcquire(StatusCode status, long elapsedNanos) {
        count(ACQUIRE_TOTAL, status);
        durationTimer(resultOf(status)).record(elapsedNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * 获取耗时计时器（按 result 三档缓存）：开 Prometheus 直方图桶，
     * 线路形态钉为 {@code _seconds_bucket/_count/_sum}；
     * 桶位取 Micrometer 默认集，非配置项。
     *
     * @param result 结果标签值
     * @return 计时器实例
     */
    private Timer durationTimer(String result) {
        return durationTimers.computeIfAbsent(result, r -> Timer.builder(ACQUIRE_DURATION)
                .tag("result", r)
                .publishPercentileHistogram()
                .register(registry));
    }

    /**
     * 记录一次释放/倒计数应答。
     *
     * @param status 应答协议状态码
     */
    public void recordRelease(StatusCode status) {
        count(RELEASE_TOTAL, status);
    }

    /**
     * 记录一次续租应答（失败续租计数是锁丢失前兆告警输入）。
     *
     * @param status 应答协议状态码
     */
    public void recordRenew(StatusCode status) {
        count(RENEW_TOTAL, status);
    }

    /**
     * 记录一次已受理（形状合法）的原子操作应答：计数线
     * {@code atomic_total{kind,op,status}}。CAS 家族"未落值"是
     * {@code status=OK} 的有效读数，成败区分不经本指标维度（避免线的组合
     * 爆炸，成败量由 {@code cas/cas_stamped} 线对照写侧总量推得）。
     * 调用点在单机 {@code RequestDispatcher.dispatchAtomicOp} 与集群
     * {@code ClusterRequestHandler.handleAtomicOp}——两形态同一收口口径。
     *
     * @param kind   协议形态判别（三原子类型之一；其余值记 "other" 防御线）
     * @param op     协议操作枚举（未知值记 "unknown" 防御线）
     * @param status 应答协议状态码
     */
    public void recordAtomic(io.github.lamspace.openlatch.protocol.LockType kind,
                             io.github.lamspace.openlatch.protocol.AtomicOp op,
                             StatusCode status) {
        Counter.builder(ATOMIC_TOTAL)
                .tag("kind", switch (kind) {
                    case LOCK_TYPE_ATOMIC_LONG -> ATOMIC_KIND_LONG;
                    case LOCK_TYPE_ATOMIC_INTEGER -> ATOMIC_KIND_INTEGER;
                    case LOCK_TYPE_ATOMIC_BOOLEAN -> ATOMIC_KIND_BOOLEAN;
                    default -> "other";
                })
                .tag("op", switch (op) {
                    case ATOMIC_GET -> "get";
                    case ATOMIC_SET -> "set";
                    case ATOMIC_GET_AND_SET -> "get_and_set";
                    case ATOMIC_ADD -> "add";
                    case ATOMIC_CAS -> "cas";
                    case ATOMIC_CAS_STAMPED -> "cas_stamped";
                    default -> "unknown";
                })
                .tag("status", status.name())
                .register(registry).increment();
    }

    /**
     * 租约到期强制释放计数（单机由扫描线程按 {@code expireDue()} 返回值
     * 累加；集群由状态机应用侧按实际释放逐条累加）。
     *
     * @param n 本次释放条目数（{@code >= 0}；0 为空操作）
     */
    public void recordLeaseExpired(int n) {
        if (n > 0) {
            Counter.builder(LEASE_EXPIRED_TOTAL).register(registry).increment(n);
        }
    }

    /**
     * 绑定单机形态 gauge（弱一致回调读数，抓取时实时计算）：
     * {@code locks.held{type}} 两线、{@code waiters}、{@code queue.depth.max}、
     * {@code sessions}。仅单机装配调用（集群形态见 {@link #bindClusterGauges}）。
     *
     * @param core     锁语义核心
     * @param sessions 本节点会话注册表
     */
    public void bindStandaloneGauges(CoreEngine core, ServerSessionRegistry sessions) {
        Gauge.builder(LOCKS_HELD, core, c -> c.stats().heldLocks())
                .tag("type", TYPE_LOCK).register(registry);
        Gauge.builder(LOCKS_HELD, core, c -> c.stats().heldSemaphores())
                .tag("type", TYPE_SEMAPHORE).register(registry);
        Gauge.builder(WAITERS, core, c -> c.stats().totalWaiters()).register(registry);
        Gauge.builder(QUEUE_DEPTH_MAX, core, c -> c.stats().maxQueueDepth()).register(registry);
        bindSessionsGauge(sessions);
    }

    /**
     * 绑定集群形态 gauge：held 读影子表
     * 无锁投影（Leader 上为权威值、非 Leader 上为其回放状态——两侧同为
     * "本节点本地观察"）、waiters/队深读 Leader 内存等待队列（非 Leader
     * 队列恒空）、{@code is_leader} 依 {@link LeaderTracker} 快照当时视图。
     * 全部为抓取时刻弱一致回调读数，不触碰应用锁。
     *
     * @param shadow     复制状态影子表（本副本）
     * @param waitQueue  本节点等待队列（Leader 任期内非空）
     * @param sessions   本节点会话注册表
     * @param nodeId     本节点 id（{@code is_leader} 判定与标签）
     * @param tracker    Leader 提示单源视图
     */
    public void bindClusterGauges(ShadowTable shadow, WaitQueue waitQueue,
                                  ServerSessionRegistry sessions, int nodeId,
                                  LeaderTracker tracker) {
        Gauge.builder(LOCKS_HELD, shadow, s -> s.heldFamilyCounts()[0])
                .tag("type", TYPE_LOCK).register(registry);
        Gauge.builder(LOCKS_HELD, shadow, s -> s.heldFamilyCounts()[1])
                .tag("type", TYPE_SEMAPHORE).register(registry);
        Gauge.builder(WAITERS, waitQueue, WaitQueue::totalWaiters).register(registry);
        Gauge.builder(QUEUE_DEPTH_MAX, waitQueue, WaitQueue::maxQueueDepth).register(registry);
        bindSessionsGauge(sessions);
        bindClusterIsLeader(nodeId, () -> tracker.snapshot().leaderNodeId() == nodeId);
    }

    /**
     * 会话数 gauge（两形态共用）。
     *
     * @param sessions 会话注册表
     */
    private void bindSessionsGauge(ServerSessionRegistry sessions) {
        Gauge.builder(SESSIONS, sessions, ServerSessionRegistry::size).register(registry);
    }

    /**
     * 注册集群角色 gauge（仅集群启用时注册，
     * 单机部署 MUST NOT 出现 {@code openlatch.cluster.is_leader} 线）。
     *
     * @param nodeId   本节点 id（{@code node_id} 标签值）
     * @param isLeader 角色读数（生产接线到 {@code LeaderTracker} 快照判定，
     *                 抓取时刻弱一致求值）
     */
    public void bindClusterIsLeader(int nodeId, BooleanSupplier isLeader) {
        // 保活：gauge 对 obj 弱引用，不持强引用会被 GC 连带摘除仪表（字段注释）。
        gaugeBindKeepAlive.add(isLeader);
        Gauge.builder(CLUSTER_IS_LEADER, isLeader, s -> s.getAsBoolean() ? 1d : 0d)
                .tag("node_id", String.valueOf(nodeId))
                .register(registry);
    }

    /**
     * 计数一次：指标名固定、{@code status} 标签取协议状态码名。
     *
     * @param name   counter 逻辑名
     * @param status 应答状态码
     */
    private void count(String name, StatusCode status) {
        Counter.builder(name).tag("status", status.name()).register(registry).increment();
    }

    /**
     * 应答状态码 → 耗时 {@code result} 标签折算：{@code OK→granted}、
     * {@code QUEUED→queued}、其余（拒绝/非法/提交失败）一律 {@code denied}。
     *
     * @param status 应答协议状态码
     * @return result 标签值
     */
    private static String resultOf(StatusCode status) {
        if (status == StatusCode.OK) {
            return RESULT_GRANTED;
        }
        if (status == StatusCode.QUEUED) {
            return RESULT_QUEUED;
        }
        return RESULT_DENIED;
    }
}
