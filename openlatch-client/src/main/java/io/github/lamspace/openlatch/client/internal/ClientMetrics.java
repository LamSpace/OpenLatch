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

package io.github.lamspace.openlatch.client.internal;

import io.github.lamspace.openlatch.client.OpenLatchTimeoutException;
import io.github.lamspace.openlatch.client.ServerUnavailableException;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.micrometer.core.instrument.MeterRegistry;

import java.util.concurrent.TimeUnit;

/**
 * 客户端可选指标门面。
 *
 * <p><b>启停语义</b>：构造入参注册表为 {@code null} 即禁用形态（默认）——
 * 全部记录方法首行判空短路，零计数、零额外分配路径，客户端行为与不引入
 * 该特性时完全一致；启用（宿主经 {@code Builder.meterRegistry} 注入）也
 * MUST NOT 改变任何既有行为契约（超时值、重试与同 id 重发、看门狗节奏、
 * 失锁判定）——本类只做旁路观测，无任何返回值参与裁决。
 *
 * <p><b>指标词表</b>（{@code type} 取
 * {@link MessageType} 名，{@code status} 取应答协议状态码名或失败类别
 * {@code TIMEOUT}/{@code UNAVAILABLE}/{@code FAILED}）：
 * <ul>
 *   <li>{@code openlatch.client.requests.total{type,status}}——请求终局计数
 *       （每个登记发出的请求恰一次，超时/断连失败/被重发取代各有类别）；</li>
 *   <li>{@code openlatch.client.request.duration}——请求发出至响应完成/失败
 *       的耗时（无标签汇总，宿主注册表决定其形态）；</li>
 *   <li>{@code openlatch.client.reconnect.total}——重连调度发起次数
 *       （断连后与连接失败后退避重连的发起点；首次建连不计）；</li>
 *   <li>{@code openlatch.client.locks.lost.total}——锁丢失裁决次数
 *       （{@code loseEntry} 簿记原子移除成功即计，同一条目至多一次）。</li>
 * </ul>
 *
 * <p><b>线程模型</b>：记录方法由 EventLoop、定时器、看门狗与用户线程并发
 * 调用；Micrometer 注册表契约线程安全，本类无可变状态。
 */
public final class ClientMetrics {

    /** 请求终局计数，counter，标签 {@code type}/{@code status}。 */
    public static final String REQUESTS_TOTAL = "openlatch.client.requests.total";
    /** 请求耗时，timer（无标签）。 */
    public static final String REQUEST_DURATION = "openlatch.client.request.duration";
    /** 重连发起计数，counter。 */
    public static final String RECONNECT_TOTAL = "openlatch.client.reconnect.total";
    /** 锁丢失计数，counter。 */
    public static final String LOCKS_LOST_TOTAL = "openlatch.client.locks.lost.total";

    /** 禁用形态单例（默认，全部方法判空短路）。 */
    public static final ClientMetrics DISABLED = new ClientMetrics(null);

    /** 宿主注入的注册表；{@code null} 表示禁用。 */
    private final MeterRegistry registry;

    /**
     * 以注册表构造门面。
     *
     * @param registry 宿主度量注册表；{@code null} 即禁用形态
     */
    public ClientMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /**
     * 是否启用（装配点据此决定是否注册观测回调路径）。
     *
     * @return 已注入注册表为 {@code true}
     */
    public boolean enabled() {
        return registry != null;
    }

    /**
     * 记录一个请求的终局（future 完成点回调）：按结果类别计数并记一次
     * 耗时样本。
     *
     * @param type         请求消息类型
     * @param resp         响应信封（异常完成时为 {@code null}）
     * @param err          完成异常（正常响应为 {@code null}）
     * @param elapsedNanos 发出至完成的耗时（纳秒）
     */
    public void recordRequest(MessageType type, Envelope resp, Throwable err, long elapsedNanos) {
        MeterRegistry r = registry;
        if (r == null) {
            return;
        }
        r.counter(REQUESTS_TOTAL, "type", type.name(), "status", statusOf(resp, err))
                .increment();
        r.timer(REQUEST_DURATION).record(elapsedNanos, TimeUnit.NANOSECONDS);
    }

    /**
     * 记录一次重连发起（退避调度点，首连不计）。
     */
    public void recordReconnect() {
        MeterRegistry r = registry;
        if (r != null) {
            r.counter(RECONNECT_TOTAL).increment();
        }
    }

    /**
     * 记录一次锁丢失裁决（{@code loseEntry} 簿记移除成功处）。
     */
    public void recordLockLost() {
        MeterRegistry r = registry;
        if (r != null) {
            r.counter(LOCKS_LOST_TOTAL).increment();
        }
    }

    /**
     * 完成结果 → {@code status} 标签：正常应答取载荷状态码名；超时、
     * 不可用与其余异常分别归 {@code TIMEOUT}/{@code UNAVAILABLE}/
     * {@code FAILED}。
     *
     * @param resp 响应信封（可为 {@code null}）
     * @param err  完成异常（可为 {@code null}）
     * @return 标签值
     */
    private static String statusOf(Envelope resp, Throwable err) {
        if (err instanceof OpenLatchTimeoutException) {
            return "TIMEOUT";
        }
        if (err instanceof ServerUnavailableException) {
            return "UNAVAILABLE";
        }
        if (err != null) {
            return "FAILED";
        }
        return statusCodeOf(resp);
    }

    /**
     * 响应信封 → 载荷状态码名（oneof 逐类型提取；无状态载荷类型回
     * {@code NONE}）。
     *
     * @param resp 响应信封
     * @return 状态码名
     */
    private static String statusCodeOf(Envelope resp) {
        return switch (resp.getType()) {
            case LOCK_ACQUIRE -> resp.getAcquireResponse().getStatus().name();
            case LOCK_RELEASE -> resp.getReleaseResponse().getStatus().name();
            case LEASE_RENEW -> resp.getLeaseRenewResponse().getStatus().name();
            case LATCH_COUNT_DOWN -> resp.getLatchCountDownResponse().getStatus().name();
            case LATCH_AWAIT -> resp.getLatchAwaitResponse().getStatus().name();
            case HELLO -> resp.getHelloResponse().getStatus().name();
            case CLUSTER_VIEW -> resp.getClusterView().getStatus().name();
            default -> "NONE";
        };
    }
}
