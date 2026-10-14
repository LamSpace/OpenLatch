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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 惰性、共享、有界的**路由刷新器**：把"当值 Leader 是谁"这件事的重新学习
 * 从每个请求手里收上来，做成一个有节制的共享动作。
 *
 * <p><b>职责</b>：接受"路由可能已陈旧"的触发（非 ACQUIRE 请求被明确零生效拒绝时），
 * 按<b>单飞 + 限频 + 合并</b>的纪律发起一次探测，把探测所得当作**期望路由**交给
 * 发布回调。它不判断"当前路由对不对"，也不读全局路由——那是发布回调与各请求
 * 自己的事（见类级协作契约）。
 *
 * <p><b>为什么要共享而不是逐请求探测</b>：坏窗内的拒绝是**高频**的（一次故障窗
 * 内同一请求可产生上万次重发），逐请求探测会把一次故障放大成探测风暴；而稳态下
 * 又完全没有探测的必要。故取"惰性触发 + 全客户端共享"：稳态零流量，坏窗内
 * 至多 {@code minIntervalMs} 一次。
 *
 * <p><b>纪律</b>：
 * <ul>
 *   <li><b>单飞</b>：同一时刻至多一个探测在飞；在飞期间的触发被<b>合并</b>为一个
 *       待办，在当前探测完成后按限频窗口安排补探（不丢触发，也不叠加风暴）。</li>
 *   <li><b>限频</b>：两次探测的<b>发起</b>间隔不小于 {@code minIntervalMs}。</li>
 *   <li><b>静默降级</b>：探测失败、无当值 Leader、单机形态（由探测实现方判定并返回
 *       {@code null}）一律不发布、不抛出、不计为业务失败——本类不向调用方回传任何
 *       失败，触发方也因此不可能因刷新而改变自身判定。</li>
 * </ul>
 *
 * <p><b>协作契约（与路由取用纪律的分工）</b>：本类只产生"期望路由"，<b>不</b>决定
 * 任何请求是否采用它。在途请求是否改道，由各请求在自己的"路由不可用"判定点上决定；
 * 因此刷新结果不可能把在途请求推入失败——这是与"periodic reconcile 直接改全局路由"
 * 那版设计的根本区别。
 *
 * <p><b>线程模型</b>：{@link #request()} 可从任意请求线程调用；内部状态由
 * {@link AtomicBoolean} 与一个 {@code volatile} 时间戳承载，探测完成回调可能落在
 * 网络/定时器线程。发布回调由探测完成线程调用，实现方须自行保证线程安全。
 */
final class RouteRefresh {

    /** 探测动作：返回当值 Leader 接入地址；无当值 Leader/不支持/失败返回 {@code null}。 */
    interface Probe {

        /**
         * 发起一次探测。
         *
         * @return 完成于探测结果的 future（{@code null} 表示本轮无可用结果）
         */
        CompletableFuture<ClientConfig.SeedAddress> probe();
    }

    /** 发布动作：收到期望路由时的副作用（建/换获取车道）。 */
    interface Publisher {

        /**
         * 发布期望路由。
         *
         * @param target 探测所得的当值 Leader 接入地址（非 {@code null}）
         */
        void publish(ClientConfig.SeedAddress target);
    }

    /** 探测动作。 */
    private final Probe probe;
    /** 发布动作。 */
    private final Publisher publisher;
    /** 限频与补探延时用的共享定时器（客户端既有实例，不新建线程）。 */
    private final HashedWheelTimer timer;
    /** 两次探测发起的最小间隔（毫秒）。 */
    private final long minIntervalMs;

    /** 是否有探测在飞（单飞闸）。 */
    private final AtomicBoolean inFlight = new AtomicBoolean();
    /** 在飞期间是否收到过触发（合并待办）。 */
    private final AtomicBoolean pending = new AtomicBoolean();
    /** 上次发起探测的时刻（限频基准）。 */
    private volatile long lastProbeAtMs;
    /** 关停标志：关停后不再发起探测、不再安排补探。 */
    private volatile boolean closed;

    /**
     * 构造刷新器。
     *
     * @param probe         探测动作
     * @param publisher     发布动作
     * @param timer         共享定时器（补探延时）
     * @param minIntervalMs 两次探测发起的最小间隔（毫秒，&gt; 0）
     */
    RouteRefresh(Probe probe, Publisher publisher, HashedWheelTimer timer, long minIntervalMs) {
        this.probe = probe;
        this.publisher = publisher;
        this.timer = timer;
        this.minIntervalMs = minIntervalMs;
    }

    /**
     * 请求一次路由刷新（触发入口）。
     *
     * <p>调用方语义：**只在自身观察到"明确零生效拒绝"时**调用。本方法立即返回，
     * 不发生任何阻塞、不回传结果、不抛出——触发方不得据此改变自身判定。
     */
    void request() {
        if (closed) {
            return;
        }
        if (inFlight.get()) {
            pending.set(true); // 在飞：合并为补探待办
            return;
        }
        long now = System.currentTimeMillis();
        long sinceLast = now - lastProbeAtMs;
        if (lastProbeAtMs != 0 && sinceLast < minIntervalMs) {
            // 限频窗内：同样合并为补探待办，并安排在窗口结束时刻补探
            pending.set(true);
            scheduleFollowUp(minIntervalMs - sinceLast);
            return;
        }
        start();
    }

    /**
     * 关停刷新器：此后不再发起探测，已安排的补探在到期时自弃。
     */
    void close() {
        closed = true;
    }

    /**
     * 发起一次探测（调用方已确认可发起）。
     */
    private void start() {
        lastProbeAtMs = System.currentTimeMillis();
        inFlight.set(true);
        pending.set(false);
        CompletableFuture<ClientConfig.SeedAddress> f;
        try {
            f = probe.probe();
        } catch (RuntimeException e) {
            // 静默降级：探测动作自身抛出也算"本轮无结果"，绝不上浮
            f = CompletableFuture.completedFuture(null);
        }
        f.whenComplete((target, err) -> {
            inFlight.set(false);
            if (err == null && target != null && !closed) {
                try {
                    publisher.publish(target);
                } catch (RuntimeException ignored) {
                    // 发布失败（如目标不可达的建连失败）由既有改道路径自行降级，
                    // 刷新器不因发布失败改变任何状态
                }
            }
            if (pending.getAndSet(false) && !closed) {
                // 合并的触发：按限频窗口安排补探（不无条件立即补）
                long elapsed = System.currentTimeMillis() - lastProbeAtMs;
                if (elapsed >= minIntervalMs) {
                    start();
                } else {
                    scheduleFollowUp(minIntervalMs - elapsed);
                }
            }
        });
    }

    /**
     * 安排一次延时补探（限频窗口结束后）。
     *
     * @param delayMs 延时（毫秒）
     */
    private void scheduleFollowUp(long delayMs) {
        timer.newTimeout(t -> {
            if (closed) {
                return;
            }
            if (inFlight.get()) {
                return; // 已另有探测在飞：本次补探并入它
            }
            pending.set(false);
            start();
        }, Math.max(1, delayMs), TimeUnit.MILLISECONDS);
    }
}
