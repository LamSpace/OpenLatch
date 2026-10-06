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

import java.util.concurrent.TimeUnit;

/**
 * 跨进程延时队列句柄（{@link OBlockingQueue} 的延时形态子接口，对应 JDK
 * {@code java.util.concurrent.DelayQueue} 的协调面形态，协议 v7）：
 * 元素携带相对延迟，到期前对一切消费通道（{@code take/poll/peek/drainTo}）
 * 不可见，到期后按<b>最早到期先出</b>摘取——出队序为 (到期时刻升序, 同到期
 * 到达序)。
 *
 * <p><b>相对 JDK {@code DelayQueue} 的语义清单</b>：
 * <ul>
 *   <li>增强：同到期时刻内保持到达 FIFO（JDK 不承诺同到期顺序）；容量定型
 *       （JDK 无界）；消费跨进程——元素的到期判定在服务端应用点以条目携带
 *       时刻折算，<b>绝对到期时刻随复制日志确定化</b>，Leader 切换与副本
 *       回放不改判；</li>
 *   <li>差异：延迟注入为 {@link #offerDelayed(byte[], long, TimeUnit)}
 *       显式命名——JDK {@code DelayQueue.offer(e, timeout, unit)} 的
 *       timeout 参数语义即"延迟"，与 {@link OBlockingQueue} 族的
 *       "容量等待预算"同形异义，照抄签名必致混读，故改名承载（本方法容量满
 *       立即回 {@code false}，不等候——需要阻塞投递用 {@link OBlockingQueue#put}
 *       后立即再注延迟的场景请改用两次操作或应用层重试）；</li>
 *   <li>差异：未到期元素<b>永不因到期被删除</b>（与 JDK 一致——到期只是
 *       可见性开关），但计入 {@link OBlockingQueue#size()} 驻留口径；</li>
 *   <li>差异：到点唤醒精度为服务端扫描 tick 级（默认 200ms 可配）——
 *       {@code take} 的到点交付可能滞后一个 tick，正确性不依赖精度（消费
 *       终判恒在应用点）。</li>
 * </ul>
 *
 * <p><b>形态互斥</b>：同一 key 的 {@code QUEUE} 与 {@code DELAY_QUEUE} 形态
 * 互斥——以 {@code newBlockingQueue} 定型的 key 上，本接口句柄首操作抛
 * {@link OpenLatchException}（类型不匹配映射），反之亦然（判例原子四形态
 * 同族互斥）。
 */
public interface ODelayQueue extends OBlockingQueue {

    /**
     * 延时注入（立即式 + 延迟）：有空位即入队并记 {@code delay} 后到期；
     * 队满立即回 {@code false}。到期前元素驻留但消费通道不可见。
     *
     * @param element 元素字节（非 {@code null}）
     * @param delay   相对延迟（{@code >= 0}；0 即立即可见）
     * @param unit    时间单位
     * @return 入队成功为 {@code true}；队满为 {@code false}
     * @throws OpenLatchException 服务端显式拒绝（超限/形状/低版本/形态不符）
     */
    boolean offerDelayed(byte[] element, long delay, TimeUnit unit);

    /**
     * UTF-8 字符串便利形态的 {@link #offerDelayed(byte[], long, TimeUnit)}。
     *
     * @param element 元素字符串（非 {@code null}）
     * @param delay   相对延迟（{@code >= 0}）
     * @param unit    时间单位
     * @return 入队成功为 {@code true}；队满为 {@code false}
     * @throws OpenLatchException 服务端显式拒绝
     */
    boolean offerDelayed(String element, long delay, TimeUnit unit);
}
