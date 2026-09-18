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

/**
 * 跨进程共享的原子 boolean——{@link java.util.concurrent.atomic.AtomicBoolean}
 * 的分布式对应物：适合"一次性开关/完成标志"协调。值域 {false, true}
 * （线路 0/1），缺省初值 false。经 {@link OpenLatchClient#newAtomicBoolean(String)}
 * 创建。
 *
 * <p>并发保证与语义降级/增强清单与 {@link OAtomicLong} 逐项一致（RTT、
 * 超时不确定窗、ABA 仅 stamped 消除、无归属回滚、条目常驻）；
 * 无累积运算与加减操作（布尔域不适用）。
 *
 * <p><b>典型用法</b>：跨进程"首到者获胜"的初始化协调——
 * {@code compareAndSet(false, true)} 成功者即抢到唯一执行权，且
 * {@link #getStamped()} 的版本戳记录了第几次翻转，可区分"翻转后又翻回"的
 * ABA 场景（stamped 形态）。
 */
public interface OAtomicBoolean {

    /**
     * 值与版本戳的不可变读数对。
     *
     * @param value   当前值
     * @param version 版本戳（服务端每成功写恰 +1）
     */
    record Stamped(boolean value, long version) { }

    /**
     * 本原子变量的 key。
     *
     * @return key
     */
    String key();

    /**
     * 读当前值（key 不存在回 {@code false} 且不建条目）。
     *
     * @return 当前值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    boolean get() throws InterruptedException;

    /**
     * 读值与版本戳。
     *
     * @return (value, version) 读数对
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    Stamped getStamped() throws InterruptedException;

    /**
     * 读版本戳（翻转次数的单调计数）。
     *
     * @return 当前版本戳
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    long getVersion() throws InterruptedException;

    /**
     * 置值（无条件写）。
     *
     * @param newValue 新值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    void set(boolean newValue) throws InterruptedException;

    /**
     * 置值并返回旧值。
     *
     * @param newValue 新值
     * @return 置位前的旧值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    boolean getAndSet(boolean newValue) throws InterruptedException;

    /**
     * 值 CAS：当前值等于 {@code expected} 时落 {@code update}。
     *
     * @param expected 期望值
     * @param update   新值
     * @return 落值返回 {@code true}；值不符返回 {@code false}（非错误）
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    boolean compareAndSet(boolean expected, boolean update) throws InterruptedException;

    /**
     * 值+版本双判定 CAS（ABA-free：能区分"翻转后翻回"与"从未变过"）。
     *
     * @param expectedValue   期望值
     * @param expectedVersion 期望版本戳（{@code >= 1}）
     * @param update          新值
     * @return 落值返回 {@code true}
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    boolean compareAndSetStamped(boolean expectedValue, long expectedVersion, boolean update)
            throws InterruptedException;
}
