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

import java.util.function.IntBinaryOperator;

/**
 * 跨进程共享的原子 int——{@link java.util.concurrent.atomic.AtomicInteger} 的
 * 分布式对应物。值落 int32 域（溢出 wrap，与 JDK 一致）。
 * 经 {@link OpenLatchClient#newAtomicInteger(String)} 创建。
 *
 * <p>并发保证、语义增强（版本戳与 ABA-free 形态）与语义降级清单
 * （RTT、超时不确定窗、无归属回滚、条目常驻、累积运算有界重试）与
 * {@link OAtomicLong} 逐项一致，仅值域为 int；线程模型同 {@link OAtomicLong}。
 * 同一 key 的形态在服务端定型，跨形态操作被拒绝（{@code INVALID_REQUEST}）。
 */
public interface OAtomicInteger {

    /**
     * 值与版本戳的不可变读数对。
     *
     * @param value   当前值（int32 域）
     * @param version 版本戳
     */
    record Stamped(int value, long version) { }

    /**
     * 本原子变量的 key。
     *
     * @return key
     */
    String key();

    /**
     * 读当前值（一次网络往返；key 不存在回 0 不建条目）。
     *
     * @return 当前值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    int get() throws InterruptedException;

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
     * 读版本戳。
     *
     * @return 当前版本戳
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    long getVersion() throws InterruptedException;

    /**
     * 置新值。
     *
     * @param newValue 新值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    void set(int newValue) throws InterruptedException;

    /**
     * 置新值并返回旧值。
     *
     * @param newValue 新值
     * @return 置位前的旧值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    int getAndSet(int newValue) throws InterruptedException;

    /**
     * 原子加一并返回新值。
     *
     * @return 加一后的新值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时（效果不确定窗口）
     */
    int incrementAndGet() throws InterruptedException;

    /**
     * 原子加 {@code delta} 并返回新值（int32 域溢出 wrap）。
     *
     * @param delta 增量
     * @return 加完后的新值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时（效果不确定窗口）
     */
    int addAndGet(int delta) throws InterruptedException;

    /**
     * 值 CAS（ABA 风险见 {@link OAtomicLong#compareAndSet(long, long)}）。
     *
     * @param expected 期望值
     * @param update   更新值
     * @return 落值返回 {@code true}
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    boolean compareAndSet(int expected, int update) throws InterruptedException;

    /**
     * 值+版本双判定 CAS（ABA-free）。
     *
     * @param expectedValue   期望值
     * @param expectedVersion 期望版本戳（{@code >= 1}）
     * @param update          更新值
     * @return 落值返回 {@code true}
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    boolean compareAndSetStamped(int expectedValue, long expectedVersion, int update)
            throws InterruptedException;

    /**
     * 累积运算（客户端版本戳 CAS 循环，有重试界限，同
     * {@link OAtomicLong#accumulateAndGet(long, java.util.function.LongBinaryOperator)}）。
     *
     * @param x                   累积参数
     * @param accumulatorFunction 累积运算
     * @return 累积后的新值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝、传输失败或争用超界限
     * @throws OpenLatchTimeoutException 请求超时
     */
    int accumulateAndGet(int x, IntBinaryOperator accumulatorFunction)
            throws InterruptedException;
}
