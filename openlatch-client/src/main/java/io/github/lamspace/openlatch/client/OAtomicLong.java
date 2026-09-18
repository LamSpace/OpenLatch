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

import java.util.function.LongBinaryOperator;

/**
 * 跨进程共享的原子 long——{@link java.util.concurrent.atomic.AtomicLong} 的
 * 分布式对应物：值存于服务端复制状态，全部客户端句柄读写同一条目。
 * 经 {@link OpenLatchClient#newAtomicLong(String)} 创建。
 *
 * <p><b>语义增强（相对 JDK）</b>：
 * <ul>
 *   <li>每 key 单调版本戳：每次成功写恰 +1（{@link #getStamped()}/{@link #getVersion()}
 *       可读）；{@link #compareAndSetStamped(long, long, long)} 提供值+版本双判定
 *       的 ABA-free 形态（{@link java.util.concurrent.atomic.AtomicStampedReference}
 *       对应物）。{@link #compareAndSet(long, long)} 仍是值判定——ABA 风险存在，
 *       争用激烈或值可回环的场景 MUST 用 stamped 形态。</li>
 *   <li>线性一致：操作以服务端多数派提交定序，跨客户端读写全序可见。</li>
 * </ul>
 *
 * <p><b>语义降级（相对 JDK，务必知悉）</b>：
 * <ul>
 *   <li><b>每操作一次网络往返</b>：非本地原子的免费内存语义；热键高频操作
 *       延迟以 RTT 计。写操作应答自带 {@code (value, version)} 读数，读后写
 *       模式应合并为单条 CAS 省一次读。</li>
 *   <li><b>超时即效果不确定</b>：{@code addAndGet}/{@code incrementAndGet} 在请求
 *       超时后，服务端可能已应用（SDK 以同序号自动重发兜底去重；重发窗口内
 *       会话切换时放弃重发）。不确定窗口以 {@link #getStamped()} 复核，MUST NOT
 *       盲目重发新序号的增量。</li>
 *   <li><b>无中断回滚</b>：值不绑定会话与线程——创建者进程死亡、会话超时均
 *       不回滚、不清零；值的生命周期是 key 的生命周期。</li>
 *   <li><b>条目不回收</b>：key 一经写入常驻服务端（无删除语义），动态 key 名
 *       场景注意基数治理。</li>
 *   <li><b>{@link #accumulateAndGet(long, LongBinaryOperator)}</b> 为客户端
 *       版本戳 CAS 循环（服务端不执行任意代码），有重试界限——超界抛
 *       {@link OpenLatchException}，JDK 形态永不抛。</li>
 * </ul>
 *
 * <p><b>线程模型</b>：句柄可被多线程并发使用。同一 key 的写操作在本客户端内
 * 互斥串行（在途去重保证），跨客户端并发的串行性由服务端定序承载。
 * 全部方法声明 {@code InterruptedException}（本地等待可中断，中断标志被复位）；
 * 失败以非受检 {@link OpenLatchException}/{@link OpenLatchTimeoutException} 表达。
 * 本接口无租约、无看门狗、不触发 {@link LockLostListener}。
 */
public interface OAtomicLong {

    /**
     * 值与版本戳的不可变读数对（{@link #getStamped()} 产物）。
     *
     * @param value   当前值
     * @param version 版本戳（服务端每成功写恰 +1）
     */
    record Stamped(long value, long version) { }

    /**
     * 本原子变量的 key（服务端条目定位，与工厂入参一致）。
     *
     * @return key
     */
    String key();

    /**
     * 读当前值（一次网络往返；不推进版本戳，key 不存在时回 0 且不建条目）。
     *
     * @return 当前值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    long get() throws InterruptedException;

    /**
     * 读值与版本戳（单往返原子读数）。
     *
     * @return (value, version) 读数对
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    Stamped getStamped() throws InterruptedException;

    /**
     * 读版本戳（等价 {@code getStamped().version()}，单往返）。
     *
     * @return 当前版本戳
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    long getVersion() throws InterruptedException;

    /**
     * 置新值（一次网络往返，返回旧值语义见 {@link #getAndSet(long)}）。
     *
     * @param newValue 新值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    void set(long newValue) throws InterruptedException;

    /**
     * 置新值并返回旧值。
     *
     * @param newValue 新值
     * @return 置位前的旧值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    long getAndSet(long newValue) throws InterruptedException;

    /**
     * 原子加一并返回新值（服务端单往返落值，非本地 CAS 循环）。
     *
     * @return 加一后的新值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时（效果不确定窗口，见类注释）
     */
    long incrementAndGet() throws InterruptedException;

    /**
     * 原子加 {@code delta} 并返回新值（long 域溢出按二进制补码 wrap，与 JDK 一致）。
     *
     * @param delta 增量（可为负）
     * @return 加完后的新值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时（效果不确定窗口，见类注释）
     */
    long addAndGet(long delta) throws InterruptedException;

    /**
     * 值 CAS：当前值等于 {@code expected} 时落 {@code update}。ABA 风险存在
     * （值回环不被察觉）；需 ABA-free 语义用 {@link #compareAndSetStamped(long, long, long)}。
     *
     * @param expected 期望值
     * @param update   更新值
     * @return 落值返回 {@code true}；值不符返回 {@code false}（非错误）
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    boolean compareAndSet(long expected, long update) throws InterruptedException;

    /**
     * 值+版本双判定 CAS（ABA-free）：仅当当前值等于 {@code expectedValue}
     * 且版本戳等于 {@code expectedVersion} 时落 {@code update}。
     *
     * @param expectedValue   期望值
     * @param expectedVersion 期望版本戳（{@code >= 1}）
     * @param update          更新值
     * @return 落值返回 {@code true}；任一不符返回 {@code false}（非错误）
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝或传输失败（不可重试类）
     * @throws OpenLatchTimeoutException 请求超时
     */
    boolean compareAndSetStamped(long expectedValue, long expectedVersion, long update)
            throws InterruptedException;

    /**
     * 以二元运算累积并返回新值——客户端版本戳 CAS 循环（运算在本地执行，
     * 服务端不接收函数）。争用下循环重试，超出重试界限抛
     * {@link OpenLatchException}（JDK 形态必然终结，此处为已声明降级）。
     *
     * @param x                  累积参数
     * @param accumulatorFunction 累积运算（当前值 × x → 新值）
     * @return 累积后的新值
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        服务端拒绝、传输失败或争用超界限
     * @throws OpenLatchTimeoutException 请求超时
     */
    long accumulateAndGet(long x, LongBinaryOperator accumulatorFunction)
            throws InterruptedException;
}
