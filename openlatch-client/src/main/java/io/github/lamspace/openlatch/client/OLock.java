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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * JUC 风格的同步锁句柄（详设 §6.3）。
 *
 * <p><b>语义约定</b>：
 * <ul>
 *   <li>{@link #lock()} 等价于以等待总超时兜底的限时获取；到时未授予以
 *       {@link LockAcquisitionTimeoutException} 结束——<b>不存在无限阻塞路径</b>
 *       （概要设计 §4.2）；</li>
 *   <li>重入计数由服务端维护：客户端每次 {@link #unlock()} 发送一次释放请求，
 *       服务端计数归零时锁才真正释放；</li>
 *   <li>{@link #unlock()} 仅持锁线程可调，非持锁线程调用抛
 *       {@link IllegalMonitorStateException}（与 JUC 一致），不产生网络请求；</li>
 *   <li>锁可能在持有期间丢失（租约失效、断连），丢失经
 *       {@link #onLockLost(LockLostListener)} 与全局监听通知；持有临界区
 *       的业务代码应自行评估丢失后果。</li>
 * </ul>
 *
 * <p><b>租约与续租</b>：锁在服务端以租约存续；授予后由客户端看门狗以
 * {@code grantedLeaseMs/3} 周期自动续租，连续 2 次续租超时判定失锁；
 * 断连时按失锁时刻定时裁决（详设 §6.2/§6.6）。{@code lock()} 的持有语义
 * 以客户端进程存活与连接可恢复为前提——不存在 JUC 式的无限持有，
 * 租约最终到期即由服务端回收。
 *
 * <p><b>线程模型</b>：阻塞等待通过内部 future 的限时 {@code get} 实现，
 * 调用线程不持有客户端内部锁，无死锁路径。
 */
public interface OLock {

    /**
     * 锁键。
     *
     * @return 锁键
     */
    String key();

    /**
     * 获取锁，等待时长受客户端等待总超时兜底（默认 30s）。
     * 到时未授予以 {@link LockAcquisitionTimeoutException} 结束。
     *
     * @throws InterruptedException 等待被中断
     * @throws LockAcquisitionTimeoutException 等待总超时到达仍未授予
     * @throws ServerUnavailableException 连接未处于可用状态（含等待期间断连）
     * @throws OpenLatchException       服务端返回错误码或传输失败
     */
    void lock() throws InterruptedException;

    /**
     * 立即式尝试获取：无快路径（锁被占用，或虽无持有者但等待队列非空——
     * 队首已通知、待重发窗口）时快速返回 {@code false}，不排队。
     *
     * @return 授予返回 {@code true}；被拒返回 {@code false}
     * @throws ServerUnavailableException 连接未处于可用状态
     * @throws OpenLatchException       服务端返回除拒绝外的错误码或传输失败
     */
    boolean tryLock();

    /**
     * 限时尝试获取：到时未授予返回 {@code false}（而非抛异常）。
     *
     * @param waitTime 等待时长，非负
     * @param unit     时长单位
     * @return 授予返回 {@code true}；到时未授予返回 {@code false}
     * @throws InterruptedException 等待被中断
     * @throws IllegalArgumentException waitTime 为负
     * @throws ServerUnavailableException 连接未处于可用状态（含等待期间断连）
     * @throws OpenLatchException       服务端返回错误码或传输失败
     */
    boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException;

    /**
     * 释放锁。仅持锁线程可调；每次调用发送一次释放请求，服务端可重入
     * 计数归零时锁才真正释放。
     *
     * @throws IllegalMonitorStateException 当前线程未持有该锁
     * @throws OpenLatchException           释放请求失败或超时（锁状态未确认）
     */
    void unlock();

    /**
     * 当前线程是否持有该锁。
     *
     * @return 持有返回 {@code true}
     */
    boolean isHeldByCurrentThread();

    /**
     * 登记单锁维度的锁丢失监听。监听器按锁键归属：该键锁完全释放
     * （服务端计数归零且本地无人重持）后登记被丢弃，之后重新获取并
     * 丢锁时旧监听器不触发，需重新注册（详设 §6.3，design D4）。
     *
     * @param listener 监听器
     */
    void onLockLost(LockLostListener listener);

    /**
     * 异步获取锁（排队式，受等待总超时兜底）。
     *
     * @return 授予结果 future；超时以 {@link LockAcquisitionTimeoutException}、
     *         连接不可用以 {@link ServerUnavailableException}、服务端错误码或
     *         传输失败以 {@link OpenLatchException} 完成失败
     */
    CompletableFuture<LockGrant> lockAsync();

    /**
     * 异步限时尝试获取。
     *
     * @param waitTime 等待时长，非负
     * @param unit     时长单位
     * @return 结果 future：授予 {@code true}，到时未授予或被拒 {@code false}；
     *         其余失败以对应异常完成
     * @throws IllegalArgumentException waitTime 折算毫秒小于 -1（经参数校验同步
     *         抛出）；恰为 -1 毫秒时转为排队式、受等待总超时兜底
     */
    CompletableFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit);
}
