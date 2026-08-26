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
 *   <li>{@link #lock()} 等价于以待等待总超时兜底的限时获取；到时未授予以
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
     */
    void lock() throws InterruptedException;

    /**
     * 立即式尝试获取：锁被占时快速返回 {@code false}，不排队。
     *
     * @return 授予返回 {@code true}
     */
    boolean tryLock();

    /**
     * 限时尝试获取：到时未授予返回 {@code false}（而非抛异常）。
     *
     * @param waitTime 等待时长
     * @param unit     时长单位
     * @return 授予返回 {@code true}；到时未授予返回 {@code false}
     * @throws InterruptedException 等待被中断
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
     * 登记单锁维度的锁丢失监听。
     *
     * @param listener 监听器
     */
    void onLockLost(LockLostListener listener);

    /**
     * 异步获取锁（排队式，受等待总超时兜底）。
     *
     * @return 授予结果 future；超时以 {@link LockAcquisitionTimeoutException} 失败
     */
    CompletableFuture<LockGrant> lockAsync();

    /**
     * 异步限时尝试获取。
     *
     * @param waitTime 等待时长
     * @param unit     时长单位
     * @return 结果 future：授予 {@code true}，到时未授予 {@code false}
     */
    CompletableFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit);
}
