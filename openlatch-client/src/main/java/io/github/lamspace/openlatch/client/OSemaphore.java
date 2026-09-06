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
 * 分布式信号量（Phase 3 详设 §2.3 / P3-04）：N 个许可的共享资源门闸，
 * 许可获取受租约保护（持有者进程死亡时由服务端到期回收归还，不泄漏）。
 *
 * <p><b>归属与重入</b>：归属为 {@code (会话, 线程)}——同一线程重复获取
 * 按次累加持有许可，释放对称扣减；不同线程各自独立计数。服务端严格 FIFO
 * 排队：仅队首请求在许可足量时获授，小请求不得越位（防大请求饥饿）。
 *
 * <p><b>租约与看门狗</b>：与 {@link OLock} 完全复用——获授后由看门狗自动
 * 续租；持有期间续租失败（进程失联、Leader 切换超窗等）触发锁丢失通知
 * （{@link OpenLatchClient#addLockLostListener}），归还随服务端回收完成。
 *
 * <p><b>方法语义通则</b>：阻塞式 {@code acquire} 受客户端等待兜底超时约束
 * （超时抛 {@link LockAcquisitionTimeoutException}）；{@code tryAcquire}
 * 立即式/限时式被拒或未授返回 {@code false}；未持有时的 {@code release}
 * 抛 {@link IllegalMonitorStateException}。
 *
 * <p><b>实例化</b>：经 {@link OpenLatchClient#newSemaphore(String, int)}
 * （建条目/定型总量）或 {@link OpenLatchClient#newSemaphore(String)}
 * （纯加入不主张总量）创建。句柄无状态，同一 key 可多实例并存。
 */
public interface OSemaphore {

    /**
     * 锁键。
     *
     * @return 信号量键
     */
    String key();

    /**
     * 获取 1 个许可（阻塞直至获授，受等待兜底超时约束）。
     *
     * @throws InterruptedException          等待被中断（若请求随后仍被授予，
     *                                       客户端补偿归还，不泄漏许可）
     * @throws LockAcquisitionTimeoutException 兜底超时仍未获授
     */
    void acquire() throws InterruptedException;

    /**
     * 获取 {@code permits} 个许可（阻塞直至获授，受等待兜底超时约束）。
     *
     * @param permits 请求许可数（{@code >= 1}）
     * @throws InterruptedException          等待被中断（补偿归还同上）
     * @throws LockAcquisitionTimeoutException 兜底超时仍未获授
     * @throws IllegalArgumentException      {@code permits < 1}
     */
    void acquire(int permits) throws InterruptedException;

    /**
     * 立即式获取 1 个许可：无快路径（池内不足或队列非空）直接返回
     * {@code false}，不排队。
     *
     * @return 获授返回 {@code true}
     * @throws InterruptedException 等待被中断（理论上仅桥接窗口）
     */
    boolean tryAcquire() throws InterruptedException;

    /**
     * 立即式获取 {@code permits} 个许可。
     *
     * @param permits 请求许可数（{@code >= 1}）
     * @return 获授返回 {@code true}
     * @throws InterruptedException     等待被中断
     * @throws IllegalArgumentException {@code permits < 1}
     */
    boolean tryAcquire(int permits) throws InterruptedException;

    /**
     * 限时获取 1 个许可：至多等待 {@code timeout}，期间排队并经通知重发。
     *
     * @param timeout 等待时长
     * @param unit    时长单位
     * @return 获授返回 {@code true}；到时未授返回 {@code false}
     * @throws InterruptedException     等待被中断（补偿归还）
     * @throws IllegalArgumentException {@code timeout < 0}
     */
    boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * 限时获取 {@code permits} 个许可。
     *
     * @param permits 请求许可数（{@code >= 1}）
     * @param timeout 等待时长
     * @param unit    时长单位
     * @return 获授返回 {@code true}；到时未授返回 {@code false}
     * @throws InterruptedException     等待被中断（补偿归还）
     * @throws IllegalArgumentException {@code timeout < 0} 或 {@code permits < 1}
     */
    boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException;

    /**
     * 归还 1 个许可。
     *
     * @throws IllegalMonitorStateException 当前线程未持有该信号量许可
     */
    void release();

    /**
     * 归还 {@code permits} 个许可（对称扣减：超过当前线程持有数时服务端
     * 以 {@code INVALID_REQUEST} 拒绝、池零扰动，异常向上传播）。
     *
     * @param permits 归还许可数（{@code >= 1}）
     * @throws IllegalMonitorStateException 当前线程未持有该信号量许可
     */
    void release(int permits);
}
