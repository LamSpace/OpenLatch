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

import io.github.lamspace.openlatch.client.internal.HeldLockRegistry;
import io.github.lamspace.openlatch.protocol.StatusCode;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link OSemaphore} 的远程实现（Phase 3 详设 §2.3 / P3-04）：许可裁决全部
 * 在服务端（{@code SemaphoreEntry}），本地复用 {@link RemoteLock} 同源的
 * 桥接机制——阻塞上界（等待时长 + 请求超时 + 余量）、中断补偿归还、
 * 授予登记与看门狗续租、丢失裁决。
 *
 * <p><b>总量断言</b>：构造携带的 {@code totalPermits} 附着在每次获取请求上
 * （{@code AcquireSpec.permitsTotal}）：key 首建时定型，既有条目上作为
 * 一致性断言；{@code 0} 为纯加入。
 *
 * <p><b>释放语义</b>：部分归还（仍有持有）不触发登记移除——看门狗继续；
 * 全体归还（服务端 {@code fullyReleased}）由 {@code releaseAsync} 统一完成
 * 登记移除与续租停止，与锁路径共用一条簿记管线。
 */
final class RemoteSemaphore implements OSemaphore {

    /** 阻塞上界的额外余量（毫秒），与 {@link RemoteLock} 同值口径。 */
    private static final long BLOCK_SLACK_MS = 1000;

    /** 所属客户端。 */
    private final OpenLatchClient client;
    /** 信号量键。 */
    private final String key;
    /** 许可总量断言（0 = 纯加入不主张）。 */
    private final int totalPermits;

    /**
     * 创建远程信号量句柄。仅由 {@link OpenLatchClient} 工厂调用。
     *
     * @param client       所属客户端
     * @param key          信号量键
     * @param totalPermits 许可总量断言（{@code >= 0}，0 为不主张）
     */
    RemoteSemaphore(OpenLatchClient client, String key, int totalPermits) {
        this.client = client;
        this.key = key;
        this.totalPermits = totalPermits;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public void acquire() throws InterruptedException {
        doAcquire(1);
    }

    @Override
    public void acquire(int permits) throws InterruptedException {
        requirePermits(permits);
        doAcquire(permits);
    }

    @Override
    public boolean tryAcquire() throws InterruptedException {
        return doTryAcquire(1, 0);
    }

    @Override
    public boolean tryAcquire(int permits) throws InterruptedException {
        requirePermits(permits);
        return doTryAcquire(permits, 0);
    }

    @Override
    public boolean tryAcquire(long timeout, TimeUnit unit) throws InterruptedException {
        return doTryAcquire(1, toWaitMs(timeout, unit));
    }

    @Override
    public boolean tryAcquire(int permits, long timeout, TimeUnit unit) throws InterruptedException {
        requirePermits(permits);
        return doTryAcquire(permits, toWaitMs(timeout, unit));
    }

    @Override
    public void release() {
        release(1);
    }

    @Override
    public void release(int permits) {
        requirePermits(permits);
        long threadId = Thread.currentThread().threadId();
        HeldLockRegistry.HeldEntry entry = client.heldLockRegistry().get(key, threadId);
        if (entry == null) {
            throw new IllegalMonitorStateException(
                    "current thread holds no permits of semaphore '" + key + "'");
        }
        long boundMs = client.config().requestTimeout().toMillis() + BLOCK_SLACK_MS;
        try {
            client.releaseAsync(key, entry.leaseToken(), threadId, permits)
                    .get(boundMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof OpenLatchException ole
                    && (ole.status() == StatusCode.INVALID_TOKEN
                            || ole.status() == StatusCode.NOT_HELD)) {
                // 许可在归还前已被回收（如租约到期）：停止续租、移除登记并
                // 通知，归还意图视为达成（与解锁的丢失裁决同语义）。
                client.watchdog().stop(entry);
                client.heldLockRegistry().remove(key, threadId);
                client.fireLockLost(key, new LockLostException(ole.status(),
                        "semaphore '" + key + "' permits lost before release: " + ole.status()));
                return;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new OpenLatchException("release of '" + key + "' failed", cause);
        } catch (TimeoutException e) {
            throw new OpenLatchTimeoutException("release of '" + key + "' timed out");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OpenLatchException("release of '" + key + "' interrupted", e);
        }
    }

    /**
     * 发起携带许可参数的异步获取。
     *
     * @param permits 请求许可数
     * @param waitMs  等待模式（-1 排队、0 立即、&gt;0 限时）
     * @return 授予 future
     */
    private CompletableFuture<LockGrant> acquireAsync(int permits, long waitMs) {
        AcquireSpec spec = new AcquireSpec(key, LockType.SEMAPHORE,
                Thread.currentThread().threadId(), 0, waitMs, permits, totalPermits);
        return client.acquireAsync(spec);
    }

    /**
     * 阻塞获取公共路径：排队式等待兜底超时，超上界未授以
     * {@link LockAcquisitionTimeoutException} 结束；中断离开等待时挂
     * 补偿归还（与 {@code RemoteLock.doTryLock} 同规则）。
     *
     * @param permits 请求许可数
     * @throws InterruptedException 等待被中断
     */
    private void doAcquire(int permits) throws InterruptedException {
        long threadId = Thread.currentThread().threadId();
        CompletableFuture<LockGrant> future = acquireAsync(permits, -1);
        long boundMs = client.config().defaultWaitTimeout().toMillis()
                + client.config().requestTimeout().toMillis() + BLOCK_SLACK_MS;
        try {
            future.get(boundMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new OpenLatchException("acquire of '" + key + "' failed", cause);
        } catch (TimeoutException e) {
            // 兜底超时由 tracker 先行以 LockAcquisitionTimeoutException 完成；
            // 抵达此处属防御窗口，按超时语义结束。
            throw new LockAcquisitionTimeoutException("acquire of '" + key + "' timed out");
        } catch (InterruptedException e) {
            future.whenComplete((grant, err) -> {
                if (grant != null) {
                    client.releaseAsync(key, grant.leaseToken(), threadId, permits);
                }
            });
            throw e;
        }
    }

    /**
     * tryAcquire 公共路径（立即式/限时式合一）：未授（DENIED 或超时）返回
     * {@code false}，其余错误传播，中断补偿归还——逐分支对齐
     * {@code RemoteLock.doTryLock}。
     *
     * @param permits 请求许可数
     * @param waitMs  等待时长（0 立即式）
     * @return 获授返回 {@code true}
     * @throws InterruptedException 等待被中断
     */
    private boolean doTryAcquire(int permits, long waitMs) throws InterruptedException {
        long threadId = Thread.currentThread().threadId();
        CompletableFuture<LockGrant> future = acquireAsync(permits, waitMs);
        long boundMs = waitMs > 0
                ? waitMs + client.config().requestTimeout().toMillis() + BLOCK_SLACK_MS
                : client.config().requestTimeout().toMillis() + BLOCK_SLACK_MS;
        try {
            future.get(boundMs, TimeUnit.MILLISECONDS);
            return true;
        } catch (ExecutionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof LockAcquisitionTimeoutException) {
                return false;
            }
            if (cause instanceof OpenLatchException ole && ole.status() == StatusCode.DENIED) {
                return false;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new OpenLatchException("acquire of '" + key + "' failed", cause);
        } catch (TimeoutException e) {
            return false;
        } catch (InterruptedException e) {
            future.whenComplete((grant, err) -> {
                if (grant != null) {
                    client.releaseAsync(key, grant.leaseToken(), threadId, permits);
                }
            });
            throw e;
        }
    }

    /**
     * 等待时长折算（负值拒绝）。
     *
     * @param timeout 等待时长
     * @param unit    单位
     * @return 毫秒值
     */
    private static long toWaitMs(long timeout, TimeUnit unit) {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must be >= 0");
        }
        return unit.toMillis(timeout);
    }

    /**
     * 许可数校验。
     *
     * @param permits 待校验许可数
     * @throws IllegalArgumentException 小于 1
     */
    private static void requirePermits(int permits) {
        if (permits < 1) {
            throw new IllegalArgumentException("permits must be >= 1");
        }
    }

    /**
     * 解包 {@link ExecutionException}/{@link CompletionException}。
     *
     * @param t 待解包异常
     * @return 真实原因
     */
    private static Throwable unwrap(Throwable t) {
        Throwable cause = t;
        while ((cause instanceof ExecutionException || cause instanceof CompletionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }
}
