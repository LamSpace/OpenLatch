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
 * {@link OLock} 的远程实现：全部语义裁决委托服务端，本地仅做归属簿记与
 * 阻塞桥接（详设 §6.3）。
 *
 * <p><b>阻塞桥接</b>：{@code lock}/{{@code tryLock}} 通过获取 future 的限时
 * {@code get} 实现；阻塞上界 = 等待时长 + 请求超时 + 1s 余量，杜绝死等。
 *
 * <p><b>中断处理</b>：等待中被中断时，若获取请求随后仍被服务端授予，
 * 以补偿释放归还，防止静默泄漏。
 *
 * <p><b>解锁错误分支</b>：
 * <ul>
 *   <li>{@code INVALID_TOKEN}/{@code NOT_HELD}：锁在解锁前已丢失（如租约到期）
 *       ——移除本地登记并触发锁丢失通知，方法正常返回（解锁意图已达成）；</li>
 *   <li>请求超时/传输失败：服务端状态未知，本地登记与续租保持不变，
 *       抛出异常告知调用方。</li>
 * </ul>
 */
final class RemoteLock implements OLock {

    /** 阻塞上界的额外余量（毫秒）：覆盖响应写回与线程调度抖动。 */
    private static final long BLOCK_SLACK_MS = 1000;

    /** 所属客户端。 */
    private final OpenLatchClient client;
    /** 锁键。 */
    private final String key;
    /** 锁类型。 */
    private final LockType lockType;

    /**
     * 创建远程锁句柄。仅由 {@link OpenLatchClient} 的工厂方法调用。
     *
     * @param client   所属客户端
     * @param key      锁键
     * @param lockType 锁类型
     */
    RemoteLock(OpenLatchClient client, String key, LockType lockType) {
        this.client = client;
        this.key = key;
        this.lockType = lockType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String key() {
        return key;
    }

    /**
     * {@inheritDoc}
     *
     * <p>实现：以待等待总超时调用限时获取，到时未授予以超时异常结束。
     */
    @Override
    public void lock() throws InterruptedException {
        boolean acquired = tryLock(client.config().defaultWaitTimeout().toMillis(),
                TimeUnit.MILLISECONDS);
        if (!acquired) {
            throw new LockAcquisitionTimeoutException("lock() timed out for '" + key + "'");
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>实现：立即式获取（{@code waitMs = 0}），被拒返回 {@code false}。
     */
    @Override
    public boolean tryLock() {
        try {
            return doTryLock(0);
        } catch (InterruptedException e) {
            // 立即式获取不做阻塞等待，中断仅可能来自 future 桥接，恢复标志并视为失败。
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean tryLock(long waitTime, TimeUnit unit) throws InterruptedException {
        if (waitTime < 0) {
            throw new IllegalArgumentException("waitTime must be >= 0");
        }
        return doTryLock(unit.toMillis(waitTime));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unlock() {
        long threadId = Thread.currentThread().threadId();
        HeldLockRegistry.HeldEntry entry = client.heldLockRegistry().get(key, threadId);
        if (entry == null) {
            throw new IllegalMonitorStateException(
                    "current thread does not hold lock '" + key + "'");
        }
        long boundMs = client.config().requestTimeout().toMillis() + BLOCK_SLACK_MS;
        try {
            client.releaseAsync(key, entry.leaseToken(), threadId).get(boundMs, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof OpenLatchException ole
                    && (ole.status() == StatusCode.INVALID_TOKEN || ole.status() == StatusCode.NOT_HELD)) {
                // 解锁前锁已丢失：停止续租、移除登记并通知，解锁意图视为达成。
                client.watchdog().stop(entry);
                client.heldLockRegistry().remove(key, threadId);
                client.fireLockLost(key, new LockLostException(ole.status(),
                        "lock '" + key + "' lost before unlock: " + ole.status()));
                return;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new OpenLatchException("unlock of '" + key + "' failed", cause);
        } catch (TimeoutException e) {
            throw new OpenLatchTimeoutException("unlock of '" + key + "' timed out");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OpenLatchException("unlock of '" + key + "' interrupted", e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * <p>实现：查询本地持锁簿记中 {@code (key, 当前线程)} 的登记。
     */
    @Override
    public boolean isHeldByCurrentThread() {
        return client.heldLockRegistry().get(key, Thread.currentThread().threadId()) != null;
    }

    /**
     * {@inheritDoc}
     *
     * <p>实现：登记到锁键维度的监听表，锁丢失时与全局监听一并回调。
     */
    @Override
    public void onLockLost(LockLostListener listener) {
        client.addLockLostListener(key, listener);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CompletableFuture<LockGrant> lockAsync() {
        AcquireSpec spec = new AcquireSpec(key, lockType, Thread.currentThread().threadId(), 0, -1);
        return client.acquireAsync(spec);
    }

    /**
     * {@inheritDoc}
     *
     * <p>实现：限时获取的结果映射——授予 {@code true}；等待超时或被拒
     * {@code false}；其余错误原样传播。
     */
    @Override
    public CompletableFuture<Boolean> tryLockAsync(long waitTime, TimeUnit unit) {
        AcquireSpec spec = new AcquireSpec(key, lockType, Thread.currentThread().threadId(),
                0, unit.toMillis(waitTime));
        return client.acquireAsync(spec).handle((grant, err) -> {
            if (grant != null) {
                return Boolean.TRUE;
            }
            Throwable cause = unwrap(err);
            if (cause instanceof LockAcquisitionTimeoutException) {
                return Boolean.FALSE;
            }
            if (cause instanceof OpenLatchException ole && ole.status() == StatusCode.DENIED) {
                return Boolean.FALSE;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new OpenLatchException("tryLockAsync of '" + key + "' failed", cause);
        });
    }

    /**
     * 限时获取的公共路径。
     *
     * @param waitMs 等待时长（毫秒），0 表示立即式
     * @return 授予返回 {@code true}；到时未授予或被拒返回 {@code false}
     * @throws InterruptedException 等待被中断（中断前会挂补偿归还防泄漏）
     */
    private boolean doTryLock(long waitMs) throws InterruptedException {
        long threadId = Thread.currentThread().threadId();
        AcquireSpec spec = new AcquireSpec(key, lockType, threadId, 0, waitMs);
        CompletableFuture<LockGrant> future = client.acquireAsync(spec);
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
            // 防御分支：阻塞上界已含全部超时余量，到达此处按未授予处理。
            return false;
        } catch (InterruptedException e) {
            // 中断离开等待：若请求随后仍被授予，补偿归还防止静默泄漏。
            future.whenComplete((grant, err) -> {
                if (grant != null) {
                    client.releaseAsync(key, grant.leaseToken(), threadId);
                }
            });
            throw e;
        }
    }

    /**
     * 解包 {@link ExecutionException}/{@link CompletionException} 至真实原因。
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
