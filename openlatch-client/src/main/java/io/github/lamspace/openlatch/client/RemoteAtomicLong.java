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

import io.github.lamspace.openlatch.protocol.AtomicOp;
import io.github.lamspace.openlatch.protocol.LockType;

import java.util.function.LongBinaryOperator;

/**
 * {@link OAtomicLong} 的远程实现——每个方法一次往返（写含同序号自动重发），
 * 语义降级/增强声明见 {@link OAtomicLong} 接口注释与
 * {@link RemoteAtomicBase} 的裁决注释。
 *
 * <p><b>accumulateAndGet</b>：读 (value, version) → 本地运算 → 带版本 CAS，
 * 循环至成功或 {@value #ACCUMULATE_MAX_RETRIES} 次界限（超界抛
 * {@link OpenLatchException}，与 JDK 必然终结的差异系已声明降级）。
 */
final class RemoteAtomicLong extends RemoteAtomicBase implements OAtomicLong {

    /** 累积循环重试界限（版本戳 CAS 在激烈争用下的收敛上限）。 */
    private static final int ACCUMULATE_MAX_RETRIES = 64;

    /**
     * 构造远程原子 long 句柄（仅由 {@link OpenLatchClient} 工厂调用）。
     *
     * @param client       所属客户端
     * @param key          原子变量 key
     * @param initialClaim 初值主张（{@code >= 0}，0 不主张）
     */
    RemoteAtomicLong(OpenLatchClient client, String key, long initialClaim) {
        super(client, key, LockType.LOCK_TYPE_ATOMIC_LONG, initialClaim);
    }

    @Override
    public long get() throws InterruptedException {
        return read().value();
    }

    @Override
    public Stamped getStamped() throws InterruptedException {
        Quad q = read();
        return new Stamped(q.value(), q.version());
    }

    @Override
    public long getVersion() throws InterruptedException {
        return read().version();
    }

    @Override
    public void set(long newValue) throws InterruptedException {
        write(AtomicOp.ATOMIC_SET, newValue, 0, 0);
    }

    @Override
    public long getAndSet(long newValue) throws InterruptedException {
        return write(AtomicOp.ATOMIC_GET_AND_SET, newValue, 0, 0).oldValue();
    }

    @Override
    public long incrementAndGet() throws InterruptedException {
        return write(AtomicOp.ATOMIC_ADD, 1, 0, 0).value();
    }

    @Override
    public long addAndGet(long delta) throws InterruptedException {
        return write(AtomicOp.ATOMIC_ADD, delta, 0, 0).value();
    }

    @Override
    public boolean compareAndSet(long expected, long update) throws InterruptedException {
        return write(AtomicOp.ATOMIC_CAS, update, expected, 0).applied();
    }

    @Override
    public boolean compareAndSetStamped(long expectedValue, long expectedVersion, long update)
            throws InterruptedException {
        return write(AtomicOp.ATOMIC_CAS_STAMPED, update, expectedValue, expectedVersion)
                .applied();
    }

    @Override
    public long accumulateAndGet(long x, LongBinaryOperator accumulatorFunction)
            throws InterruptedException {
        java.util.Objects.requireNonNull(accumulatorFunction);
        for (int attempt = 0; attempt < ACCUMULATE_MAX_RETRIES; attempt++) {
            Stamped cur = getStamped();
            long next = accumulatorFunction.applyAsLong(cur.value(), x);
            if (compareAndSetStamped(cur.value(), cur.version(), next)) {
                return next;
            }
        }
        throw new OpenLatchException("accumulateAndGet on '" + key()
                + "' exceeded " + ACCUMULATE_MAX_RETRIES + " CAS attempts (contention bound)");
    }
}
