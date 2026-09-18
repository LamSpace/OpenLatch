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

import java.util.function.IntBinaryOperator;

/**
 * {@link OAtomicInteger} 的远程实现——int32 值域，落值由服务端截断
 * （溢出 wrap 与 JDK 一致）。裁决与重试语义同 {@link RemoteAtomicLong}
 * （读经 int 窄化返回；本端运算按 int 域进行）。
 */
final class RemoteAtomicInteger extends RemoteAtomicBase implements OAtomicInteger {

    /** 累积循环重试界限。 */
    private static final int ACCUMULATE_MAX_RETRIES = 64;

    /**
     * 构造远程原子 int 句柄（仅由 {@link OpenLatchClient} 工厂调用）。
     *
     * @param client       所属客户端
     * @param key          原子变量 key
     * @param initialClaim 初值主张（{@code >= 0}，0 不主张）
     */
    RemoteAtomicInteger(OpenLatchClient client, String key, long initialClaim) {
        super(client, key, LockType.LOCK_TYPE_ATOMIC_INTEGER, initialClaim);
    }

    @Override
    public int get() throws InterruptedException {
        return (int) read().value();
    }

    @Override
    public Stamped getStamped() throws InterruptedException {
        Quad q = read();
        return new Stamped((int) q.value(), q.version());
    }

    @Override
    public long getVersion() throws InterruptedException {
        return read().version();
    }

    @Override
    public void set(int newValue) throws InterruptedException {
        write(AtomicOp.ATOMIC_SET, newValue, 0, 0);
    }

    @Override
    public int getAndSet(int newValue) throws InterruptedException {
        return (int) write(AtomicOp.ATOMIC_GET_AND_SET, newValue, 0, 0).oldValue();
    }

    @Override
    public int incrementAndGet() throws InterruptedException {
        return (int) write(AtomicOp.ATOMIC_ADD, 1, 0, 0).value();
    }

    @Override
    public int addAndGet(int delta) throws InterruptedException {
        return (int) write(AtomicOp.ATOMIC_ADD, delta, 0, 0).value();
    }

    @Override
    public boolean compareAndSet(int expected, int update) throws InterruptedException {
        return write(AtomicOp.ATOMIC_CAS, update, expected, 0).applied();
    }

    @Override
    public boolean compareAndSetStamped(int expectedValue, long expectedVersion, int update)
            throws InterruptedException {
        return write(AtomicOp.ATOMIC_CAS_STAMPED, update, expectedValue, expectedVersion)
                .applied();
    }

    @Override
    public int accumulateAndGet(int x, IntBinaryOperator accumulatorFunction)
            throws InterruptedException {
        java.util.Objects.requireNonNull(accumulatorFunction);
        for (int attempt = 0; attempt < ACCUMULATE_MAX_RETRIES; attempt++) {
            Stamped cur = getStamped();
            int next = accumulatorFunction.applyAsInt(cur.value(), x);
            if (compareAndSetStamped(cur.value(), cur.version(), next)) {
                return next;
            }
        }
        throw new OpenLatchException("accumulateAndGet on '" + key()
                + "' exceeded " + ACCUMULATE_MAX_RETRIES + " CAS attempts (contention bound)");
    }
}
