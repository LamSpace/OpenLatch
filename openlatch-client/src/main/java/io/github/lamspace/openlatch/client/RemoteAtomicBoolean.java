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

/**
 * {@link OAtomicBoolean} 的远程实现——线路值 0/1 与 boolean 互转
 * （0=false、1=true），缺省初值 false。裁决与重试语义同
 * {@link RemoteAtomicLong}；无累积运算（布尔域不存在有意义的 accumulation，
 * 需要时用 {@code compareAndSet}/{@code compareAndSetStamped} 显式表达）。
 */
final class RemoteAtomicBoolean extends RemoteAtomicBase implements OAtomicBoolean {

    /**
     * 构造远程原子 boolean 句柄（仅由 {@link OpenLatchClient} 工厂调用）。
     *
     * @param client 所属客户端
     * @param key    原子变量 key
     * @param initialClaim 初值主张（0=false、1=true，0 同时为不主张缺省）
     */
    RemoteAtomicBoolean(OpenLatchClient client, String key, long initialClaim) {
        super(client, key, LockType.LOCK_TYPE_ATOMIC_BOOLEAN, initialClaim);
    }

    @Override
    public boolean get() throws InterruptedException {
        return read().value() != 0;
    }

    @Override
    public Stamped getStamped() throws InterruptedException {
        Quad q = read();
        return new Stamped(q.value() != 0, q.version());
    }

    @Override
    public long getVersion() throws InterruptedException {
        return read().version();
    }

    @Override
    public void set(boolean newValue) throws InterruptedException {
        write(AtomicOp.ATOMIC_SET, newValue ? 1 : 0, 0, 0);
    }

    @Override
    public boolean getAndSet(boolean newValue) throws InterruptedException {
        return write(AtomicOp.ATOMIC_GET_AND_SET, newValue ? 1 : 0, 0, 0).oldValue() != 0;
    }

    @Override
    public boolean compareAndSet(boolean expected, boolean update) throws InterruptedException {
        return write(AtomicOp.ATOMIC_CAS, update ? 1 : 0, expected ? 1 : 0, 0).applied();
    }

    @Override
    public boolean compareAndSetStamped(boolean expectedValue, long expectedVersion, boolean update)
            throws InterruptedException {
        return write(AtomicOp.ATOMIC_CAS_STAMPED, update ? 1 : 0,
                expectedValue ? 1 : 0, expectedVersion).applied();
    }
}
