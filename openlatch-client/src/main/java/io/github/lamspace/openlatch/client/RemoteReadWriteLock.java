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

/**
 * {@link OReadWriteLock} 的远程实现：同一锁键上读/写两个 {@link RemoteLock}
 * 句柄的组合。仅由 {@link OpenLatchClient} 的工厂方法创建。
 */
final class RemoteReadWriteLock implements OReadWriteLock {

    /** 锁键。 */
    private final String key;
    /** 读锁句柄。 */
    private final OLock readLock;
    /** 写锁句柄。 */
    private final OLock writeLock;

    /**
     * 以两个已构建的句柄组装。
     *
     * @param key       锁键
     * @param readLock  读锁句柄
     * @param writeLock 写锁句柄
     */
    RemoteReadWriteLock(String key, OLock readLock, OLock writeLock) {
        this.key = key;
        this.readLock = readLock;
        this.writeLock = writeLock;
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
     */
    @Override
    public OLock readLock() {
        return readLock;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OLock writeLock() {
        return writeLock;
    }
}
