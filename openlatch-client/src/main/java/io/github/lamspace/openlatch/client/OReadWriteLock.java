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
 * 读写锁门面（详设 §6.3）：同一锁键上的读/写两个 {@link OLock} 句柄。
 *
 * <p>读写互斥与严格 FIFO 由服务端裁决（详设 §4.4 规则 5）：
 * 有写持有者或等待队列非空时读者排队，杜绝写者饥饿。
 * Phase 1 不支持持读升级写或持写降级读的特判。
 */
public interface OReadWriteLock {

    /**
     * 锁键。
     *
     * @return 锁键
     */
    String key();

    /**
     * 读锁句柄（共享）。
     *
     * @return 读锁
     */
    OLock readLock();

    /**
     * 写锁句柄（互斥）。
     *
     * @return 写锁
     */
    OLock writeLock();
}
