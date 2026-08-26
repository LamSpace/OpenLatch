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

package io.github.lamspace.openlatch.client.internal;

import io.github.lamspace.openlatch.client.OpenLatchException;
import io.github.lamspace.openlatch.protocol.StatusCode;

/**
 * 立即式获取被拒（服务端回 {@code DENIED}）的内部标记异常。
 *
 * <p>同步包装层将其映射为 {@code tryLock() == false}；
 * {@code acquireAsync} 将其转换为携带 {@code DENIED} 状态码的
 * {@link OpenLatchException} 上抛。
 */
public final class LockDeniedException extends OpenLatchException {

    /**
     * 以锁键构造。
     *
     * @param key 被拒的锁键
     */
    public LockDeniedException(String key) {
        super(StatusCode.DENIED, "immediate acquire of '" + key + "' denied: lock is held");
    }
}
