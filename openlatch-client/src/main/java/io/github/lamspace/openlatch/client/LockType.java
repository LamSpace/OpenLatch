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
 * 锁类型（详设 §3.2 协议 {@code LockType} 的客户端公开映射）。
 *
 * <p>归属键统一为 {@code (sessionId, threadId)}。各类型语义由服务端裁决：
 * <ul>
 *   <li>{@link #REENTRANT}：可重入互斥，重入计数由服务端维护；</li>
 *   <li>{@link #SIMPLE}：不可重入互斥，同持有者再次获取将排队等待自身
 *       直至租约到期，队列满则直接拒绝（{@code REJECT_QUEUE_FULL}）；
 *       立即式同归属重复获取直接拒绝（{@code DENIED}）
 *       （详设 §4.4"SimpleLock 的自锁问题"）；</li>
 *   <li>{@link #READ}：共享读锁，多读者并发持有；</li>
 *   <li>{@link #WRITE}：互斥写锁；</li>
 *   <li>{@link #FAIR}：显式公平承诺互斥（v3 起）——语义与 {@link #REENTRANT}
 *       逐项等价，额外携带服务端"授予顺序等于排队顺序"的显式承诺。</li>
 * </ul>
 * Phase 1 不支持持读升级写或持写降级读的特判，一律走通用排队规则。
 * {@code FAIR} 为 v3 类型：客户端以 v3 握手（{@code protocol_version = 3}）
 * 使用方有效，v1/v2 服务端会话上请求该类型会被拒绝。
 */
public enum LockType {
    /** 可重入互斥锁。 */
    REENTRANT(io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_REENTRANT),
    /** 不可重入互斥锁。 */
    SIMPLE(io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_SIMPLE),
    /** 共享读锁。 */
    READ(io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_READ),
    /** 互斥写锁。 */
    WRITE(io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_WRITE),
    /** 显式公平承诺互斥锁（v3，语义等价 {@link #REENTRANT}）。 */
    FAIR(io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_FAIR);

    /** 对应的协议枚举值。 */
    private final io.github.lamspace.openlatch.protocol.LockType wireType;

    /**
     * 绑定协议枚举值。
     *
     * @param wireType 协议枚举值
     */
    LockType(io.github.lamspace.openlatch.protocol.LockType wireType) {
        this.wireType = wireType;
    }

    /**
     * 协议枚举值，出站请求使用。
     *
     * @return 协议锁类型
     */
    public io.github.lamspace.openlatch.protocol.LockType wireType() {
        return wireType;
    }
}
