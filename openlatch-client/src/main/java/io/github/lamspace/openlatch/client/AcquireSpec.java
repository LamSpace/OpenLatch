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

import java.util.Objects;

/**
 * 异步获取请求参数（{@code acquireAsync} 入参）。
 *
 * <p><b>{@code waitMs} 语义</b>：
 * <ul>
 *   <li>{@code 0}：立即式——无快路径（锁被占用，或虽无持有者但等待队列非空）
 *       时直接拒绝（{@code DENIED}），不排队；</li>
 *   <li>{@code -1}：排队等待，受客户端 {@code defaultWaitTimeout} 兜底；</li>
 *   <li>{@code >0}：限时等待该毫秒数，由客户端本地计时（对服务端等价于排队）。</li>
 * </ul>
 *
 * @param key      锁键，非空
 * @param lockType 锁类型
 * @param threadId 申请线程标识，与 {@code sessionId} 共同构成锁归属
 * @param leaseMs  期望租约（毫秒），0 表示使用服务端默认值
 * @param waitMs   等待模式（毫秒），取值见上
 * @param permits  请求许可数（仅 {@link LockType#SEMAPHORE} 消费，{@code >= 1}；
 *                 锁类型请求携带 {@code 1} 与缺省等价）
 * @param permitsTotal Semaphore 许可总量断言：{@code > 0} 建条目定型/既有条目
 *                 校验匹配；{@code 0} 为纯加入不主张；锁类型请求 MUST 为 {@code 0}
 */
public record AcquireSpec(String key, LockType lockType, long threadId, long leaseMs, long waitMs,
        int permits, int permitsTotal) {

    /**
     * 锁家族便捷构造：许可参数取缺省
     * （{@code permits = 1}、{@code permitsTotal = 0}）。
     *
     * @param key      锁键
     * @param lockType 锁类型
     * @param threadId 申请线程标识
     * @param leaseMs  期望租约（毫秒）
     * @param waitMs   等待模式（毫秒）
     */
    public AcquireSpec(String key, LockType lockType, long threadId, long leaseMs, long waitMs) {
        this(key, lockType, threadId, leaseMs, waitMs, 1, 0);
    }

    /**
     * 紧凑构造器：校验锁键非空、锁类型非空、租约非负、等待模式不小于 -1
     * （{@code -1} 合法，表示排队式）；许可数非负、总量断言非负
     * （{@code permits == 0} 按 {@code 1} 归一）。
     */
    public AcquireSpec {
        Objects.requireNonNull(key, "key must not be null");
        Objects.requireNonNull(lockType, "lockType must not be null");
        if (leaseMs < 0) {
            throw new IllegalArgumentException("leaseMs must be >= 0");
        }
        if (waitMs < -1) {
            throw new IllegalArgumentException("waitMs must be >= -1");
        }
        if (permits < 0 || permitsTotal < 0) {
            throw new IllegalArgumentException("permits and permitsTotal must be >= 0");
        }
        if (permits == 0) {
            permits = 1;
        }
    }
}
