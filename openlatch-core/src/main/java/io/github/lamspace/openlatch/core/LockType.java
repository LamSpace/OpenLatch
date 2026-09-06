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

package io.github.lamspace.openlatch.core;

/**
 * 锁类型，与协议层 {@code LockType} 一一对应。core 不依赖 protocol，
 * 故在此定义独立枚举，由 server 层做映射。
 *
 * <p><b>兼容性</b>：{@link #READ} 之间相互兼容（可多读者共存）；
 * {@link #READ} 与 {@link #SIMPLE}、{@link #REENTRANT}、{@link #WRITE}、
 * {@link #FAIR} 互斥；后四者相互之间亦互斥（同一时刻至多一个写侧持有者）。
 *
 * <p><b>重入归属</b>：归属由 {@code (sessionId, threadId)} 唯一确定。
 * {@link #SIMPLE} 不可重入（同归属重复获取将排队或拒绝）；
 * {@link #REENTRANT}、{@link #WRITE} 与 {@link #FAIR} 可重入；
 * {@link #READ} 的同归属重复获取按读侧计数重入。
 *
 * <p><b>同 key 同类型约定</b>（与 Redisson 约定一致）：同一 key 应始终
 * 使用一致的锁类型。条目的可重入性由首次请求定型（{@code SIMPLE} 为
 * 不可重入，其余为可重入），建条目后不再变化。
 */
public enum LockType {
    /** 可重入互斥（默认）：同归属可重复获取，持有计数逐层递增、逐层释放。 */
    REENTRANT,
    /** 不可重入互斥：同归属在持有期间重复获取将排队或拒绝。 */
    SIMPLE,
    /** 读锁：读者间共享，与任何写侧互斥；整 key 共用一个租约凭证。 */
    READ,
    /** 写锁：与任何持有者互斥，可重入。 */
    WRITE,
    /**
     * 显式公平承诺互斥（Phase 3 T1）：语义与 {@link #REENTRANT} 逐项等价
     * （同族别名、互通互认），作为公平承诺的 API 标识；授予顺序等于排队
     * 顺序的保证由 FIFO 队列本体承载（详设 §2.2）。
     */
    FAIR,
    /**
     * 许可门闸（Phase 3 T1）：非锁家族类型，仅作请求定型判别——携带此
     * 类型的命令由门面分派至 {@code SemaphoreEntry}（详设 §2.3），不参与
     * 上述互斥矩阵。
     */
    SEMAPHORE
}
