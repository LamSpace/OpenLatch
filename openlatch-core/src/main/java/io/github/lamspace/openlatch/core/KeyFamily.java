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
 * key 状态条目的家族判别。key 由首次创建它的请求定型家族（与既有的
 * "可重入性由首次请求定型"约定同构），条目存续期内不变；跨家族的
 * 请求/操作 MUST 被判定为类型不匹配（{@link io.github.lamspace.openlatch.core.result.Outcome#REJECT_TYPE_MISMATCH}）
 * 而拒绝，MUST NOT 改变条目状态。
 *
 * <p><b>家族边界</b>：
 * <ul>
 *   <li>{@link #LOCK}——互斥/读写锁家族（{@link LockType} 的
 *       REENTRANT/SIMPLE/READ/WRITE/FAIR 全部落此家族，由 {@code LockEntry} 承载）；</li>
 *   <li>{@link #SEMAPHORE}——许可门闸家族（{@code SemaphoreEntry} 承载，Phase 3 T1 引入）；</li>
 *   <li>{@link #LATCH}——倒计数屏障家族（{@code LatchEntry} 承载，Phase 3 T1 引入）。</li>
 * </ul>
 *
 * <p><b>判定归属</b>：{@link LockType} 到家族的映射由 core 门面
 * （{@code CoreEngine}）在分派处完成，条目实现不自判——{@code LockEntry}
 * 恒报 {@link #LOCK}，其内部的类型差异（读/写/重入性）不跨家族边界。
 */
public enum KeyFamily {

    /** 互斥/读写锁家族（{@code LockEntry}）。 */
    LOCK,

    /** 许可门闸家族（{@code SemaphoreEntry}，Phase 3 T1）。 */
    SEMAPHORE,

    /** 倒计数屏障家族（{@code LatchEntry}，Phase 3 T1）。 */
    LATCH
}
