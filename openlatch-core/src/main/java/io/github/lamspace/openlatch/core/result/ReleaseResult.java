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

package io.github.lamspace.openlatch.core.result;

/**
 * 释放锁结果。
 *
 * @param status        释放状态
 * @param fullyReleased 持有计数归零（锁完全释放）时为 true
 * @param releasedCount 本次成功释放实际归还的持有计数（锁逐层恒 1，
 *                      Semaphore 为归还许可数；非 {@code OK} 恒 0）——
 *                      集群影子表按此增量镜像回退，与引擎计数严格对称
 */
public record ReleaseResult(ReleaseStatus status, boolean fullyReleased, int releasedCount) {

    /**
     * 无成功归还语义的构造便捷形态（{@code releasedCount = 0}）：
     * 拒绝类结果统一使用。
     *
     * @param status        释放状态
     * @param fullyReleased 是否完全释放
     */
    public ReleaseResult(ReleaseStatus status, boolean fullyReleased) {
        this(status, fullyReleased, 0);
    }
}
