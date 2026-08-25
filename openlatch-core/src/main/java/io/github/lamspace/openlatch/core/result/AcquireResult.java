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
 * 获取锁结果。
 *
 * @param outcome        获取结果状态
 * @param leaseToken     {@link Outcome#GRANTED} 时有效：解锁与续租凭证
 * @param grantedLeaseMs {@link Outcome#GRANTED} 时实际生效租约时长
 * @param queuePosition  {@link Outcome#QUEUED} 时有效：队列位次（1 起）
 */
public record AcquireResult(
        Outcome outcome,
        long leaseToken,
        long grantedLeaseMs,
        int queuePosition) {
}
