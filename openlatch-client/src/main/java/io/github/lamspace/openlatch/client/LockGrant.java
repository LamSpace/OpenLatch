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
 * 锁授予结果（详设 §6.1）：获取成功时由异步接口返回。
 *
 * @param leaseToken     服务端签发的租约凭证，释放与续租的唯一凭据
 * @param grantedLeaseMs 实际生效租约（毫秒），看门狗据此设定续租周期
 */
public record LockGrant(long leaseToken, long grantedLeaseMs) {
}
