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
 * 核心引擎限额与租约配置。各默认值与设计说明书 §5.7 对齐。
 *
 * @param defaultLeaseMs      请求未指定租约时使用的默认租约时长（毫秒）
 * @param minLeaseMs          租约时长下限（毫秒）
 * @param maxLeaseMs          租约时长上限（毫秒）
 * @param headReplyTimeoutMs  已通知队首的响应超时，超时未重发则移除（毫秒）
 * @param maxKeyLength        锁键长度上限（UTF-8 字节）
 * @param maxQueueDepthPerKey 单 key 等待队列深度上限
 */
public record CoreConfig(
        long defaultLeaseMs,
        long minLeaseMs,
        long maxLeaseMs,
        long headReplyTimeoutMs,
        int maxKeyLength,
        int maxQueueDepthPerKey) {

    /** 默认租约时长（毫秒）。 */
    public static final long DEFAULT_LEASE_MS = 30_000L;
    /** 租约时长下限（毫秒）。 */
    public static final long MIN_LEASE_MS = 1_000L;
    /** 租约时长上限（毫秒）。 */
    public static final long MAX_LEASE_MS = 3_600_000L;
    /** 已通知队首的响应超时（毫秒）。 */
    public static final long HEAD_REPLY_TIMEOUT_MS = 5_000L;
    /** 锁键长度上限（UTF-8 字节）。 */
    public static final int MAX_KEY_LENGTH = 512;
    /** 单 key 等待队列深度上限。 */
    public static final int MAX_QUEUE_DEPTH_PER_KEY = 4096;

    /** 全默认配置。 */
    public CoreConfig() {
        this(DEFAULT_LEASE_MS, MIN_LEASE_MS, MAX_LEASE_MS,
                HEAD_REPLY_TIMEOUT_MS, MAX_KEY_LENGTH, MAX_QUEUE_DEPTH_PER_KEY);
    }
}
