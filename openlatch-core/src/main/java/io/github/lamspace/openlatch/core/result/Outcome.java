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
 * 获取结果。相较设计说明书 §4.2 将 {@code REJECT_KEY} 细分为空/超长两值，
 * 使 server 层无需重校验即可映射到协议 {@code KEY_EMPTY} / {@code KEY_TOO_LONG}。
 */
public enum Outcome {
    /** 授予：携带租约凭证与实际租约。 */
    GRANTED,
    /** 排队：携带队列位次，等待队首通知后重发。 */
    QUEUED,
    /** 拒绝：锁被占用且请求为立即式（不排队）。 */
    DENIED,
    /** 拒绝：锁键为空。 */
    REJECT_KEY_EMPTY,
    /** 拒绝：锁键超长。 */
    REJECT_KEY_TOO_LONG,
    /** 拒绝：该 key 等待队列已满。 */
    REJECT_QUEUE_FULL,
    /** 拒绝：会话不存在或已关闭。 */
    REJECT_SESSION
}
