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

package io.github.lamspace.openlatch.core.command;

/**
 * 释放锁命令。
 *
 * @param sessionId  发起请求的会话
 * @param key        锁键
 * @param leaseToken 租约凭证，须与当前持有匹配
 * @param threadId   发起请求的客户端线程标识
 */
public record ReleaseCommand(
        long sessionId,
        String key,
        long leaseToken,
        long threadId) {
}
