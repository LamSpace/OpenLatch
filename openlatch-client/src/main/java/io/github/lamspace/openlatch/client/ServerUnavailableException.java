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
 * 连接不可用时抛出：断连瞬间对全部挂起请求的快速失败（详设 §6.2），
 * 以及在连接非 ACTIVE 状态下发起新请求时的即时拒绝。
 *
 * <p>等待中的操作不自动重试，由调用方决定重试策略。
 */
public class ServerUnavailableException extends OpenLatchException {

    /**
     * 以消息构造。
     *
     * @param message 异常消息
     */
    public ServerUnavailableException(String message) {
        super(message);
    }
}
