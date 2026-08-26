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
 * 单个网络请求（获取/释放/续租/握手）在请求超时时限内未收到响应时抛出
 * （详设 §6.4"每个请求必有超时"）。对应概要设计 §4.3 标准 3：
 * 客户端所有请求路径带超时，无死等。
 */
public class OpenLatchTimeoutException extends OpenLatchException {

    /**
     * 以消息构造。
     *
     * @param message 异常消息，通常携带超时的请求标识
     */
    public OpenLatchTimeoutException(String message) {
        super(message);
    }
}
