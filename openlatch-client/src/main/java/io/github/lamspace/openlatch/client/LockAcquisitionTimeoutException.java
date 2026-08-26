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
 * 获取锁的总等待时限（调用方指定的等待时长或 {@code lock()} 的兜底超时）
 * 到达而仍未被授予时抛出（详设 §6.3）。
 *
 * <p>与 {@link OpenLatchTimeoutException} 的区别：前者是 <b>等待整体</b>
 * 超时（期间可能经历了多次请求/重发），后者是 <b>单个请求</b> 无响应。
 */
public class LockAcquisitionTimeoutException extends OpenLatchException {

    /**
     * 以消息构造。
     *
     * @param message 异常消息，通常携带锁键
     */
    public LockAcquisitionTimeoutException(String message) {
        super(message);
    }
}
