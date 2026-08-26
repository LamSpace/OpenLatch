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
 * 锁丢失的原因载体，作为 {@link LockLostListener} 回调参数传递（详设 §6.6）。
 *
 * <p>丢失来源包括：续租收到明确失效错误（凭证不匹配/未持有/会话失效）、
 * 续租连续超时、断连后的失锁时刻到达或重连成功（旧会话已被服务端清理）。
 */
public class LockLostException extends OpenLatchException {

    /**
     * 以消息构造。
     *
     * @param message 丢失原因描述
     */
    public LockLostException(String message) {
        super(message);
    }

    /**
     * 以服务端状态码与消息构造。
     *
     * @param status  触发丢失的服务端状态码
     * @param message 丢失原因描述
     */
    public LockLostException(io.github.lamspace.openlatch.protocol.StatusCode status, String message) {
        super(status, message);
    }
}
