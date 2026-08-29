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

import io.github.lamspace.openlatch.protocol.StatusCode;

/**
 * 客户端运行时异常基类（详设 §6.1）。
 *
 * <p>所有客户端可预期失败均以本类或其子类抛出（或以其完成失败的
 * {@link java.util.concurrent.CompletableFuture}），并尽量携带服务端状态码
 * 以便调用方区分失败原因。超时、断连类本地失败不携带状态码
 * （{@link #status()} 返回 {@code null}）；<b>锁丢失可能携带服务端状态码</b>
 * （{@code INVALID_TOKEN}/{@code NOT_HELD}/{@code SESSION_EXPIRED}，来源为
 * 续租被拒或解锁前发现丢失），仅断连/宽限到期路径为 {@code null}——
 * 调用方不应以 {@code status() == null} 判别"非锁丢失"。
 */
public class OpenLatchException extends RuntimeException {

    /** 服务端状态码；纯本地失败（超时/断连）为 {@code null}，锁丢失可能携带。 */
    private final StatusCode status;

    /**
     * 以消息构造，不携带状态码。
     *
     * @param message 异常消息
     */
    public OpenLatchException(String message) {
        super(message);
        this.status = null;
    }

    /**
     * 以消息与原因构造，不携带状态码。
     *
     * @param message 异常消息
     * @param cause   原因
     */
    public OpenLatchException(String message, Throwable cause) {
        super(message, cause);
        this.status = null;
    }

    /**
     * 以服务端状态码与消息构造。
     *
     * @param status  服务端返回的状态码
     * @param message 异常消息
     */
    public OpenLatchException(StatusCode status, String message) {
        super(message);
        this.status = status;
    }

    /**
     * 服务端状态码。
     *
     * @return 状态码；纯本地失败（超时/断连）为 {@code null}；锁丢失可能携带
     *         服务端状态码（见类注释）
     */
    public StatusCode status() {
        return status;
    }
}
