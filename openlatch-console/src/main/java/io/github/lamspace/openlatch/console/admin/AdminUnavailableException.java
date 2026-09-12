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

package io.github.lamspace.openlatch.console.admin;

/**
 * 管理通道不可用异常：控制台侧对"节点查询失败"的单一收口
 * 类型——Web 层按 {@link Kind} 分流降级呈现（认证失败横幅 vs 不可达标注）。
 *
 * <p>非受检：页面装配路径上每个节点查询都是"尽力而为 + 局部降级"，
 * 受检异常会把降级逻辑撕成样板（只读容错取向）。
 */
public class AdminUnavailableException extends RuntimeException {

    /** 失败类别（决定页面降级形态）。 */
    public enum Kind {
        /** 连接层不可达（进程停止、拒绝连接、握手失败）。 */
        UNREACHABLE,
        /** 请求超时（连接活着但应答未在预算内到达）。 */
        TIMEOUT,
        /** 管理认证被拒（令牌不符或服务端未配置令牌；已进退避窗）。 */
        AUTH
    }

    /** 失败类别。 */
    private final Kind kind;

    /**
     * 构造异常。
     *
     * @param kind  失败类别
     * @param message 面向运维的中文描述（MUST NOT 含令牌内容或比对细节）
     * @param cause 底层异常，可为 {@code null}
     */
    public AdminUnavailableException(Kind kind, String message, Throwable cause) {
        super(message, cause);
        this.kind = kind;
    }

    /**
     * 失败类别。
     *
     * @return kind
     */
    public Kind kind() {
        return kind;
    }
}
