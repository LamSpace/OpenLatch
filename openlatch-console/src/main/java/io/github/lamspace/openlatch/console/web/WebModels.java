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

package io.github.lamspace.openlatch.console.web;

import io.github.lamspace.openlatch.console.admin.AdminUnavailableException;

/**
 * 控制台页面视图模型（数据载体，无行为；模板经 Thymeleaf 属性导航消费）。
 */
public final class WebModels {

    /** 工具类不可实例化。 */
    private WebModels() {
    }

    /**
     * 单节点单查询的尽力而为结果（任一节点故障 MUST NOT 阻断对其余
     * 节点的呈现）：失败时 {@code value} 为 {@code null}、错误分类可见。
     *
     * @param display     节点展示名（host:port）
     * @param ok          查询是否成功
     * @param authFailure 是否管理认证被拒（页面据此打认证横幅）
     * @param error       面向运维的中文错误描述（成功为 {@code null}）
     * @param value       应答载荷（失败为 {@code null}）
     * @param <T>         应答类型
     */
    public record NodeResult<T>(String display, boolean ok, boolean authFailure,
                                String error, T value) {

        /**
         * 成功形态。
         *
         * @param display 节点展示名
         * @param value   应答
         * @param <V>     应答类型
         * @return 结果
         */
        public static <V> NodeResult<V> ok(String display, V value) {
            return new NodeResult<>(display, true, false, null, value);
        }

        /**
         * 失败形态（异常分类映射到页面降级）。
         *
         * @param display 节点展示名
         * @param cause   底层异常
         * @param <V>     应答类型
         * @return 结果
         */
        public static <V> NodeResult<V> fail(String display, Throwable cause) {
            boolean auth = cause instanceof AdminUnavailableException aue
                    && aue.kind() == AdminUnavailableException.Kind.AUTH;
            String text = cause instanceof AdminUnavailableException
                    ? cause.getMessage() : "节点查询异常: " + cause;
            return new NodeResult<>(display, false, auth, text, null);
        }
    }

    /**
     * 概览页一节点的 sparkline 数据（SVG points 属性串已按公共画布
     * 160×36 归一化；样本新→旧序绘制为旧→新）。
     *
     * @param ok            本轮是否拿到指标（false=曲线区降级标注）
     * @param heldPoints    持有线点集
     * @param waitersPoints 等待有线集
     * @param sessionsPoints 会话线点集
     * @param latestHeld    最近 held 读数（降级时为空串）
     * @param latestWaiters 最近 waiters 读数
     * @param latestSessions 最近 sessions 读数
     */
    public record Spark(boolean ok, String heldPoints, String waitersPoints,
                        String sessionsPoints, String latestHeld, String latestWaiters,
                        String latestSessions) {

        /**
         * 指标不可用形态（曲线区整体降级）。
         *
         * @return 降级 spark
         */
        public static Spark unavailable() {
            return new Spark(false, "", "", "", "", "", "");
        }
    }
}
