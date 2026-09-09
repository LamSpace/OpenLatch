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

package io.github.lamspace.openlatch.server.security;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

/**
 * 令牌常量时间比较原语（Phase 3 详设 §5.2/§10.4 P3-16，spec"常量时间比较"）：
 * 业务令牌与管理令牌共用的单一比较实现，保证两处语义永久一致。
 *
 * <p><b>长度侧信道</b>：{@link MessageDigest#isEqual} 仅对<em>等长</em>输入
 * 常量时间——长度不等会提前返回，泄露配置令牌长度。本原语先把两侧补齐至
 * 二者最大长度再做等长比较，杜绝"长度不等早退"分支（代码评审即可断言，
 * 无需运行时计时）。
 *
 * <p><b>null 语义</b>：任一输入为 {@code null} 一律返回 {@code false}（调用方
 * 在门闩/handler 入口先行裁决"缺失即拒"，本方法对 null 不抛异常）。
 */
public final class ConstantTime {

    /** 工具类禁止实例化。 */
    private ConstantTime() {
    }

    /**
     * 呈现令牌与配置令牌是否一致（常量时间，长度差异不早退）。
     *
     * @param presented  呈现的令牌（HELLO/ADMIN 请求携带）
     * @param configured 配置的令牌
     * @return 一致返回 {@code true}；任一为 {@code null} 返回 {@code false}
     */
    public static boolean matches(String presented, String configured) {
        if (presented == null || configured == null) {
            return false;
        }
        byte[] a = presented.getBytes(StandardCharsets.UTF_8);
        byte[] b = configured.getBytes(StandardCharsets.UTF_8);
        int len = Math.max(a.length, b.length);
        byte[] paddedA = new byte[len];
        byte[] paddedB = new byte[len];
        System.arraycopy(a, 0, paddedA, 0, a.length);
        System.arraycopy(b, 0, paddedB, 0, b.length);
        return MessageDigest.isEqual(paddedA, paddedB);
    }
}
