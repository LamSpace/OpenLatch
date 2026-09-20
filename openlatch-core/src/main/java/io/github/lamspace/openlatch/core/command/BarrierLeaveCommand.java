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
 * 循环屏障离场命令（超时离场、本地中断离场、显式破障共用形态）。
 *
 * <p>离场即破障：对当前世代到场记录的摘除 MUST 使该世代即时破障，
 * 全体在队者以破障了结；对已终结世代的离场 MUST 幂等无操作。
 *
 * @param sessionId      发起请求的会话
 * @param key            屏障键
 * @param awaitRequestId 原到场命令的请求 id（{@code > 0}：摘除该在队等待项并破
 *                       其所属当前世代；{@code 0}：无在队身份的纯破障主张——
 *                       破当前世代，条目不存在或当前世代无到场记录时无操作）
 */
public record BarrierLeaveCommand(long sessionId, String key, long awaitRequestId) {

    /**
     * 紧凑构造器：拒绝负的等待项引用。
     */
    public BarrierLeaveCommand {
        if (awaitRequestId < 0) {
            throw new IllegalArgumentException("awaitRequestId must be >= 0");
        }
    }
}
