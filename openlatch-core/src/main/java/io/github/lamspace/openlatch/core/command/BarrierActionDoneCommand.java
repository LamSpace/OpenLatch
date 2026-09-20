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
 * 循环屏障动作了结命令（执行者回报 barrierAction 已完成）。
 *
 * <p>仅该世代指定执行者的回报被受理（会话身份匹配）；生效后该世代
 * 定型合拢了结、世代号回卷、其余等待者进入放行通知；重复回报
 * MUST 幂等成功。
 *
 * @param sessionId  发起回报的会话（MUST 为该世代动作挂账的执行者会话）
 * @param key        屏障键
 * @param generation 执行者持有的世代号（到场应答回显值）
 */
public record BarrierActionDoneCommand(long sessionId, String key, long generation) {

    /**
     * 紧凑构造器：拒绝非正世代号。
     */
    public BarrierActionDoneCommand {
        if (generation < 0) {
            throw new IllegalArgumentException("generation must be >= 0");
        }
    }
}
