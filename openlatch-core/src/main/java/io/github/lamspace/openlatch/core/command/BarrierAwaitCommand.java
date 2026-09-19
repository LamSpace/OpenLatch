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
 * 循环屏障到场命令（等待-通知-重发闭环的"到场/重发"单请求形态）。
 *
 * <p>{@code (sessionId, requestId)} 为等待项身份：同身份重发（应答丢失、
 * 通知丢失兜底）MUST NOT 重复计次，按所属世代的了结记录幂等了结。
 *
 * @param sessionId     发起请求的会话
 * @param requestId     会话内请求 id（重发沿用同值以命中幂等去重）
 * @param key           屏障键
 * @param parties       许可数定型断言：条目不存在时非零创建、零拒绝；
 *                      条目存在时非零须与定型值一致、零为不主张
 * @param carriesAction 调用方携带 barrierAction——若本次到场恰为最后
 *                      到场者，其应答标记执行者，动作完成经动作了结命令
 *                      回报后该世代方放行其余等待者
 */
public record BarrierAwaitCommand(long sessionId, long requestId, String key,
        long parties, boolean carriesAction) {

    /**
     * 紧凑构造器：拒绝负许可数主张。
     */
    public BarrierAwaitCommand {
        if (parties < 0) {
            throw new IllegalArgumentException("parties must be >= 0");
        }
    }
}
