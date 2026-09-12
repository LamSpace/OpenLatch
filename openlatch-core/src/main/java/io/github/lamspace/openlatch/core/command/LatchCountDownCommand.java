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
 * 屏障倒计数命令。
 *
 * @param sessionId 发起请求的会话（登记为屏障参与者）
 * @param key       屏障键
 * @param count     本次扣减量（{@code >= 0}；{@code 0} 为纯初始化/断言调用）
 * @param total     初始计数定型断言：条目不存在时非零创建、零拒绝；
 *                  条目存在时非零须与定型值一致、零为不主张
 */
public record LatchCountDownCommand(long sessionId, String key, long count, long total) {

    /**
     * 紧凑构造器：拒绝负扣减与负断言。
     */
    public LatchCountDownCommand {
        if (count < 0) {
            throw new IllegalArgumentException("count must be >= 0");
        }
        if (total < 0) {
            throw new IllegalArgumentException("total must be >= 0");
        }
    }
}
