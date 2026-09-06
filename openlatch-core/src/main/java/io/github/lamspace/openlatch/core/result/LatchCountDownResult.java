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

package io.github.lamspace.openlatch.core.result;

/**
 * 屏障倒计数结果。
 *
 * @param outcome   结果：{@link Outcome#GRANTED} 表示扣减/无操作已生效；
 *                  其余为拒绝原因（会话/键/家族/总量断言）
 * @param remaining 生效后的剩余计数（仅 GRANTED 有意义，拒绝时为 0）
 */
public record LatchCountDownResult(Outcome outcome, long remaining) {
}
