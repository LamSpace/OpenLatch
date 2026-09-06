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
 * 屏障等待结果。
 *
 * @param outcome      结果：{@link Outcome#GRANTED} 表示屏障已归零（立即通过）；
 *                   {@link Outcome#QUEUED} 表示挂起等待归零广播；其余为拒绝原因
 * @param queuePosition 挂起时的队列位次（1 起，仅 QUEUED 有意义）
 */
public record LatchAwaitResult(Outcome outcome, int queuePosition) {
}
