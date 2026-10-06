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
 * await 折叠释放半程的应用结果（v9，集群应用点专用）。释放本身是复制态迁移，
 * 结果仅二元：守卫结论 + 是否发生释放。
 *
 * @param outcome  守卫结果——{@link Outcome#GRANTED}=守卫通过（含无条目/非持有
 *                 的零操作幂等，重放照常）；其余为拒绝（会话失效、key 非法、
 *                 家族/形态不匹配）
 * @param released true=本次应用中 (会话,线程) 恰为写侧持有归属，重入计数一步
 *                 清零并清除租约（Leader 据此登记唤醒链与改写回执）；
 *                 false=无条目/非持有的零操作（重放或重挂幂等形态）
 */
public record AwaitReleaseResult(Outcome outcome, boolean released) {
}
