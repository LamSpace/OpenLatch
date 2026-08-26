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
 * 释放/续租的状态。{@link #OK} 表示操作成功（释放成功或续租成功）。
 *
 * <p><b>返回步骤与优先级</b>（释放与续租一致，首个不满足者即为返回值）：
 * 会话校验（{@link #REJECT_SESSION}）→ 该 key 存在条目且有持有者
 * （否则 {@link #NOT_HELD}）→ 凭证匹配（否则 {@link #INVALID_TOKEN}）→
 * 成功为 {@link #OK}。因此：无人持有时即使凭证非法也回 {@link #NOT_HELD}；
 * {@link #INVALID_TOKEN} 仅在"有人持有但凭证不匹配"时返回（典型场景：
 * 租约已到期被回收后重放旧凭证）。释放路径另有一个防御性分支——凭证
 * 匹配但归属不匹配——仍回 {@link #NOT_HELD}（正常路径理论不可达）。
 */
public enum ReleaseStatus {
    /** 操作成功（释放成功或续租成功）。 */
    OK,
    /** 凭证与当前租约不匹配；前提是有持有者，否则先返回 {@link #NOT_HELD}。 */
    INVALID_TOKEN,
    /** 无条目、无持有者，或凭证匹配但归属不匹配（防御性）。 */
    NOT_HELD,
    /** 会话不存在或已关闭，先于一切条目检查。 */
    REJECT_SESSION
}
