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

package io.github.lamspace.openlatch.core.session;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 会话登记表：{@code sessionId → 该会话触及的 key 集合}，加速断连清理（设计说明书 §4.7）。
 * 授予/排队时登记 key；{@link #remove} 原子移除会话并返回其触及的 key 集合，
 * 使会话校验（{@link #touchIfPresent}）与清理（{@link #remove}）相互原子互斥。
 */
public final class SessionRegistry {

    /** 构造空会话登记表。 */
    public SessionRegistry() {
    }

    /** sessionId → 该会话触及的 key 集合。 */
    private final ConcurrentHashMap<Long, Set<String>> touchedKeys = new ConcurrentHashMap<>();

    /**
     * 登记新会话。
     *
     * @param sessionId 要登记的会话
     */
    public void register(long sessionId) {
        touchedKeys.putIfAbsent(sessionId, ConcurrentHashMap.newKeySet());
    }

    /**
     * 会话是否存在（快速预检，非权威）。
     *
     * @param sessionId 要查询的会话
     * @return 会话已登记返回 true
     */
    public boolean contains(long sessionId) {
        return touchedKeys.containsKey(sessionId);
    }

    /**
     * 原子地"会话仍存在则登记 key"。与 {@link #remove} 在同一 key 上原子互斥：
     * 返回 {@code false} 表示会话已关闭（调用方必须拒绝该请求），
     * 返回 {@code true} 表示已登记，会话关闭时该 key 必会被清理。
     *
     * @param sessionId 要校验的会话
     * @param key       要登记的锁键
     * @return {@code false} 表示会话已关闭，{@code true} 表示已登记
     */
    public boolean touchIfPresent(long sessionId, String key) {
        return touchedKeys.computeIfPresent(sessionId, (id, set) -> {
            set.add(key);
            return set;
        }) != null;
    }

    /**
     * 原子移除会话并返回其触及的 key 集合；会话不存在返回 {@code null}（幂等）。
     *
     * @param sessionId 要移除的会话
     * @return 该会话触及的 key 集合；会话不存在返回 {@code null}
     */
    public Set<String> remove(long sessionId) {
        return touchedKeys.remove(sessionId);
    }
}
