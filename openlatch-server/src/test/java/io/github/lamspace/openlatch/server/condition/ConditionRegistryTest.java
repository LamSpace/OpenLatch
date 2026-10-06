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

package io.github.lamspace.openlatch.server.condition;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ConditionRegistry} 判定矩阵单测（v9，Leader 本地条件等待集的纯集合
 * 语义，不依赖集群基座）：幂等重登（含跨条件同身份判重与登记时刻保留）、
 * 跨条件搬运隔离、promoteFirst 排除调用归属/promoteAll 全员+排除自身、
 * LEAVE 幂等只中目标、purgeOwner 按 (会话,线程) 跨 key 收口、removeSession
 * 只触该会话、clear 任期清零、count/totalCount/maxCountCurrent/views 的
 * 口径与到达序。
 */
class ConditionRegistryTest {

    /**
     * 登记并断言新增。
     *
     * @param r     登记表
     * @param sid   会话
     * @param rid   折叠 request_id
     * @param tid   线程
     * @param key   锁键
     * @param cond  条件名
     * @param nowMs 登记时刻
     */
    private static void reg(ConditionRegistry r, long sid, long rid, long tid,
            String key, String cond, long nowMs) {
        assertThat(r.register(sid, rid, tid, key, cond, nowMs)).isTrue();
    }

    @Test
    void reRegisterIsIdempotentAndKeepsOriginalTimestamp() {
        ConditionRegistry r = new ConditionRegistry();
        reg(r, 1, 100, 11, "k", "x", 1_000L);
        // 同 (会话,请求) 重挂：不二次入集、不更新登记时刻（等待时长如实）。
        assertThat(r.register(1, 100, 11, "k", "x", 9_000L)).isFalse();
        assertThat(r.count("k")).isEqualTo(1);
        assertThat(r.views("k").get(0).registeredAtMs()).isEqualTo(1_000L);
        // 跨条件同身份同样判重（在集身份不含条件名——同请求只属一个集）。
        assertThat(r.register(1, 100, 11, "k", "y", 9_000L)).isFalse();
        assertThat(r.count("k")).isEqualTo(1);
        assertThat(r.isRegistered(1, 100, "k")).isTrue();
        assertThat(r.isRegistered(1, 101, "k")).isFalse();
    }

    @Test
    void promoteTouchesOnlyNamedCondition() {
        ConditionRegistry r = new ConditionRegistry();
        reg(r, 1, 100, 11, "k", "x", 1L);
        reg(r, 2, 200, 21, "k", "y", 2L);
        // "x" 的 signal 不搬运 "y" 集的等待项（跨条件隔离）。
        var moved = r.promoteFirst("k", "x", 9, 9);
        assertThat(moved).isPresent();
        assertThat(moved.get().sessionId()).isEqualTo(1);
        assertThat(r.count("k")).isEqualTo(1);
        assertThat(r.views("k").get(0).condition()).isEqualTo("y");
        // 不存在的条件名：空搬运、零扰动。
        assertThat(r.promoteFirst("k", "z", 9, 9)).isEmpty();
        assertThat(r.promoteAll("k", "missing-key-cond", 9, 9)).isEmpty();
        assertThat(r.count("k")).isEqualTo(1);
    }

    @Test
    void promoteFirstSkipsCallerOwner() {
        ConditionRegistry r = new ConditionRegistry();
        // 预检窗内 awaiter 短暂"持有且在集"：自 signal 防御性跳过自身。
        reg(r, 7, 700, 70, "k", "x", 1L);
        assertThat(r.promoteFirst("k", "x", 7, 70)).isEmpty();
        assertThat(r.count("k")).isEqualTo(1);
        reg(r, 8, 800, 80, "k", "x", 2L);
        var moved = r.promoteFirst("k", "x", 7, 70);
        assertThat(moved).isPresent();
        assertThat(moved.get().sessionId()).isEqualTo(8);
        // 仅被搬运项出集；调用归属仍留集（预检窗"持有且在集"的登记不被
        // 自 signal 消费），集非空故不触发级联回收。
        assertThat(r.count("k")).isEqualTo(1);
        assertThat(r.isRegistered(7, 700, "k")).isTrue();
        assertThat(r.isRegistered(8, 800, "k")).isFalse();
    }

    @Test
    void promoteAllDrainsEveryoneExceptCallerInArrivalOrder() {
        ConditionRegistry r = new ConditionRegistry();
        reg(r, 1, 100, 11, "k", "x", 1L);
        reg(r, 7, 700, 70, "k", "x", 2L);
        reg(r, 2, 200, 21, "k", "x", 3L);
        List<ConditionRegistry.Promoted> out = r.promoteAll("k", "x", 7, 70);
        assertThat(out).extracting(ConditionRegistry.Promoted::sessionId)
                .containsExactly(1L, 2L);
        assertThat(out).extracting(ConditionRegistry.Promoted::requestId)
                .containsExactly(100L, 200L);
        assertThat(r.count("k")).isEqualTo(1);
        assertThat(r.views("k").get(0).sessionId()).isEqualTo(7);
    }

    @Test
    void leaveIsIdempotentAndTargetsOnlyRequestedRegistration() {
        ConditionRegistry r = new ConditionRegistry();
        reg(r, 1, 100, 11, "k", "x", 1L);
        reg(r, 1, 101, 12, "k", "x", 2L);
        reg(r, 1, 100, 11, "k2", "x", 3L);
        // 按 (会话,请求) 摘除：只中 rid=100 的登记（跨 key 命中，k 的 101 存续）。
        assertThat(r.leave(1, 100)).isTrue();
        assertThat(r.count("k")).isEqualTo(1);
        assertThat(r.count("k2")).isZero();
        // 幂等：重复 LEAVE 无错误态（返回 false，受理侧恒 OK）。
        assertThat(r.leave(1, 100)).isFalse();
        assertThat(r.leave(9, 999)).isFalse();
    }

    @Test
    void purgeOwnerSpansKeysAndConditionsBySessionThread() {
        ConditionRegistry r = new ConditionRegistry();
        reg(r, 1, 100, 11, "k", "x", 1L);
        reg(r, 1, 101, 11, "k2", "y", 2L);
        reg(r, 1, 102, 12, "k", "x", 3L);
        reg(r, 2, 200, 11, "k", "x", 4L);
        // (会话,线程) 归属收口：跨 key、跨条件全摘；同线程他会话、
        // 同 key 他线程不受触。
        r.purgeOwner(1, 11);
        assertThat(r.totalCount()).isEqualTo(2);
        assertThat(r.isRegistered(1, 100, "k")).isFalse();
        assertThat(r.isRegistered(1, 101, "k2")).isFalse();
        assertThat(r.isRegistered(1, 102, "k")).isTrue();
        assertThat(r.isRegistered(2, 200, "k")).isTrue();
        assertThat(r.count("k2")).isZero();
    }

    @Test
    void removeSessionTouchesOnlyThatSession() {
        ConditionRegistry r = new ConditionRegistry();
        reg(r, 1, 100, 11, "k", "x", 1L);
        reg(r, 1, 101, 12, "k", "y", 2L);
        reg(r, 2, 200, 21, "k", "x", 3L);
        r.removeSession(1);
        assertThat(r.totalCount()).isEqualTo(1);
        assertThat(r.isRegistered(2, 200, "k")).isTrue();
        r.removeSession(99);
        assertThat(r.totalCount()).isEqualTo(1);
    }

    @Test
    void clearDropsAllRegistrations() {
        ConditionRegistry r = new ConditionRegistry();
        reg(r, 1, 100, 11, "k", "x", 1L);
        reg(r, 2, 200, 21, "k2", "y", 2L);
        r.clear();
        assertThat(r.totalCount()).isZero();
        assertThat(r.count("k")).isZero();
        assertThat(r.maxCountCurrent()).isZero();
        assertThat(r.views("k")).isEmpty();
        // 清零后重挂正常登记（换主后幂等接纳的新基线）。
        reg(r, 1, 100, 11, "k", "x", 5L);
    }

    @Test
    void countsAndViewsFollowArrivalOrder() {
        ConditionRegistry r = new ConditionRegistry();
        reg(r, 1, 100, 11, "kA", "c2", 1L);
        reg(r, 1, 101, 12, "kA", "c1", 2L);
        reg(r, 1, 102, 13, "kA", "c2", 3L);
        reg(r, 2, 200, 21, "kB", "x", 4L);
        // count/totalCount/maxCountCurrent：单键合计口径。
        assertThat(r.count("kA")).isEqualTo(3);
        assertThat(r.count("kB")).isEqualTo(1);
        assertThat(r.count("missing")).isZero();
        assertThat(r.totalCount()).isEqualTo(4);
        assertThat(r.maxCountCurrent()).isEqualTo(3);
        // views：集建立序（c2 先于 c1）内按到达序展开——管理明细口径。
        List<ConditionRegistry.View> views = r.views("kA");
        assertThat(views).extracting(ConditionRegistry.View::requestId)
                .containsExactly(100L, 102L, 101L);
        assertThat(views).extracting(ConditionRegistry.View::condition)
                .containsExactly("c2", "c2", "c1");
        assertThat(views.get(0).sessionId()).isEqualTo(1);
        assertThat(views.get(0).threadId()).isEqualTo(11);
        assertThat(views.get(0).registeredAtMs()).isEqualTo(1L);
        // 搬运后集空：views 立即反映（纯读零扰动）。
        r.promoteFirst("kA", "c2", 9, 9);
        assertThat(r.views("kA")).extracting(ConditionRegistry.View::requestId)
                .containsExactly(102L, 101L);
    }
}
