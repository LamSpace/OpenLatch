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

package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.protocol.raft.SnapshotLock;
import io.github.lamspace.openlatch.protocol.raft.SnapshotState;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 复制状态全量比对工具（P2-10 交付，design D8）：S2 退出门的跨副本一致判据
 * 与 S4 快照恢复比对（P2-16）共用的单一实现，随 openlatch-server test-jar
 * 发布供下游测试模块复用。
 *
 * <p>两级比对：摘要级（{@link #awaitDigestsAgree}，快速判定"是否一致"）与
 * 结构级（{@link #diff}，在不一致时给出逐字段差异清单，定位到 key/holder
 * 粒度——MUST NOT 以抽样代替全量，§10 快照层要求）。
 */
public final class StateComparisons {

    private StateComparisons() {
    }

    /**
     * 轮询等待全部命名摘要源收敛一致（超时抛错并附各源最新值）。
     *
     * @param digests   标签 → 摘要供给（各副本/恢复前后节点）
     * @param timeoutMs 收敛时限（毫秒）
     * @throws AssertionError 超时仍不一致（消息含各源摘要）
     */
    public static void awaitDigestsAgree(Map<String, Supplier<String>> digests, long timeoutMs) {
        long deadline = System.currentTimeMillis() + timeoutMs;
        AssertionError last = null;
        while (true) {
            try {
                assertDigestsAgree(digests);
                return;
            } catch (AssertionError e) {
                last = e;
            }
            if (System.currentTimeMillis() >= deadline) {
                throw last;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new AssertionError("收敛等待被中断", ie);
            }
        }
    }

    /**
     * 立即断言全部命名摘要一致。
     *
     * @param digests 标签 → 摘要供给
     * @throws AssertionError 存在不一致（消息含各源摘要）
     */
    public static void assertDigestsAgree(Map<String, Supplier<String>> digests) {
        Map<String, String> values = new LinkedHashMap<>();
        digests.forEach((label, sup) -> values.put(label, sup.get()));
        String ref = null;
        List<String> diverged = new ArrayList<>();
        for (Map.Entry<String, String> en : values.entrySet()) {
            if (ref == null) {
                ref = en.getValue();
            } else if (!ref.equals(en.getValue())) {
                diverged.add(en.getKey() + "=" + en.getValue());
            }
        }
        if (!diverged.isEmpty()) {
            throw new AssertionError("复制状态摘要不一致：基准=" + values.keySet().iterator().next()
                    + "=" + ref + "；偏离 " + diverged);
        }
    }

    /**
     * 两份快照状态的结构级全量 diff（逐 key、逐 holder、凭证/到期/租期/模式）。
     *
     * @param expected 期望（如快照前捕获的集群状态）
     * @param actual   实际（如快照加载重建后的状态）
     * @return 差异行列表（"key: 说明"），空表即逐字段一致
     */
    public static List<String> diff(SnapshotState expected, SnapshotState actual) {
        List<String> out = new ArrayList<>();
        Map<String, SnapshotLock> exp = index(expected);
        Map<String, SnapshotLock> act = index(actual);
        for (Map.Entry<String, SnapshotLock> en : exp.entrySet()) {
            SnapshotLock a = act.get(en.getKey());
            if (a == null) {
                out.add(en.getKey() + ": 缺失（期望存在 " + describe(en.getValue()) + "）");
                continue;
            }
            diffLock(en.getKey(), en.getValue(), a, out);
        }
        for (String key : act.keySet()) {
            if (!exp.containsKey(key)) {
                out.add(key + ": 多余（" + describe(act.get(key)) + "）");
            }
        }
        if (expected.getSessionsList().size() != actual.getSessionsList().size()) {
            out.add("sessions: 数量不等 expected=" + expected.getSessionsCount()
                    + " actual=" + actual.getSessionsCount());
        } else if (!new java.util.HashSet<>(expected.getSessionsList())
                .equals(new java.util.HashSet<>(actual.getSessionsList()))) {
            out.add("sessions: 集合不等 expected=" + expected.getSessionsList()
                    + " actual=" + actual.getSessionsList());
        }
        return out;
    }

    /** key → 锁条目索引（重复 key 以最后一条为准，理论不应出现）。 */
    private static Map<String, SnapshotLock> index(SnapshotState st) {
        Map<String, SnapshotLock> m = new LinkedHashMap<>();
        for (SnapshotLock l : st.getLocksList()) {
            m.put(l.getKey(), l);
        }
        return m;
    }

    /** 单 key 两侧逐字段比对，差异写入 out。 */
    private static void diffLock(String key, SnapshotLock e, SnapshotLock a, List<String> out) {
        if (e.getLockType() != a.getLockType()) {
            out.add(key + ": lockType expected=" + e.getLockType() + " actual=" + a.getLockType());
        }
        if (e.getLeaseToken() != a.getLeaseToken()) {
            out.add(key + ": leaseToken expected=" + e.getLeaseToken() + " actual=" + a.getLeaseToken());
        }
        if (e.getExpiresAtMs() != a.getExpiresAtMs()) {
            out.add(key + ": expiresAtMs expected=" + e.getExpiresAtMs() + " actual=" + a.getExpiresAtMs());
        }
        if (e.getLeaseMs() != a.getLeaseMs()) {
            out.add(key + ": leaseMs expected=" + e.getLeaseMs() + " actual=" + a.getLeaseMs());
        }
        Map<String, Integer> eh = holders(e);
        Map<String, Integer> ah = holders(a);
        if (!eh.equals(ah)) {
            out.add(key + ": holders expected=" + eh + " actual=" + ah);
        }
    }

    /** holder → "sid:thread" → 计数 的可比映射。 */
    private static Map<String, Integer> holders(SnapshotLock l) {
        Map<String, Integer> m = new LinkedHashMap<>();
        l.getHoldersList().forEach(h ->
                m.put(h.getSessionId() + ":" + h.getThreadId(), (int) h.getCount()));
        return m;
    }

    /** 锁条目一行简述（diff 消息用）。 */
    private static String describe(SnapshotLock l) {
        return "type=" + l.getLockType() + " token=" + l.getLeaseToken()
                + " exp=" + l.getExpiresAtMs() + " holders=" + holders(l);
    }
}
