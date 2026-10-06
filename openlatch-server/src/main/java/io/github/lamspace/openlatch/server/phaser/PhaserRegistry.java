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

package io.github.lamspace.openlatch.server.phaser;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 相位器等待簿记（v10，Leader 本地易失态）。
 *
 * <p><b>职责边界</b>：相位账簿本体在复制状态机（{@code PhaserEntry}），本类
 * 只承担集群形态等待项的<b>展示侧簿记</b>——(会话, 请求) → 已见相位号与登记
 * 时刻的映射，供 {@code ADMIN_KEY_DETAIL} 的等待明细呈现 {@code expected_phase}
 * （{@code WaitQueue} 的连接簿记不携该字段）。等待的挂起/唤醒投递簿记归
 * {@code WaitQueue}（判例 BARRIER：位次与推送簿记在队列、账簿在条目），
 * 唤醒正确性不依赖本类——了结恒由纯谓词"当前相位 &gt; 已见相位"承载，
 * 本类任何时刻丢数据只影响观察读数，不影响裁决。
 *
 * <p><b>生命周期</b>：装配位判例 {@code WaitQueue}/{@code ConditionRegistry}
 * ——Leader 权威车道受理时登记、唤醒/了结/取消/CANCEL 与 SESSION_CLOSE 应用点
 * 摘除、换主清零（进程本地态无日志/快照来源；客户端经双通道重挂自动补登——
 * 重挂对 phaser 无损耗，谓词在复制账簿）。
 *
 * <p><b>线程模型</b>：登记/摘除发生在状态机应用线程（gateway 副作用）与
 * Leader 受理线程两处，全部公开方法 {@code synchronized}；观察者读取
 * （{@link #waiters}/{@link #count}）允许弱一致，纯读零扰动。单 key 条目数
 * 受合并等待深度护栏在受理点钳制（超限拒绝先行于本类登记），本类自身不设
 * 上限。
 */
public final class PhaserRegistry {

    /**
     * 等待簿记项（展示侧只读视图）。
     *
     * @param sessionId     逻辑会话 id
     * @param requestId     等待请求 id（唤醒通知关联键）
     * @param expectedPhase 已见相位号
     * @param enqueuedAtMs  登记时刻（epoch 毫秒；换主重挂后重新计时）
     */
    public record WaiterView(long sessionId, long requestId, long expectedPhase,
            long enqueuedAtMs) {
    }

    /** key → (会话,请求) 簿记（LinkedHashMap 保持登记到达序）。 */
    private final Map<String, LinkedHashMap<Long, Map<Long, Entry>>> keys =
            new LinkedHashMap<>();

    /**
     * 簿记条目（expectedPhase + 登记时刻）。
     *
     * @param expectedPhase 已见相位号
     * @param enqueuedAtMs  登记时刻（epoch 毫秒）
     */
    private record Entry(long expectedPhase, long enqueuedAtMs) {
    }

    /** 簿记空构造（无状态起步）。 */
    public PhaserRegistry() {
    }

    /**
     * 登记等待簿记（幂等：同 (会话,请求) 重复登记覆盖为最新时刻——重发/
     * 重挂形态下等待身份唯一）。
     *
     * @param key           相位器键
     * @param sessionId     逻辑会话 id
     * @param requestId     等待请求 id
     * @param expectedPhase 已见相位号
     * @param nowMs         登记时刻（epoch 毫秒）
     */
    public synchronized void register(String key, long sessionId, long requestId,
            long expectedPhase, long nowMs) {
        keys.computeIfAbsent(key, k -> new LinkedHashMap<>())
                .computeIfAbsent(sessionId, s -> new LinkedHashMap<>())
                .put(requestId, new Entry(expectedPhase, nowMs));
    }

    /**
     * 摘除单个等待簿记（唤醒投递/了结重发/CANCEL 落点；不存在为无操作）。
     *
     * @param key       相位器键
     * @param sessionId 逻辑会话 id
     * @param requestId 等待请求 id
     */
    public synchronized void remove(String key, long sessionId, long requestId) {
        LinkedHashMap<Long, Map<Long, Entry>> bySession = keys.get(key);
        if (bySession == null) {
            return;
        }
        Map<Long, Entry> entries = bySession.get(sessionId);
        if (entries != null) {
            entries.remove(requestId);
            if (entries.isEmpty()) {
                bySession.remove(sessionId);
            }
        }
        if (bySession.isEmpty()) {
            keys.remove(key);
        }
    }

    /**
     * 会话关闭摘除（SESSION_CLOSE 应用点，判例 {@code ConditionRegistry} 与
     * "死亡即退订"口径——摘簿记，相位账簿的配额摘除在条目内另行发生）。
     *
     * @param sessionId 逻辑会话 id
     */
    public synchronized void removeSession(long sessionId) {
        for (Iterator<Map.Entry<String, LinkedHashMap<Long, Map<Long, Entry>>>> it =
                keys.entrySet().iterator(); it.hasNext(); ) {
            LinkedHashMap<Long, Map<Long, Entry>> bySession = it.next().getValue();
            bySession.remove(sessionId);
            if (bySession.isEmpty()) {
                it.remove();
            }
        }
    }

    /**
     * 指定 key 的等待簿记快照（登记到达序；管理观察专用，弱一致、零扰动）。
     *
     * @param key 相位器键
     * @return 簿记视图列表（无登记为空表）
     */
    public synchronized List<WaiterView> waiters(String key) {
        LinkedHashMap<Long, Map<Long, Entry>> bySession = keys.get(key);
        if (bySession == null) {
            return List.of();
        }
        List<WaiterView> out = new ArrayList<>();
        for (Map.Entry<Long, Map<Long, Entry>> s : bySession.entrySet()) {
            for (Map.Entry<Long, Entry> r : s.getValue().entrySet()) {
                out.add(new WaiterView(s.getKey(), r.getKey(),
                        r.getValue().expectedPhase(), r.getValue().enqueuedAtMs()));
            }
        }
        return List.copyOf(out);
    }

    /**
     * 指定 key 的簿记计数（观察口径；不用于裁决）。
     *
     * @param key 相位器键
     * @return 簿记条目数
     */
    public synchronized int count(String key) {
        LinkedHashMap<Long, Map<Long, Entry>> bySession = keys.get(key);
        if (bySession == null) {
            return 0;
        }
        int n = 0;
        for (Map<Long, Entry> entries : bySession.values()) {
            n += entries.size();
        }
        return n;
    }

    /**
     * 推进唤醒排空：摘除并返回该 key 中 {@code expectedPhase < newPhase} 的
     * 全部簿记项（登记到达序）。唤醒正确性不依赖本方法被调用的时刻——谓词
     * 判定在受理点先行（迟到的登记即刻了结），本方法负责"已挂起者"的通知
     * 投递源；返回项由网关逐一推送 {@code AWAIT_NOTIFY}。
     *
     * @param key      相位器键
     * @param newPhase 推进后的当前相位
     * @return 需通知的等待项列表（已从簿记摘除）
     */
    public synchronized List<WaiterView> wake(String key, long newPhase) {
        LinkedHashMap<Long, Map<Long, Entry>> bySession = keys.get(key);
        if (bySession == null) {
            return List.of();
        }
        List<WaiterView> out = new ArrayList<>();
        for (Iterator<Map.Entry<Long, Map<Long, Entry>>> sit = bySession.entrySet().iterator();
                sit.hasNext(); ) {
            Map.Entry<Long, Map<Long, Entry>> sEn = sit.next();
            for (Iterator<Map.Entry<Long, Entry>> rit = sEn.getValue().entrySet().iterator();
                    rit.hasNext(); ) {
                Map.Entry<Long, Entry> rEn = rit.next();
                if (rEn.getValue().expectedPhase() < newPhase) {
                    out.add(new WaiterView(sEn.getKey(), rEn.getKey(),
                            rEn.getValue().expectedPhase(), rEn.getValue().enqueuedAtMs()));
                    rit.remove();
                }
            }
            if (sEn.getValue().isEmpty()) {
                sit.remove();
            }
        }
        if (bySession.isEmpty()) {
            keys.remove(key);
        }
        return List.copyOf(out);
    }

    /**
     * 全部簿记计数（{@code waiters} Gauge 合计口径消费；弱一致无意义精度，
     * 仅作总数投影）。
     *
     * @return 簿记条目总数
     */
    public synchronized int totalCount() {
        int n = 0;
        for (var bySession : keys.values()) {
            for (var entries : bySession.values()) {
                n += entries.size();
            }
        }
        return n;
    }

    /**
     * 换主清零（Leader 易失纪律判例 {@code WaitQueue}/{@code TopicRegistry}/
     * {@code ConditionRegistry}）：等待项经客户端重挂以 {@code AWAIT_ADVANCE}
     * 重发补簿记——谓词在复制账簿，簿记缺失只延后展示不延后了结。
     */
    public synchronized void clear() {
        keys.clear();
    }
}
