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

package io.github.lamspace.openlatch.server.timer;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Leader 侧延时触发等待簿记（v11，判例 {@code PhaserRegistry}/{@code
 * ConditionRegistry} 双拓扑延伸）。
 *
 * <p><b>职责边界</b>：timer 的账簿本体（代次/装载态/绝对到期时刻/装载去重
 * 槽）在复制状态机内（{@code TimerEntry}），本登记表只承载 <b>Leader 本地
 * 易失</b>的等待订阅——"等待是订阅不是状态"（v10 类目延伸）。AWAIT 登记、
 * 唤醒投递、{@code CANCEL} 摘除、会话死亡摘除与换主清零全部作用于本表；
 * MUST NOT 改变任何复制态（到期本身无位可置——谓词由等待方重发时对照影子
 * 账簿重评）。
 *
 * <p><b>唤醒即出集</b>：与 {@code PhaserRegistry} 同纪律——到期 tick 或
 * DISARM 应用点排空唤醒时，被收集项即从簿记摘除（不留"已通知待重发"位）；
 * 丢失的重发由 {@code headReplyTimeoutMs} 清扫兜底为"未再登记即消失"，
 * 了结幂等由等待方原 {@code request_id} 重发的谓词重评承载。
 *
 * <p><b>线程模型</b>：全部方法 {@code synchronized}；簿记结构为
 * key → 会话 → 请求 三级 {@link LinkedHashMap}（到达序供管理明细）；
 * 与条目锁无交叉持锁（唤醒收集在网关侧完成，本表不提供跨表判定）。
 */
public final class TimerRegistry {

    /**
     * 单个等待登记项（数据载体）。
     *
     * @param armedAtMs 登记时刻（epoch 毫秒；换主重挂后重新计时）
     */
    private record Entry(long armedAtMs) {
    }

    /** key → 会话 → 请求 → 登记项（三级到达序表，空层即剪枝）。 */
    private final Map<String, LinkedHashMap<Long, Map<Long, Entry>>> keys = new LinkedHashMap<>();

    /** 簿记空构造（无状态起步）。 */
    public TimerRegistry() {
    }

    /**
     * 登记等待簿记（幂等：同 (会话,请求) 重复登记覆盖为最新时刻——重发/
     * 重挂不二次入集，判例 {@code PhaserRegistry.register}）。
     *
     * @param key       延时触发键
     * @param sessionId 逻辑会话 id
     * @param requestId 等待请求 id（唤醒通知关联键）
     * @param nowMs     登记时刻（epoch 毫秒）
     */
    public synchronized void register(String key, long sessionId, long requestId, long nowMs) {
        keys.computeIfAbsent(key, k -> new LinkedHashMap<>())
                .computeIfAbsent(sessionId, s -> new LinkedHashMap<>())
                .put(requestId, new Entry(nowMs));
    }

    /**
     * 摘除单个等待簿记（唤醒投递/了结重发/CANCEL 落点；不存在为无操作）。
     *
     * @param key       延时触发键
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
     * 会话关闭摘除（SESSION_CLOSE 应用点，判例 {@code PhaserRegistry}——
     * 摘簿记；timer 账簿本体零扰动，"死亡不撤钟"与"订阅随会话灭"两面在此
     * 分界）。
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
     * @param key 延时触发键
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
                out.add(new WaiterView(s.getKey(), r.getKey(), r.getValue().armedAtMs()));
            }
        }
        return List.copyOf(out);
    }

    /**
     * 指定 key 的簿记计数（观察口径与合并深度护栏消费；不用于裁决）。
     *
     * @param key 延时触发键
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
     * 唤醒排空：摘除并返回该 key 的全部在簿等待项（到期与代终结两种唤醒源
     * 共用——谓词到点即全员共见、无 phaser "已见相位 &lt; 新相位" 的逐人
     * 筛选，timer 等待不携代次入参，排空即全体）。
     *
     * @param key 延时触发键
     * @return 需通知的等待项列表（已从簿记摘除）
     */
    public synchronized List<WaiterView> wakeAll(String key) {
        LinkedHashMap<Long, Map<Long, Entry>> bySession = keys.remove(key);
        if (bySession == null) {
            return List.of();
        }
        List<WaiterView> out = new ArrayList<>();
        for (Map.Entry<Long, Map<Long, Entry>> s : bySession.entrySet()) {
            for (Map.Entry<Long, Entry> r : s.getValue().entrySet()) {
                out.add(new WaiterView(s.getKey(), r.getKey(), r.getValue().armedAtMs()));
            }
        }
        return List.copyOf(out);
    }

    /**
     * 含挂起等待者的 key 集快照（驱动扫描消费；弱一致遍历序，扫描期间新
     * 登记由下轮 tick 覆盖——正确性不依赖精度）。
     *
     * @return 有簿记的 key 列表
     */
    public synchronized List<String> pendingKeys() {
        return List.copyOf(keys.keySet());
    }

    /**
     * 全部簿记计数（{@code waiters} Gauge 合计口径消费；弱一致总数投影）。
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
     * 换主清零（Leader 易失纪律判例 {@code PhaserRegistry}/{@code
     * TopicRegistry}）：等待项经客户端重挂以 {@code AWAIT} 重发补簿记——
     * 谓词在复制账簿，簿记缺失只延后投递不延后了结（重发即重评）。
     */
    public synchronized void clear() {
        keys.clear();
    }

    /**
     * 等待簿记项视图（管理明细与唤醒投递消费）。
     *
     * @param sessionId  逻辑会话 id
     * @param requestId  等待请求 id（唤醒通知关联键）
     * @param armedAtMs  登记时刻（epoch 毫秒；换主重挂后重新计时）
     */
    public record WaiterView(long sessionId, long requestId, long armedAtMs) {
    }
}
