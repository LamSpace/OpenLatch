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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 条件等待集登记表（v9，集群形态的 Leader 本地结构）。
 *
 * <p><b>职责</b>：承载 await 的登记半程——{@code key → (condition_name → 到达序
 * 等待队列)}，供 SIGNAL/SIGNAL_ALL 搬运（转入 {@code WaitQueue} 后由队首纪律
 * 接管唤醒）与 LEAVE/会话摘除。await 的释放半程经复制（折叠 ACQUIRE 条目的
 * 应用点），本登记表 MUST NOT 进入日志与快照（零复制日志边界的登记面，判例
 * {@code TopicRegistry} 装配位与 {@code WaitQueue.clear()} 换主清零）。
 *
 * <p><b>"登记先于释放可见"不变式的承载点</b>：登记发生在 Leader 受理预检点
 * （提交之前），此刻持有归属仍是 awaiter——任何第三方 signal 必被权限检查拒绝，
 * signal 不可能穿过"已释放未登记"的窗。单机形态不经本类（core 条目关键区内
 * 一体完成，判例 {@code LockEntry.awaitFold}）。
 *
 * <p><b>线程模型</b>：全部公开方法 {@code synchronized} 单锁收口（装配位
 * Leader EventLoop 与状态机应用回调并发到达）；{@link #promoteFirst}/{@link #promoteAll}
 * 的调用者权限裁决在 {@code ClusterRequestHandler} 完成（影子表权威读），
 * 本类只做集合搬运并排除调用归属自身（集群预检窗内 awaiter 短暂"持有且在集"，
 * 自 signal 防御性跳过，判例 {@code LockEntry.signal}）。
 *
 * <p><b>身份与幂等</b>：在集身份 = (逻辑会话 id, 折叠 request_id)，同身份
 * 重复登记不二次入集（应答丢失重发/换主重挂均幂等）；登记时刻为观察值
 * （{@code registeredAtMs}，管理明细消费，不参与任何判定，换 term 后重新计时）。
 */
public final class ConditionRegistry {

    /** key → (condition_name → 到达序队列)；外层 LinkedHashMap 保持 key 建立序。 */
    private final Map<String, LinkedHashMap<String, ArrayDeque<Entry>>> keys =
            new LinkedHashMap<>();

    /** 构造空登记表（无实例配置——深度护栏在受理预检点经 WaitQueue 与本类计数合并）。 */
    public ConditionRegistry() {
    }

    /**
     * 幂等登记：同 (会话, 请求) 已在该 key 任一条件集内时不二次入集、不更新
     * 登记时刻（重挂幂等——保留原时刻使等待时长如实）。
     *
     * @param sessionId 逻辑会话 id
     * @param requestId 折叠 ACQUIRE 的 request_id（唤醒通知的 ref 关联键）
     * @param threadId  归属线程（搬运入等待队列后的归属身份）
     * @param key       锁键
     * @param condition 条件名（命名寻址）
     * @param nowMs     登记时刻（epoch 毫秒，观察值）
     * @return true=新增登记；false=同身份已在集（幂等命中）
     */
    public synchronized boolean register(long sessionId, long requestId, long threadId,
            String key, String condition, long nowMs) {
        LinkedHashMap<String, ArrayDeque<Entry>> sets =
                keys.computeIfAbsent(key, k -> new LinkedHashMap<>());
        for (ArrayDeque<Entry> set : sets.values()) {
            for (Entry e : set) {
                if (e.sessionId == sessionId && e.requestId == requestId) {
                    return false;
                }
            }
        }
        sets.computeIfAbsent(condition, c -> new ArrayDeque<>())
                .addLast(new Entry(sessionId, requestId, threadId, nowMs));
        return true;
    }

    /**
     * 同身份是否已在集（预检幂等判定用）。
     *
     * @param sessionId 逻辑会话 id
     * @param requestId 折叠 request_id
     * @param key       锁键
     * @return 在集返回 true
     */
    public synchronized boolean isRegistered(long sessionId, long requestId, String key) {
        LinkedHashMap<String, ArrayDeque<Entry>> sets = keys.get(key);
        if (sets == null) {
            return false;
        }
        for (ArrayDeque<Entry> set : sets.values()) {
            for (Entry e : set) {
                if (e.sessionId == sessionId && e.requestId == requestId) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 搬运该条件集的首个<b>非调用归属</b>等待项（SIGNAL 一人）。
     *
     * @param key        锁键
     * @param condition  条件名
     * @param callerSid  调用会话（权限已由上层裁决持有归属；此处仅排除自身候选）
     * @param callerTid  调用线程
     * @return 被摘除项（已移出集）；无候选返回空
     */
    public synchronized Optional<Promoted> promoteFirst(String key, String condition,
            long callerSid, long callerTid) {
        LinkedHashMap<String, ArrayDeque<Entry>> sets = keys.get(key);
        if (sets == null) {
            return Optional.empty();
        }
        ArrayDeque<Entry> set = sets.get(condition);
        if (set == null) {
            return Optional.empty();
        }
        Iterator<Entry> it = set.iterator();
        while (it.hasNext()) {
            Entry e = it.next();
            if (e.sessionId == callerSid && e.threadId == callerTid) {
                continue;
            }
            it.remove();
            prune(key, condition, set);
            return Optional.of(new Promoted(e.sessionId, e.requestId, e.threadId));
        }
        return Optional.empty();
    }

    /**
     * 搬运该条件集的全部<b>非调用归属</b>等待项（SIGNAL_ALL），按到达序返回。
     *
     * @param key       锁键
     * @param condition 条件名
     * @param callerSid 调用会话
     * @param callerTid 调用线程
     * @return 被摘除项列表（到达序，可能为空）
     */
    public synchronized List<Promoted> promoteAll(String key, String condition,
            long callerSid, long callerTid) {
        LinkedHashMap<String, ArrayDeque<Entry>> sets = keys.get(key);
        List<Promoted> out = new ArrayList<>();
        if (sets == null) {
            return out;
        }
        ArrayDeque<Entry> set = sets.get(condition);
        if (set == null) {
            return out;
        }
        Iterator<Entry> it = set.iterator();
        while (it.hasNext()) {
            Entry e = it.next();
            if (e.sessionId == callerSid && e.threadId == callerTid) {
                continue;
            }
            it.remove();
            out.add(new Promoted(e.sessionId, e.requestId, e.threadId));
        }
        prune(key, condition, set);
        return out;
    }

    /**
     * LEAVE：按 (会话, 请求) 跨集摘除（幂等——不存在亦返回 false 但受理侧
     * 恒回 OK，无操作语义）。
     *
     * @param sessionId     逻辑会话 id
     * @param awaitRequestId 原折叠 ACQUIRE 的 request_id
     * @return 是否实际摘除
     */
    public synchronized boolean leave(long sessionId, long awaitRequestId) {
        boolean removed = false;
        for (Iterator<Map.Entry<String, LinkedHashMap<String, ArrayDeque<Entry>>>> kit =
                keys.entrySet().iterator(); kit.hasNext(); ) {
            Map.Entry<String, LinkedHashMap<String, ArrayDeque<Entry>>> k = kit.next();
            for (Iterator<Map.Entry<String, ArrayDeque<Entry>>> sit =
                    k.getValue().entrySet().iterator(); sit.hasNext(); ) {
                Map.Entry<String, ArrayDeque<Entry>> s = sit.next();
                if (s.getValue().removeIf(e -> e.sessionId == sessionId
                        && e.requestId == awaitRequestId)) {
                    removed = true;
                    if (s.getValue().isEmpty()) {
                        sit.remove();
                    }
                }
            }
            if (k.getValue().isEmpty()) {
                kit.remove();
            }
        }
        return removed;
    }

    /**
     * 授予侧收口：按 (会话,线程) 归属摘除其在全部 key 条件集的陈旧登记
     * （同一线程不可能既持锁又条件等待——普通/折叠获取授予即上一次 await
     * 的终结，判例 {@code LockEntry} 的 purge）。
     *
     * @param sessionId 逻辑会话 id
     * @param threadId  被授予归属线程
     */
    public synchronized void purgeOwner(long sessionId, long threadId) {
        for (Iterator<Map.Entry<String, LinkedHashMap<String, ArrayDeque<Entry>>>> kit =
                keys.entrySet().iterator(); kit.hasNext(); ) {
            Map.Entry<String, LinkedHashMap<String, ArrayDeque<Entry>>> k = kit.next();
            for (Iterator<Map.Entry<String, ArrayDeque<Entry>>> sit =
                    k.getValue().entrySet().iterator(); sit.hasNext(); ) {
                Map.Entry<String, ArrayDeque<Entry>> s = sit.next();
                s.getValue().removeIf(e -> e.sessionId == sessionId && e.threadId == threadId);
                if (s.getValue().isEmpty()) {
                    sit.remove();
                }
            }
            if (k.getValue().isEmpty()) {
                kit.remove();
            }
        }
    }

    /**
     * 会话摘除（"死亡即退订"，三路回收之一——本节点断连传播与失联探针补发的
     * SESSION_CLOSE 同径）：摘除该会话全部条件登记；锁持有/租约零触碰
     * （死亡不吞锁，登记面无锁账可碰）。
     *
     * @param sessionId 逻辑会话 id
     */
    public synchronized void removeSession(long sessionId) {
        for (Iterator<Map.Entry<String, LinkedHashMap<String, ArrayDeque<Entry>>>> kit =
                keys.entrySet().iterator(); kit.hasNext(); ) {
            Map.Entry<String, LinkedHashMap<String, ArrayDeque<Entry>>> k = kit.next();
            for (Iterator<Map.Entry<String, ArrayDeque<Entry>>> sit =
                    k.getValue().entrySet().iterator(); sit.hasNext(); ) {
                Map.Entry<String, ArrayDeque<Entry>> s = sit.next();
                s.getValue().removeIf(e -> e.sessionId == sessionId);
                if (s.getValue().isEmpty()) {
                    sit.remove();
                }
            }
            if (k.getValue().isEmpty()) {
                kit.remove();
            }
        }
    }

    /**
     * 任期清零（换主当选时调用，判例 {@code WaitQueue.clear()}）：丢弃全部
     * 登记——旧任期的搬运时序与 signal 事件不跨任期存续，等待项由客户端
     * ACQUIRE 车道迁移重挂补登记。
     */
    public synchronized void clear() {
        keys.clear();
    }

    /**
     * 单 key 条件等待者计数（合并深度护栏与位次口径）。
     *
     * @param key 锁键
     * @return 该 key 全部条件集人数合计（未登记为 0）
     */
    public synchronized int count(String key) {
        LinkedHashMap<String, ArrayDeque<Entry>> sets = keys.get(key);
        if (sets == null) {
            return 0;
        }
        int total = 0;
        for (ArrayDeque<Entry> set : sets.values()) {
            total += set.size();
        }
        return total;
    }

    /**
     * 全部 key 条件等待者合计（集群形态 {@code waiters} Gauge 与 SUMMARY
     * 等待者总数的加数口径——条件等待者是等待者，判例队列双轨合计同型）。
     *
     * @return 当前在册条件等待者总数
     */
    public synchronized int totalCount() {
        int total = 0;
        for (LinkedHashMap<String, ArrayDeque<Entry>> sets : keys.values()) {
            for (ArrayDeque<Entry> set : sets.values()) {
                total += set.size();
            }
        }
        return total;
    }

    /**
     * 抓取时刻单键条件等待峰值（{@code condition.waiters.max} gauge 口径，
     * 与等待队深/元素/订阅/条件四口径注释互引防混读）。
     *
     * @return 当前各 key 条件等待数最大值；无登记为 0
     */
    public synchronized int maxCountCurrent() {
        int max = 0;
        for (LinkedHashMap<String, ArrayDeque<Entry>> sets : keys.values()) {
            int total = 0;
            for (ArrayDeque<Entry> set : sets.values()) {
                total += set.size();
            }
            max = Math.max(max, total);
        }
        return max;
    }

    /**
     * 单 key 条件等待明细（管理面 KEY_DETAIL 口径：key 建立序、集内到达序；
     * 纯读零扰动，MUST NOT 推进搬运/摘除时序）。
     *
     * @param key 锁键
     * @return 明细视图列表（未登记为空列表）
     */
    public synchronized List<View> views(String key) {
        LinkedHashMap<String, ArrayDeque<Entry>> sets = keys.get(key);
        List<View> out = new ArrayList<>();
        if (sets == null) {
            return out;
        }
        for (Map.Entry<String, ArrayDeque<Entry>> s : sets.entrySet()) {
            for (Entry e : s.getValue()) {
                out.add(new View(s.getKey(), e.sessionId, e.requestId,
                        e.threadId, e.registeredAtMs));
            }
        }
        return out;
    }

    /**
     * 空集与空 key 的级联回收（保持 keys 无零值壳，管理/护栏计数干净）。
     *
     * @param key       锁键
     * @param condition 条件名（搬运后置空则摘集）
     * @param set       被搬运的条件集（判空即回收）
     */
    private void prune(String key, String condition, ArrayDeque<Entry> set) {
        LinkedHashMap<String, ArrayDeque<Entry>> sets = keys.get(key);
        if (sets == null) {
            return;
        }
        if (set.isEmpty()) {
            sets.remove(condition);
        }
        if (sets.isEmpty()) {
            keys.remove(key);
        }
    }

    /**
     * 在集登记项（数据载体）。
     *
     * @param sessionId       逻辑会话 id
     * @param requestId       折叠 ACQUIRE 的 request_id（在集身份之二）
     * @param threadId        归属线程（搬运入等待队列后的归属身份）
     * @param registeredAtMs  登记时刻（epoch 毫秒，观察值，不参与判定）
     */
    private record Entry(long sessionId, long requestId, long threadId, long registeredAtMs) {
    }

    /**
     * 搬运结果（数据载体）：转入等待队列时的身份三元组。
     *
     * @param sessionId 逻辑会话 id
     * @param requestId 原折叠 request_id（队列幂等与通知 ref 同源）
     * @param threadId  归属线程
     */
    public record Promoted(long sessionId, long requestId, long threadId) {
    }

    /**
     * 管理明细视图（数据载体）。
     *
     * @param condition        条件名
     * @param sessionId        逻辑会话 id
     * @param requestId        折叠 request_id
     * @param threadId         归属线程
     * @param registeredAtMs   登记时刻（epoch 毫秒，受理节点应用时刻）
     */
    public record View(String condition, long sessionId, long requestId,
                       long threadId, long registeredAtMs) {
    }
}
