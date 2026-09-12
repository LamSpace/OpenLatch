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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Leader 侧等待队列（集群承载结构）：FIFO 排队
 * 是 Leader 任期内状态、不复制、不进引擎——本结构是该契约的唯一实现。
 *
 * <p><b>规则对齐</b>（与 {@code CoreEngine} 单机等待语义同构）：
 * <ul>
 *   <li>位次自 1 起、按入队序；同一 {@code (sessionId, requestId)} 重复请求
 *       幂等去重（不二次入队，返回当前位次）；</li>
 *   <li>深度限额 {@code maxQueueDepthPerKey}（超限回 {@code -1}，调用方映射
 *       {@code OVERLOADED}）；</li>
 *   <li>等待项携带请求许可数（{@code permits}，锁与屏障恒 1）：Semaphore 的
 *       队首唤醒须"可用许可 ≥ 队首请求"方可通知（防大请求饥饿，
 *       与单机 {@code SemaphoreEntry} 判定语义等价）；屏障归零
 *       经 {@link #broadcastKey} 全体放行；</li>
 *   <li>队首唤醒一次性：{@link #onKeyFreed} 只标记"已通知"并返回待推送项，
 *       截止 {@code headReplyTimeoutMs} 前不重复通知；超时未重发由
 *       {@link #sweepNotified} 摘除并让位下一队首（AWAIT_NOTIFY 丢失兜底，
 *       同单机规则）；</li>
 *   <li>授予成功（含幂等重发抵达）经 {@link #onGranted} 出队；会话关闭经
 *       {@link #purgeSession} 全量摘除。</li>
 * </ul>
 *
 * <p><b>任期作用域</b>：结构仅在 Leader 任期内有意义；WinLeadership 调用
 * {@link #clear()}——"单个 Leader 任期内的严格 FIFO"由任期边界机械保证，
 * 降级残留随下次当选一并清除。
 *
 * <p><b>线程模型</b>：全部方法以实例锁同步——写侧来源包括连接 EventLoop
 * （入队/出队）与状态机应用线程（唤醒/摘除），读侧来源含调度线程
 * （清扫）。实例锁内 MUST NOT 做网络 I/O 或阻塞（推送由调用方出锁后投递）。
 */
public final class WaitQueue {

    /**
     * 单个等待项（逻辑会话粒度）。
     *
     * @param sessionId 逻辑会话 id
     * @param requestId 原 ACQUIRE 请求 id
     * @param key       锁键
     * @param permits   请求许可数（锁/屏障恒 1，Semaphore 为申请量）
     */
    public record Waiter(long sessionId, long requestId, String key, int permits) {

        /**
         * 单许可等待项便捷构造（锁与屏障路径）。
         *
         * @param sessionId 逻辑会话 id
         * @param requestId 原 ACQUIRE 请求 id
         * @param key       锁键
         */
        public Waiter(long sessionId, long requestId, String key) {
            this(sessionId, requestId, key, 1);
        }
    }

    /** 队列节点：等待项 + 已通知标记（0=未通知，否则为通知时刻）+ 入队时刻。 */
    private static final class Node {
        /** 该节点的等待项。 */
        private final Waiter waiter;
        /** 已通知时刻（毫秒）；0 表示尚未通知。 */
        private long notifiedAtMs;
        /** 入队时刻（epoch 毫秒，管理观察 waited_ms 折算基准）。 */
        private final long enqueuedAtMs;

        /**
         * 构造队列节点（未通知态）。
         *
         * @param waiter 等待项
         * @param enqueuedAtMs 入队时刻（epoch 毫秒）
         */
        private Node(Waiter waiter, long enqueuedAtMs) {
            this.waiter = waiter;
            this.enqueuedAtMs = enqueuedAtMs;
        }
    }

    /**
     * 管理观察的等待项视图：位次、归属与已等待时长的不可变
     * 快照——{@code waitedMs} 以调用方提供的 {@code now} 折算。
     *
     * @param position   1 起位次
     * @param sessionId  逻辑会话 id
     * @param requestId  挂起的原请求 id
     * @param permits    请求许可数（锁/屏障恒 1）
     * @param waitedMs   已等待时长（毫秒，下限 0）
     * @param notified   是否处于"已通知、待重发"窗口
     */
    public record WaiterView(int position, long sessionId, long requestId, int permits,
                             long waitedMs, boolean notified) {
    }

    /** key → FIFO 队列（插入序=登记序）。 */
    private final LinkedHashMap<String, ArrayDeque<Node>> queues = new LinkedHashMap<>();
    /** 单 key 深度上限。 */
    private final int maxDepthPerKey;
    /** 已通知队首的重发窗口（毫秒）。 */
    private final long headReplyTimeoutMs;

    /**
     * 构造等待队列。
     *
     * @param maxDepthPerKey     单 key 等待深度上限（≥1）
     * @param headReplyTimeoutMs 已通知队首响应超时（毫秒，&gt;0）
     */
    public WaitQueue(int maxDepthPerKey, long headReplyTimeoutMs) {
        this.maxDepthPerKey = maxDepthPerKey;
        this.headReplyTimeoutMs = headReplyTimeoutMs;
    }

    /**
     * 入队（或幂等命中）。锁内完成，返回位次。
     *
     * @param sessionId 逻辑会话 id
     * @param requestId 原 ACQUIRE 请求 id（去重键）
     * @param key       锁键
     * @param now       当前时刻（毫秒，幂等命中的通知窗口续约用）
     * @return 1 起位次；深度超限返回 {@code -1}
     */
    public synchronized int enqueue(long sessionId, long requestId, String key, long now) {
        return enqueue(sessionId, requestId, key, 1, now);
    }

    /**
     * 入队（携带请求许可数；幂等命中返回位次）。
     *
     * @param sessionId 逻辑会话 id
     * @param requestId 请求 id
     * @param key       锁键
     * @param permits   请求许可数（Semaphore）；锁与屏障传 1
     * @param now       当前时刻（毫秒）——幂等命中且该等待项处于"已通知"
     *                  窗口时用以续约（重发抵达即存活证明，窗口重新起算；
     *                  Semaphore 的"通知时池足量、重发时池又被占"竞态由此
     *                  不丢队列位）
     * @return 1 起位次；深度超限返回 {@code -1}
     */
    public synchronized int enqueue(long sessionId, long requestId, String key, int permits, long now) {
        ArrayDeque<Node> q = queues.computeIfAbsent(key, k -> new ArrayDeque<>());
        for (Node n : q) {
            if (n.waiter.sessionId() == sessionId && n.waiter.requestId() == requestId) {
                if (n.notifiedAtMs != 0) {
                    n.notifiedAtMs = now; // 已通知队首重发抵达：窗口续约
                }
                return indexOf(q, n); // 幂等：重复请求返回当前位次，不二次入队
            }
        }
        if (q.size() >= maxDepthPerKey) {
            return -1;
        }
        q.addLast(new Node(new Waiter(sessionId, requestId, key, permits), now));
        return q.size();
    }

    /**
     * 锁被完全释放/到期回收后的队首推进：若存在未通知队首，标记已通知并
     * 返回其待推送；已通知且窗口未过期者不重复返回。
     *
     * @param key 被释放的锁键
     * @param now 当前时刻（毫秒）
     * @return 至多一个待通知等待项（可能为空）
     */
    public synchronized List<Waiter> onKeyFreed(String key, long now) {
        return onKeyFreedPermits(key, now, Integer.MAX_VALUE);
    }

    /**
     * 许可感知的队首推进：仅当可用许可满足队首请求
     * 时通知队首；队首不满足时不检查任何后续条目（防大请求饥饿）。锁路径
     * {@code available} 传 {@link Integer#MAX_VALUE} 与 {@link #onKeyFreed} 等价。
     *
     * @param key       锁键
     * @param now       当前时刻（毫秒）
     * @param available 该 key 当前可用许可数
     * @return 至多一个待通知等待项
     */
    public synchronized List<Waiter> onKeyFreedPermits(String key, long now, int available) {
        ArrayDeque<Node> q = queues.get(key);
        if (q == null || q.isEmpty()) {
            return List.of();
        }
        Node head = q.peekFirst();
        if (head.notifiedAtMs != 0) {
            return List.of(); // 已通知未超时：等重发，不重复推
        }
        if (head.waiter.permits() > available) {
            return List.of(); // 队首不满足：全体原地等待
        }
        head.notifiedAtMs = now;
        return List.of(head.waiter);
    }

    /**
     * 屏障归零全体广播：标记全部未通知等待项为已通知并返回
     * 待推送列表；已通知项不重复推（其重发自行抵达放行）。条目不离队——
     * 等待者经重发命中"已归零"时由 {@link #onGranted} 出队。
     *
     * @param key 屏障键
     * @param now 当前时刻（毫秒）
     * @return 需推送通知的等待项列表
     */
    public synchronized List<Waiter> broadcastKey(String key, long now) {
        ArrayDeque<Node> q = queues.get(key);
        if (q == null || q.isEmpty()) {
            return List.of();
        }
        List<Waiter> out = new ArrayList<>();
        for (Node n : q) {
            if (n.notifiedAtMs == 0) {
                n.notifiedAtMs = now;
                out.add(n.waiter);
            }
        }
        return out;
    }

    /**
     * 撤销队首的已通知标记（清扫推进遇 Semaphore 队首许可不足时的回退臂）：
     * 下一轮清扫重新评估通知资格。
     *
     * @param key 锁键
     */
    public synchronized void deferHead(String key) {
        ArrayDeque<Node> q = queues.get(key);
        if (q != null && !q.isEmpty()) {
            q.peekFirst().notifiedAtMs = 0;
        }
    }

    /**
     * 授予成功的出队（幂等重发经复制路径授予后调用；也覆盖"队首重发获批"）。
     *
     * @param sessionId 逻辑会话 id
     * @param requestId 请求 id
     */
    public synchronized void onGranted(long sessionId, long requestId) {
        for (Iterator<ArrayDeque<Node>> it = queues.values().iterator(); it.hasNext(); ) {
            ArrayDeque<Node> q = it.next();
            if (q.removeIf(n -> n.waiter.sessionId() == sessionId && n.waiter.requestId() == requestId)) {
                if (q.isEmpty()) {
                    it.remove();
                }
                return; // (sid, rid) 全集群唯一，命中即止
            }
        }
    }

    /**
     * 会话关闭摘除：移除该会话全部等待项；若因此改变了某个 key 的队首，
     * 返回新队首供通知（同单机的摘除级联语义）。
     *
     * @param sessionId 逻辑会话 id
     * @param now       当前时刻（毫秒，用于新队首通知标记）
     * @return 需要补发通知的等待项列表
     */
    public synchronized List<Waiter> purgeSession(long sessionId, long now) {
        List<Waiter> promote = new ArrayList<>();
        for (Iterator<java.util.Map.Entry<String, ArrayDeque<Node>>> it = queues.entrySet().iterator();
                it.hasNext(); ) {
            java.util.Map.Entry<String, ArrayDeque<Node>> en = it.next();
            ArrayDeque<Node> q = en.getValue();
            boolean headRemoved = !q.isEmpty() && q.peekFirst().waiter.sessionId() == sessionId;
            q.removeIf(n -> n.waiter.sessionId() == sessionId);
            if (q.isEmpty()) {
                it.remove();
                continue;
            }
            if (headRemoved && q.peekFirst().notifiedAtMs == 0) {
                q.peekFirst().notifiedAtMs = now;
                promote.add(q.peekFirst().waiter);
            }
        }
        return promote;
    }

    /**
     * 已通知队首超时清扫（调度线程周期调用）：超过 {@code headReplyTimeoutMs}
     * 未重发即摘除视为放弃，并推进新队首通知。
     *
     * @param now 当前时刻（毫秒）
     * @return 需要补发通知的新队首列表
     */
    public synchronized List<Waiter> sweepNotified(long now) {
        List<Waiter> promote = new ArrayList<>();
        for (Iterator<java.util.Map.Entry<String, ArrayDeque<Node>>> it = queues.entrySet().iterator();
                it.hasNext(); ) {
            ArrayDeque<Node> q = it.next().getValue();
            Node head = q.peekFirst();
            if (head != null && head.notifiedAtMs > 0 && now - head.notifiedAtMs >= headReplyTimeoutMs) {
                q.pollFirst();
                if (q.isEmpty()) {
                    it.remove();
                    continue;
                }
                Node next = q.peekFirst();
                if (next.notifiedAtMs == 0) {
                    next.notifiedAtMs = now;
                    promote.add(next.waiter);
                }
            }
        }
        return promote;
    }

    /**
     * key 是否已有等待者（预检查规则：队列非空时后来者 MUST NOT 越过在队者
     * 被授予——与 core "规则 3" 对齐）。
     *
     * @param key 锁键
     * @return 存在等待项为 {@code true}
     */
    public synchronized boolean hasWaiters(String key) {
        ArrayDeque<Node> q = queues.get(key);
        return q != null && !q.isEmpty();
    }

    /**
     * 全部队列条目总数（gauge 指标读数，含锁/Semaphore 等待与
     * latch awaiter——同队列承载）。与入队/出队经同一监视器互斥，
     * 读数对本队列内部一致、与引擎状态弱一致。
     *
     * @return 等待条目总数
     */
    public synchronized int totalWaiters() {
        int total = 0;
        for (ArrayDeque<Node> q : queues.values()) {
            total += q.size();
        }
        return total;
    }

    /**
     * 单 key 队列深度的当时最大值（gauge 指标采样读数，
     * 口径为"抓取时刻"，允许错过两次抓取之间的瞬时峰高）。
     *
     * @return 最大队深；无等待返回 0
     */
    public synchronized int maxQueueDepth() {
        int max = 0;
        for (ArrayDeque<Node> q : queues.values()) {
            if (q.size() > max) {
                max = q.size();
            }
        }
        return max;
    }

    /**
     * 指定 key 的等待队列明细快照（ADMIN_KEY_DETAIL Leader 侧
     * 数据源）：实例锁内按 FIFO 序拷贝位次、归属与以 {@code now} 折算的
     * 已等待时长。与 {@link #totalWaiters()} 同监视器互斥，对本队列内部
     * 一致、与引擎状态弱一致。
     *
     * @param key 锁键
     * @param now 折算时刻（epoch 毫秒）
     * @return 位次自 1 起的不可变列表（无等待返回空表）
     */
    public synchronized List<WaiterView> keyWaiters(String key, long now) {
        ArrayDeque<Node> q = queues.get(key);
        if (q == null || q.isEmpty()) {
            return List.of();
        }
        List<WaiterView> out = new ArrayList<>(q.size());
        int position = 0;
        for (Node n : q) {
            position++;
            out.add(new WaiterView(position, n.waiter.sessionId(), n.waiter.requestId(),
                    n.waiter.permits(), Math.max(0, now - n.enqueuedAtMs), n.notifiedAtMs != 0));
        }
        return List.copyOf(out);
    }

    /**
     * 按逻辑会话聚合的在队等待数（ADMIN_LIST_SESSIONS 的
     * {@code waiting_keys} 来源；仅 Leader 队列有真实值）。
     *
     * @return sessionId → 等待项数（弱一致快照）
     */
    public synchronized java.util.Map<Long, Integer> waitCountsBySession() {
        java.util.Map<Long, Integer> counts = new java.util.HashMap<>();
        for (ArrayDeque<Node> q : queues.values()) {
            for (Node n : q) {
                counts.merge(n.waiter.sessionId(), 1, Integer::sum);
            }
        }
        return java.util.Map.copyOf(counts);
    }

    /**
     * 指定请求是否为该 key 队列的队首（AWAIT_NOTIFY 后重发的自推进判定：
     * 队首且锁已空出时 MUST 走复制授予路径，而非再次入队）。
     *
     * @param sessionId 逻辑会话 id
     * @param requestId 请求 id
     * @param key       锁键
     * @return 队首命中为 {@code true}
     */
    public synchronized boolean isHead(long sessionId, long requestId, String key) {
        ArrayDeque<Node> q = queues.get(key);
        if (q == null || q.isEmpty()) {
            return false;
        }
        Waiter head = q.peekFirst().waiter;
        return head.sessionId() == sessionId && head.requestId() == requestId;
    }

    /**
     * key 的当前等待深度（测试与诊断）。
     *
     * @param key 锁键
     * @return 队列长度（无队列为 0）
     */
    public synchronized int waitCount(String key) {
        ArrayDeque<Node> q = queues.get(key);
        return q == null ? 0 : q.size();
    }

    /**
     * 清空全部队列（WinLeadership 任期边界调用）。
     */
    public synchronized void clear() {
        queues.clear();
    }

    /**
     * 队列在实例锁内求指定节点的位次（1 起）。
     *
     * @param q      目标队列
     * @param target 待定位节点
     * @return 1 起位次；未找到返回 1（理论不可达，防御值）
     */
    private static int indexOf(ArrayDeque<Node> q, Node target) {
        int i = 1;
        for (Node n : q) {
            if (n == target) {
                return i;
            }
            i++;
        }
        return 1; // 理论不可达（target 来自本队列）
    }
}
