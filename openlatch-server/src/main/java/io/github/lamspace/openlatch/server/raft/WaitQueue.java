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
 * Leader 侧等待队列（详设 §4.4/§4.5 的集群承载结构，design D9）：FIFO 排队
 * 是 Leader 任期内状态、不复制、不进引擎——本结构是该契约的唯一实现。
 *
 * <p><b>规则对齐</b>（与 {@code CoreEngine} 单机等待语义同构）：
 * <ul>
 *   <li>位次自 1 起、按入队序；同一 {@code (sessionId, requestId)} 重复请求
 *       幂等去重（不二次入队，返回当前位次）；</li>
 *   <li>深度限额 {@code maxQueueDepthPerKey}（超限回 {@code -1}，调用方映射
 *       {@code OVERLOADED}）；</li>
 *   <li>队首唤醒一次性：{@link #onKeyFreed} 只标记"已通知"并返回待推送项，
 *       截止 {@code headReplyTimeoutMs} 前不重复通知；超时未重发由
 *       {@link #sweepNotified} 摘除并让位下一队首（AWAIT_NOTIFY 丢失兜底，
 *       同 Phase 1 规则）；</li>
 *   <li>授予成功（含幂等重发抵达）经 {@link #onGranted} 出队；会话关闭经
 *       {@link #purgeSession} 全量摘除。</li>
 * </ul>
 *
 * <p><b>任期作用域</b>：结构仅在 Leader 任期内有意义；WinLeadership 调用
 * {@link #clear()}——"单个 Leader 任期内的严格 FIFO"（§4.4 公平性表述）由
 * 任期边界机械保证，降级残留随下次当选一并清除。
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
     */
    public record Waiter(long sessionId, long requestId, String key) { }

    /** 队列节点：等待项 + 已通知标记（0=未通知，否则为通知时刻）。 */
    private static final class Node {
        /** 该节点的等待项。 */
        private final Waiter waiter;
        /** 已通知时刻（毫秒）；0 表示尚未通知。 */
        private long notifiedAtMs;

        /**
         * 构造队列节点（未通知态）。
         *
         * @param waiter 等待项
         */
        private Node(Waiter waiter) {
            this.waiter = waiter;
        }
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
     * @return 1 起位次；深度超限返回 {@code -1}
     */
    public synchronized int enqueue(long sessionId, long requestId, String key) {
        ArrayDeque<Node> q = queues.computeIfAbsent(key, k -> new ArrayDeque<>());
        for (Node n : q) {
            if (n.waiter.sessionId() == sessionId && n.waiter.requestId() == requestId) {
                return indexOf(q, n); // 幂等：重复请求返回当前位次，不二次入队
            }
        }
        if (q.size() >= maxDepthPerKey) {
            return -1;
        }
        q.addLast(new Node(new Waiter(sessionId, requestId, key)));
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
        ArrayDeque<Node> q = queues.get(key);
        if (q == null || q.isEmpty()) {
            return List.of();
        }
        Node head = q.peekFirst();
        if (head.notifiedAtMs == 0) {
            head.notifiedAtMs = now;
            return List.of(head.waiter);
        }
        return List.of(); // 已通知未超时：等重发，不重复推
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
     * 返回新队首供通知（同 Phase 1 的摘除级联语义）。
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
     * 清空全部队列（WinLeadership 任期边界调用，design D9）。
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
