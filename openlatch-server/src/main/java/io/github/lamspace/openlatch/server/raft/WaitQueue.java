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
     * @param track     v7 队列挂起轨道（0=非队列单轨；1=等容量 put-waiter；
     *                  2=等元素 take-waiter）——双轨在同 key 的 FIFO 序列内
     *                  交错存储、按轨独立计位次与推进
     */
    public record Waiter(long sessionId, long requestId, String key, int permits, int track) {

        /**
         * 单许可等待项便捷构造（锁与屏障路径）。
         *
         * @param sessionId 逻辑会话 id
         * @param requestId 原 ACQUIRE 请求 id
         * @param key       锁键
         */
        public Waiter(long sessionId, long requestId, String key) {
            this(sessionId, requestId, key, 1, 0);
        }

        /**
         * 许可数形态便捷构造（Semaphore 路径，单轨）。
         *
         * @param sessionId 逻辑会话 id
         * @param requestId 原请求 id
         * @param key       锁键
         * @param permits   请求许可数
         */
        public Waiter(long sessionId, long requestId, String key, int permits) {
            this(sessionId, requestId, key, permits, 0);
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
     * @param position   轨道内 1 起位次
     * @param sessionId  逻辑会话 id
     * @param requestId  挂起的原请求 id
     * @param permits    请求许可数（锁/屏障恒 1）
     * @param waitedMs   已等待时长（毫秒，下限 0）
     * @param notified   是否处于"已通知、待重发"窗口
     * @param track      v7 队列轨道（0=非队列单轨；1=等容量；2=等元素）
     */
    public record WaiterView(int position, long sessionId, long requestId, int permits,
                             long waitedMs, boolean notified, int track) {
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
        return enqueue(sessionId, requestId, key, permits, 0, now);
    }

    /**
     * 队列双轨入队（v7，QUEUE 阻塞式挂起专用；permits 恒 1）。
     *
     * @param sessionId 逻辑会话 id
     * @param requestId 原 QUEUE_OP 请求 id（去重键）
     * @param key       队列键
     * @param track     轨道（1=等容量，2=等元素）
     * @param now       当前时刻（毫秒）
     * @return 轨内 1 起位次；深度超限（双轨合计）返回 {@code -1}
     */
    public synchronized int enqueueTrack(long sessionId, long requestId, String key,
                                         int track, long now) {
        return enqueue(sessionId, requestId, key, 1, track, now);
    }

    /**
     * 入队实现（全轨道共用）：幂等命中返回轨内位次并续约通知窗口；
     * 深度护栏按整键合计（双轨共用判例）。
     *
     * @param sessionId 逻辑会话 id
     * @param requestId 请求 id
     * @param key       锁键
     * @param permits   许可数
     * @param track     轨道
     * @param now       当前时刻（毫秒）
     * @return 轨内位次或 -1
     */
    private int enqueue(long sessionId, long requestId, String key, int permits, int track,
            long now) {
        ArrayDeque<Node> q = queues.computeIfAbsent(key, k -> new ArrayDeque<>());
        for (Node n : q) {
            if (n.waiter.sessionId() == sessionId && n.waiter.requestId() == requestId) {
                if (n.notifiedAtMs != 0) {
                    n.notifiedAtMs = now; // 已通知队首重发抵达：窗口续约
                }
                return trackIndexOf(q, n); // 幂等：重复请求返回轨内位次，不二次入队
            }
        }
        if (q.size() >= maxDepthPerKey) {
            return -1;
        }
        q.addLast(new Node(new Waiter(sessionId, requestId, key, permits, track), now));
        return trackIndexOf(q, q.peekLast());
    }

    /**
     * 元素落地唤醒（v7）：该 key take 轨（track=2）队首若未通知则标记已通知
     * 并返回待推送项；满足性谓词（队首已到期等）由调用方判定后经
     * {@code elementAvailable} 传入——{@code false} 时不推进。
     *
     * @param key              队列键
     * @param now              当前时刻（毫秒）
     * @param elementAvailable 当前存在可消费元素（调用侧按影子表判定）
     * @return 至多一个待通知等待项
     */
    public synchronized List<Waiter> onElementReady(String key, long now,
                                                    boolean elementAvailable) {
        return wakeTrackHead(key, now, 2, elementAvailable);
    }

    /**
     * 容量释放唤醒（v7）：该 key put 轨（track=1）队首若未通知且
     * {@code capacityFree} 为真则标记已通知并返回待推送项。
     *
     * @param key          队列键
     * @param now          当前时刻（毫秒）
     * @param capacityFree 当前存在空余容量（调用侧按影子表判定）
     * @return 至多一个待通知等待项
     */
    public synchronized List<Waiter> onCapacityFreed(String key, long now,
                                                    boolean capacityFree) {
        return wakeTrackHead(key, now, 1, capacityFree);
    }

    /**
     * 轨内队首推进实现。
     *
     * @param key       队列键
     * @param now       当前时刻（毫秒）
     * @param track     目标轨
     * @param satisfied 调用侧判定的满足性
     * @return 待推送列表（空=不推进）
     */
    private List<Waiter> wakeTrackHead(String key, long now, int track, boolean satisfied) {
        if (!satisfied) {
            return List.of();
        }
        ArrayDeque<Node> q = queues.get(key);
        if (q == null) {
            return List.of();
        }
        for (Node n : q) {
            if (n.waiter.track() == track) {
                if (n.notifiedAtMs != 0) {
                    return List.of(); // 轨首已通知窗口内：不重复推
                }
                n.notifiedAtMs = now;
                return List.of(n.waiter);
            }
        }
        return List.of();
    }

    /**
     * 节点在其轨道内的 1 起位次（双轨交错时按轨独立计）。
     *
     * @param q     队列
     * @param target 目标节点
     * @return 轨内位次
     */
    private static int trackIndexOf(ArrayDeque<Node> q, Node target) {
        int position = 0;
        for (Node n : q) {
            if (n.waiter.track() == target.waiter.track()) {
                position++;
            }
            if (n == target) {
                return position;
            }
        }
        return position;
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
            ArrayDeque<Node> q = it.next().getValue();
            // 受影响轨道：会话死亡节点恰为该轨（轨道内）队首时，摘除后须推进新轨首。
            java.util.Set<Integer> affectedTracks = new java.util.HashSet<>();
            java.util.Set<Integer> seenTracks = new java.util.HashSet<>();
            for (Node n : q) {
                if (seenTracks.add(n.waiter.track()) && n.waiter.sessionId() == sessionId) {
                    affectedTracks.add(n.waiter.track());
                }
            }
            q.removeIf(n -> n.waiter.sessionId() == sessionId);
            if (q.isEmpty()) {
                it.remove();
                continue;
            }
            for (int track : affectedTracks) {
                Node head = firstOfTrack(q, track);
                if (head != null && head.notifiedAtMs == 0) {
                    head.notifiedAtMs = now;
                    promote.add(head.waiter);
                }
            }
        }
        return promote;
    }

    /**
     * 轨道内队首节点。
     *
     * @param q     队列
     * @param track 轨道
     * @return 首个该轨节点；无则 {@code null}
     */
    private static Node firstOfTrack(ArrayDeque<Node> q, int track) {
        for (Node n : q) {
            if (n.waiter.track() == track) {
                return n;
            }
        }
        return null;
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
            // 逐轨道扫描：每轨的队首独立判定超时与推进（单轨键行为与既有一致）。
            java.util.Set<Integer> tracks = new java.util.LinkedHashSet<>();
            for (Node n : q) {
                tracks.add(n.waiter.track());
            }
            for (int track : tracks) {
                sweepTrack(q, track, now, promote);
            }
            if (q.isEmpty()) {
                it.remove();
            }
        }
        return promote;
    }

    /**
     * 单轨队首超时摘除与新队首推进。
     *
     * @param q       队列
     * @param track   轨道
     * @param now     当前时刻（毫秒）
     * @param promote 推进收集列表
     */
    private void sweepTrack(ArrayDeque<Node> q, int track, long now, List<Waiter> promote) {
        Node head = firstOfTrack(q, track);
        if (head == null || head.notifiedAtMs <= 0 || now - head.notifiedAtMs < headReplyTimeoutMs) {
            return;
        }
        q.remove(head);
        Node next = firstOfTrack(q, track);
        if (next != null && next.notifiedAtMs == 0) {
            next.notifiedAtMs = now;
            promote.add(next.waiter);
        }
    }

    /**
     * 已挂起等待项在其轨道内的位次（v7 重发队首门判定用——非队首的重发
     * 不得越过在队前辈直接提交，判例锁"队首已通知窗口禁止越过"）。
     *
     * @param sessionId 逻辑会话 id
     * @param requestId 请求 id
     * @param key       队列键
     * @param track     轨道（1/2）
     * @return 轨内位次（1 起）；未在队返回 0
     */
    public synchronized int trackPosition(long sessionId, long requestId, String key,
                                          int track) {
        ArrayDeque<Node> q = queues.get(key);
        if (q == null) {
            return 0;
        }
        int position = 0;
        for (Node n : q) {
            if (n.waiter.track() == track) {
                position++;
                if (n.waiter.sessionId() == sessionId && n.waiter.requestId() == requestId) {
                    return position;
                }
            }
        }
        return 0;
    }

    /**
     * 存在指定轨道挂起等待者的键集合（v7 就绪扫描的驱动面——只扫有
     * 等待者的键，不做全表扫描）。
     *
     * @param track 轨道（1=等容量，2=等元素）
     * @return 键列表（登记序快照）
     */
    public synchronized List<String> trackKeys(int track) {
        List<String> out = new ArrayList<>();
        for (java.util.Map.Entry<String, ArrayDeque<Node>> en : queues.entrySet()) {
            for (Node n : en.getValue()) {
                if (n.waiter.track() == track) {
                    out.add(en.getKey());
                    break;
                }
            }
        }
        return out;
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
        int[] perTrack = new int[3];
        for (Node n : q) {
            int track = n.waiter.track();
            perTrack[track]++;
            out.add(new WaiterView(perTrack[track], n.waiter.sessionId(), n.waiter.requestId(),
                    n.waiter.permits(), Math.max(0, now - n.enqueuedAtMs), n.notifiedAtMs != 0,
                    track));
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
