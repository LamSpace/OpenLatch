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

package io.github.lamspace.openlatch.core.lock;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.core.CoreInspection;
import io.github.lamspace.openlatch.core.KeyFamily;
import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.QueueOpType;
import io.github.lamspace.openlatch.core.command.QueueOpCommand;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.QueueOpResult;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * 有界队列条目（QUEUE 家族状态机）：定型形态/定型容量的元素队列、
 * put/take 双轨等待队列与每会话去重槽。
 *
 * <p><b>职责</b>：承载 {@code OBlockingQueue}/{@code ODelayQueue} 的全部
 * 服务端裁决——入队/出队/批量摘取/读数/去重/挂起/唤醒收集。命令分派与
 * 会话/key 校验由 {@code CoreEngine.queueOp} 门面完成，本条目只负责
 * 形态断言之后的条目内规则。
 *
 * <p><b>线程模型</b>：全部公开方法（含 {@link KeyEntry} 契约实现与观察
 * 读数）在条目监视器（{@code synchronized}）内完成，可安全多线程并发；
 * 唤醒收集仅向出参 {@code notify} 列表追加，通知触发由调用方在条目锁外
 * 执行（判例锁/Latch/Barrier）。元素序由 {@code TreeSet} 比较器
 * （到期时刻升序、同到期按到达序）承载：{@code QUEUE} 形态到期时刻恒 0，
 * 该序退化为纯到达 FIFO；{@code DELAY_QUEUE} 形态实现"最早到期先出、
 * 同到期保持到达序"——后者相对 JDK {@code DelayQueue}（同到期不承诺
 * 顺序）是<b>语义增强</b>，对外契约已显式声明。
 *
 * <p><b>状态机</b>：形态与容量构造时定型、之后不变（跨形态请求
 * {@link Outcome#REJECT_TYPE_MISMATCH}、非零容量主张不符
 * {@link Outcome#REJECT_QUEUE_CAPACITY}，均零扰动）。元素生命周期绑定
 * key 而非会话：<b>会话关闭不摘除、不重排、不回收任何元素</b>——投递者
 * 死亡不吞元素（相对 JDK 同进程堆消散语义的分布式增强声明）；未消费元素
 * 不因到期被删除（到期只影响可见性，条目无 TTL 回收路径）。条目常驻：
 * {@link #isEmpty()} 恒 false，深度清零不触发回收（判例 Latch/ATOMIC/
 * BARRIER）。
 *
 * <p><b>契约边界</b>：元素为不透明字节且<b>不可为 null</b>（与有值引用
 * 形态"null 为一等公民"的 presence 语义刻意不同）；本条目 MUST NOT
 * 复核载荷字节数、容量上限（{@code maxQueueCapacity}）与批量预算
 * （{@code maxDrainBytes}）——尺寸与钳制属接入层专属判定点，节点本地
 * 配置参与 apply 判定会引入跨副本回放分歧（判例 v6）。延时元素的绝对
 * 到期时刻由调用侧（apply）以条目携带时刻折算后传入命令的相对值加
 * 判定时刻计算，条目只保存折算结果并以判定时刻比较。
 *
 * <p><b>去重</b>：每会话单槽（{@code session → (op_seq, op, 交付回执)}），
 * 客户端"同 key 在途写互斥"纪律保证单会话至多一个在途写——同会话同
 * {@code op_seq} 的重发恒命中本会话槽并返回原交付（TAKE/DRAIN 重放交付
 * <b>同一份字节</b>、不重复摘取；PUT 重放不双插）；跨会话槽互不遮蔽；
 * {@code SESSION_CLOSE} 摘除该会话槽与挂起等待。
 */
public final class QueueEntry implements KeyEntry {

    /**
     * 元素状态（快照/重建/观察通道的数据载体）。
     *
     * @param payload      元素载荷字节（非 {@code null}；零长度为合法空串元素）
     * @param expiresAtMs  绝对到期时刻（毫秒；QUEUE 形态恒 0）
     */
    public record ElementState(byte[] payload, long expiresAtMs) {

        /**
         * 构造并做载荷校验与防御性复制。
         *
         * @throws IllegalArgumentException 载荷为 {@code null}（元素非空契约）
         */
        public ElementState {
            Objects.requireNonNull(payload, "queue element payload must be non-null");
            payload = payload.clone();
        }
    }

    /**
     * 去重槽状态（快照/重建/观察通道的数据载体）。
     *
     * @param sessionId 槽主会话 id
     * @param opSeq     槽内写序号
     * @param op        了结的写类操作（PUT/TAKE/DRAIN）
     * @param element   TAKE 已交付元素（其余形态 {@code null}）
     * @param drained   DRAIN 已交付列表（其余形态空表）
     */
    public record SlotState(long sessionId, long opSeq, QueueOpType op,
                            byte[] element, List<byte[]> drained) {

        /**
         * 构造并做交付字节的防御性复制。
         */
        public SlotState {
            Objects.requireNonNull(op, "slot op must be non-null");
            element = element == null ? null : element.clone();
            List<byte[]> copy = new ArrayList<>(drained.size());
            for (byte[] b : drained) {
                copy.add(b.clone());
            }
            drained = List.copyOf(copy);
        }
    }

    /** 元素内部形态：载荷 + 折算后的绝对到期时刻 + 到达序（比较器第二键）。 */
    private static final class Element {
        /** 载荷字节（条目内持有，外传必克隆）。 */
        private final byte[] payload;
        /** 绝对到期时刻（毫秒）；QUEUE 形态恒 0。 */
        private final long expiresAtMs;
        /** 到达序号（同到期时刻内的保序键，随条目单调分配）。 */
        private final long seq;

        /**
         * 构造元素。
         *
         * @param payload      载荷字节（调用侧交出所有权）
         * @param expiresAtMs  绝对到期时刻（毫秒）
         * @param seq          到达序号
         */
        private Element(byte[] payload, long expiresAtMs, long seq) {
            this.payload = payload;
            this.expiresAtMs = expiresAtMs;
            this.seq = seq;
        }
    }

    /**
     * 去重槽内部形态（每会话单槽）。
     *
     * @param opSeq   槽内写序号（同 (会话,opSeq) 重放命中本槽）
     * @param op      了结的写类操作（PUT/TAKE/DRAIN）
     * @param element TAKE 已交付元素字节（其余形态 {@code null}）
     * @param drained DRAIN 已交付列表（其余形态空表；条目内部持有）
     */
    private record Slot(long opSeq, QueueOpType op, byte[] element, List<byte[]> drained) {

        /**
         * 构造槽并固化交付列表（元素数组视为条目所有）。
         */
        public Slot {
            drained = List.copyOf(drained);
        }
    }

    /** 锁键。 */
    private final String key;
    /** 定型形态（QUEUE/DELAY_QUEUE），构造后不变。 */
    private final LockType kind;
    /** 定型容量（≥1），构造后不变。 */
    private final long capacity;
    /** 元素序集合：到期时刻升序、同到期按到达序（QUEUE 形态退化为到达序）。 */
    private final NavigableSet<Element> elements;
    /** 等容量挂起轨（put-waiter，到达序）。 */
    private final ArrayDeque<Waiter> putWaiters = new ArrayDeque<>();
    /** 等元素挂起轨（take-waiter，到达序）。 */
    private final ArrayDeque<Waiter> takeWaiters = new ArrayDeque<>();
    /** 每会话去重槽（会话 → 最近一次已应用写操作及其交付回执）。 */
    private final Map<Long, Slot> slots = new HashMap<>();
    /** 元素载荷字节之和（驻留观察读数，入队/出队增量维护）。 */
    private long payloadBytes;
    /** 到达序号发号器（比较器第二键，随条目单调）。 */
    private long elementSeq;

    /**
     * 私有构造：形态与容量定型。
     *
     * @param key      锁键
     * @param kind     定型形态
     * @param capacity 定型容量（调用方保证 ≥1，工厂与重建入口把关）
     */
    private QueueEntry(String key, LockType kind, long capacity) {
        this.key = key;
        this.kind = kind;
        this.capacity = capacity;
        this.elements = new TreeSet<>((a, b) -> {
            int c = Long.compare(a.expiresAtMs, b.expiresAtMs);
            return c != 0 ? c : Long.compare(a.seq, b.seq);
        });
    }

    /**
     * 建条目工厂：以形态与容量创建空队列。
     *
     * @param key      锁键
     * @param kind     定型形态（{@link LockType#QUEUE} / {@link LockType#DELAY_QUEUE}）
     * @param capacity 定型容量（MUST ≥1——协调面无无界队列）
     * @return 空队列条目
     * @throws IllegalArgumentException 形态非队列家族或容量 {@code < 1}
     */
    public static QueueEntry created(String key, LockType kind, long capacity) {
        if (kind != LockType.QUEUE && kind != LockType.DELAY_QUEUE) {
            throw new IllegalArgumentException("not a queue kind: " + kind);
        }
        if (capacity < 1) {
            throw new IllegalArgumentException("queue capacity must be >= 1: " + capacity);
        }
        return new QueueEntry(key, kind, capacity);
    }

    /**
     * 快照重建工厂：以复制状态直写装配（不经迁移规则——恢复不重演
     * 入队/出队判定），到达序按列表序回灌。
     *
     * @param key      锁键
     * @param kind     定型形态
     * @param capacity 定型容量
     * @param elements 元素列表（队列序 = 比较器序；元素序内的到达序按列表
     *                 下标重建，同到期保序即由此承载）
     * @param slots    去重槽列表（会话 id 升序导出）
     * @return 快照初态的条目
     * @throws IllegalArgumentException 形态/容量非法（正常不可达，快照
     *                                  自洽性由重建输入校验拦截）
     */
    public static QueueEntry restored(String key, LockType kind, long capacity,
            List<ElementState> elements, List<SlotState> slots) {
        if (kind != LockType.QUEUE && kind != LockType.DELAY_QUEUE) {
            throw new IllegalArgumentException("not a queue kind: " + kind);
        }
        if (capacity < 1) {
            throw new IllegalArgumentException("queue capacity must be >= 1: " + capacity);
        }
        QueueEntry e = new QueueEntry(key, kind, capacity);
        for (ElementState es : elements) {
            e.addInOrder(new Element(es.payload().clone(), es.expiresAtMs(), e.elementSeq++));
        }
        for (SlotState ss : slots) {
            e.slots.put(ss.sessionId(),
                    new Slot(ss.opSeq(), ss.op(), ss.element(), ss.drained()));
        }
        return e;
    }

    // ===== KeyEntry 生命周期契约 =====

    @Override
    public String key() {
        return key;
    }

    @Override
    public KeyFamily family() {
        return KeyFamily.QUEUE;
    }

    /**
     * 队列条目常驻不回收：深度清零与会话摘除都不构成回收判据
     * （元素为复制状态、无服务端 TTL 清理路径）。
     *
     * @return 恒 {@code false}
     */
    @Override
    public boolean isEmpty() {
        return false;
    }

    /**
     * 队列无租约语义（无持有概念）。
     *
     * @return 恒 0
     */
    @Override
    public long leaseToken() {
        return 0;
    }

    /**
     * 队列无租约语义。
     *
     * @return 恒 0
     */
    @Override
    public long leaseExpiresAtMs() {
        return 0;
    }

    /**
     * 会话摘除：仅摘除该会话的双轨挂起等待与其去重槽——<b>元素零触碰</b>
     * （元素绑定 key 而非会话，投递者/消费者死亡不吞元素、不影响其余
     * 等待者）。摘除后按各自轨道的满足性收集新队首唤醒（KeyEntry 契约
     * "清理后出现可推进的队首即收集"：死亡 take 队首让位后继时元素恒在、
     * 必然满足；死亡 put 队首让位后继时容量未变、判定天然不满足即不收集
     * ——判例 WaitQueue.purgeSession 的轨道头推进）。
     *
     * @param sessionId          要清理的会话
     * @param now                当前时刻（毫秒，唤醒收集用）
     * @param headReplyTimeoutMs 队首通知响应超时（毫秒，唤醒收集用）
     * @param notify             通知收集列表（由调用方在条目锁外触发）
     */
    @Override
    public synchronized void removeSession(long sessionId, long now,
            long headReplyTimeoutMs, List<Waiter> notify) {
        putWaiters.removeIf(w -> w.sessionId() == sessionId);
        takeWaiters.removeIf(w -> w.sessionId() == sessionId);
        slots.remove(sessionId);
        wakeTrack(takeWaiters, now, headReplyTimeoutMs, notify, true);
        wakeTrack(putWaiters, now, headReplyTimeoutMs, notify, false);
    }

    /**
     * 不可达：队列无租约、永不入到期堆（引擎 {@code expireDue} 无此条目）。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知响应超时（毫秒）
     * @param notify             通知收集列表
     * @throws IllegalStateException 恒抛出（队列条目无到期语义）
     */
    @Override
    public synchronized void forceExpire(long now, long headReplyTimeoutMs, List<Waiter> notify) {
        throw new IllegalStateException("queue entry has no lease to expire");
    }

    /**
     * 已通知队首的响应超时清扫：双轨各自检查队首——处于"已通知、待重发"
     * 且响应截止时刻已过时将其出队，并按各自轨道的满足条件收集新队首
     * 通知（与锁/Latch 同机制，双轨分列）。
     *
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表，由调用方在条目锁外触发
     * @return 是否移除了任一轨道的超时队首
     */
    @Override
    public synchronized boolean sweepNotifiedHead(long now, long headReplyTimeoutMs,
            List<Waiter> notify) {
        boolean removed = sweepTrack(putWaiters, now, headReplyTimeoutMs, notify);
        removed |= sweepTrack(takeWaiters, now, headReplyTimeoutMs, notify);
        return removed;
    }

    /**
     * 双轨挂起合计读数（统计观察面）。
     *
     * @return 本 key 当前排队等待项数（等容量 + 等元素）
     */
    @Override
    public synchronized int waiterCount() {
        return putWaiters.size() + takeWaiters.size();
    }

    // ===== 命令裁决 =====

    /**
     * 队列操作统一裁决入口。判定顺序（首个命中者即为结果）——
     * <ol>
     *   <li>形态断言：命令形态与定型形态不符回 {@link Outcome#REJECT_TYPE_MISMATCH}
     *       （家族判定属门面，形态互拒在此收口——判例标量↔引用跨形态）；</li>
     *   <li>容量断言：非零主张与定型容量不符回 {@link Outcome#REJECT_QUEUE_CAPACITY}
     *       （含 DRAIN/PEEK/SIZE 携带的断言，0 为不主张放行）；</li>
     *   <li>读类短路：PEEK/SIZE 零迁移即时读数、不去重不建条目；</li>
     *   <li>去重命中：写类同会话同 {@code op_seq} 且同 op 回原交付回执
     *       （TAKE/DRAIN 重放交付同一份字节；PUT 重放空交付）；</li>
     *   <li>执行：见各 op 小节——不可满足时按阻塞位分派挂起（QUEUED，含
     *       同 (会话,请求) 幂等位次回显与等待深度护栏）或立即拒绝（DENIED）。</li>
     * </ol>
     * 唤醒收集：入队成功可能使 take 轨队首可消费、出队成功必然释放容量
     * ——两事件分别对各自轨道队首标记"已通知"并追加 {@code notify}，
     * 已通知或不可满足的队首不收集（防重复推送，判例 {@code WaitQueue}）。
     *
     * @param cmd                队列命令（形状合法性与尺寸钳制已由接入层受理）
     * @param now                当前时刻（毫秒；集群 apply 为条目携带时刻——
     *                           延时判定与到期折算的确定性基准）
     * @param cfg                限额配置（等待深度上限）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表，由调用方在条目锁外触发
     * @return 操作结果（择用规则见 {@link QueueOpResult}）
     * @throws NullPointerException {@code cmd} 为 null
     */
    public synchronized QueueOpResult op(QueueOpCommand cmd, long now, CoreConfig cfg,
            long headReplyTimeoutMs, List<Waiter> notify) {
        Objects.requireNonNull(cmd, "cmd");
        if (cmd.kind() != kind) {
            return QueueOpResult.rejected(Outcome.REJECT_TYPE_MISMATCH);
        }
        if (cmd.capacity() != 0 && cmd.capacity() != capacity) {
            return QueueOpResult.rejected(Outcome.REJECT_QUEUE_CAPACITY);
        }
        switch (cmd.op()) {
            case PEEK -> {
                Element head = visibleHead(now);
                return QueueOpResult.grantedPeek(head == null ? null : head.payload.clone(),
                        capacity);
            }
            case SIZE -> {
                return QueueOpResult.grantedSize(elements.size(), capacity);
            }
            case PUT -> {
                Slot s = slots.get(cmd.sessionId());
                if (s != null && s.opSeq() == cmd.opSeq() && s.op() == QueueOpType.PUT) {
                    return QueueOpResult.grantedPut(capacity);
                }
                int myPut = indexOf(putWaiters, cmd.sessionId(), cmd.requestId());
                if (myPut > 0) {
                    if (myPut == 1 && elements.size() < capacity) {
                        // 唤醒重发的在队 put 队首：自我摘除后入队（判例队首重发
                        // 获批出队）。非队首重发不得插队（FIFO 队首门）。
                        removeWaiter(putWaiters, cmd.sessionId(), cmd.requestId());
                        insert(cmd, now);
                        slots.put(cmd.sessionId(),
                                new Slot(cmd.opSeq(), QueueOpType.PUT, null, List.of()));
                        wakeTrack(takeWaiters, now, headReplyTimeoutMs, notify, true);
                        wakeTrack(putWaiters, now, headReplyTimeoutMs, notify, false);
                        return QueueOpResult.grantedPut(capacity);
                    }
                    // 回弹臂/非队首早到：撤销已通知标记、原位续挂（位次保持），
                    // 等待下一次可见性/容量事件。
                    requeueAt(putWaiters, cmd.sessionId(), cmd.requestId());
                    return QueueOpResult.queued(myPut);
                }
                if (cmd.blocking() && !putWaiters.isEmpty()) {
                    // 在队等容量者不被新挂起者插队（增强承诺，对齐公平锁禁 barging）。
                    return park(putWaiters, cmd, now, cfg);
                }
                if (elements.size() < capacity) {
                    insert(cmd, now);
                    slots.put(cmd.sessionId(),
                            new Slot(cmd.opSeq(), QueueOpType.PUT, null, List.of()));
                    wakeTrack(takeWaiters, now, headReplyTimeoutMs, notify, true);
                    return QueueOpResult.grantedPut(capacity);
                }
                if (!cmd.blocking()) {
                    return QueueOpResult.rejected(Outcome.DENIED);
                }
                return park(putWaiters, cmd, now, cfg);
            }
            case TAKE -> {
                Slot s = slots.get(cmd.sessionId());
                if (s != null && s.opSeq() == cmd.opSeq() && s.op() == QueueOpType.TAKE) {
                    return QueueOpResult.grantedTake(s.element().clone(), capacity);
                }
                int myTake = indexOf(takeWaiters, cmd.sessionId(), cmd.requestId());
                Element head = visibleHead(now);
                if (myTake > 0) {
                    if (myTake == 1 && head != null) {
                        // 唤醒重发的在队 take 队首：自我摘除后消费（判例队首重发
                        // 获批出队）。非队首重发不得越过在队前辈（FIFO 队首门）。
                        removeWaiter(takeWaiters, cmd.sessionId(), cmd.requestId());
                        deliver(head);
                        slots.put(cmd.sessionId(),
                                new Slot(cmd.opSeq(), QueueOpType.TAKE, head.payload, List.of()));
                        wakeTrack(putWaiters, now, headReplyTimeoutMs, notify, false);
                        wakeTrack(takeWaiters, now, headReplyTimeoutMs, notify, true);
                        return QueueOpResult.grantedTake(head.payload, capacity);
                    }
                    if (!cmd.blocking() && myTake == 1 && head != null) {
                        // 不可达防御分支（立即式不挂起，队上无己方等待项）。
                        return QueueOpResult.rejected(Outcome.DENIED);
                    }
                    requeueAt(takeWaiters, cmd.sessionId(), cmd.requestId());
                    return QueueOpResult.queued(myTake);
                }
                if (cmd.blocking() && !takeWaiters.isEmpty()) {
                    // 在队等元素者不被新挂起者插队（增强承诺）。
                    return park(takeWaiters, cmd, now, cfg);
                }
                if (head != null) {
                    deliver(head);
                    slots.put(cmd.sessionId(),
                            new Slot(cmd.opSeq(), QueueOpType.TAKE, head.payload, List.of()));
                    wakeTrack(putWaiters, now, headReplyTimeoutMs, notify, false);
                    return QueueOpResult.grantedTake(head.payload.clone(), capacity);
                }
                if (!cmd.blocking()) {
                    return QueueOpResult.rejected(Outcome.DENIED);
                }
                return park(takeWaiters, cmd, now, cfg);
            }
            case DRAIN -> {
                Slot s = slots.get(cmd.sessionId());
                if (s != null && s.opSeq() == cmd.opSeq() && s.op() == QueueOpType.DRAIN) {
                    return QueueOpResult.grantedDrain(s.drained(), capacity);
                }
                int limit = cmd.maxElements() <= 0
                        ? (int) Math.min(capacity, Integer.MAX_VALUE) : cmd.maxElements();
                List<byte[]> batch = new ArrayList<>();
                while (batch.size() < limit) {
                    Element head = visibleHead(now);
                    if (head == null) {
                        break;
                    }
                    elements.remove(head);
                    payloadBytes -= head.payload.length;
                    batch.add(head.payload);
                }
                List<byte[]> slotBatch = new ArrayList<>(batch.size());
                for (byte[] b : batch) {
                    slotBatch.add(b.clone());
                }
                slots.put(cmd.sessionId(), new Slot(cmd.opSeq(), QueueOpType.DRAIN, null, slotBatch));
                if (!batch.isEmpty()) {
                    wakeTrack(putWaiters, now, headReplyTimeoutMs, notify, false);
                }
                return QueueOpResult.grantedDrain(batch, capacity);
            }
            default -> {
                return QueueOpResult.rejected(Outcome.REJECT_TYPE_MISMATCH);
            }
        }
    }

    /**
     * 延时就绪唤醒（v7，单机调度扫描的条目入口）：尝试唤醒 take 轨队首——
     * QUEUE 形态存在可消费元素即就绪；DELAY 形态要求队首到期时刻不晚于
     * 给定时刻（谓词收口于 {@code visibleHead}）。已通知队首不重复推送
     * （唤醒一次性，超时未重发由 {@code sweepNotifiedHead} 兜底推进）。
     *
     * @param now                当前时刻（毫秒，引擎时钟）
     * @param headReplyTimeoutMs 队首通知的响应超时（毫秒）
     * @param notify             通知收集列表（条目锁内追加、调用方锁外触发）
     * @return 本轮唤醒的等待项数（0 或 1）
     */
    public synchronized int wakeReadyHeads(long now, long headReplyTimeoutMs,
            List<Waiter> notify) {
        int before = notify.size();
        wakeTrack(takeWaiters, now, headReplyTimeoutMs, notify, true);
        return notify.size() - before;
    }

    // ===== 观察读数（须在持有条目锁时调用或由本类同步方法内部使用） =====

    /**
     * 复制态自包含快照（{@code CoreEngine.queueReplicatedState} 的导出形态，
     * 判例 {@code BarrierEntry.ReplicatedState}）。
     *
     * @param kind     定型形态
     * @param capacity 定型容量
     * @param elements 元素列表（队列序）
     * @param slots    去重槽表（会话 id 升序）
     */
    public record ReplicatedState(LockType kind, long capacity, List<ElementState> elements,
                                  List<SlotState> slots) {

        /**
         * 构造并做列表深复制。
         */
        public ReplicatedState {
            elements = List.copyOf(elements);
            slots = List.copyOf(slots);
        }
    }

    /**
     * 复制态导出（条目锁内装配；判例 {@code BarrierEntry#replicatedState()}）。
     *
     * @return 自包含复制态快照
     */
    public synchronized ReplicatedState replicatedState() {
        return new ReplicatedState(kind, capacity, elementsState(), slotsState());
    }

    /**
     * 定型形态（快照序列化侧与管理观察读取）。
     *
     * @return QUEUE 或 DELAY_QUEUE
     */
    public synchronized LockType kind() {
        return kind;
    }

    /**
     * 定型容量（快照序列化侧与管理观察读取）。
     *
     * @return 容量（≥1）
     */
    public synchronized long capacity() {
        return capacity;
    }

    /**
     * 当前深度（驻留元素数，延时形态含未到期项）。
     *
     * @return 元素数
     */
    public synchronized int depth() {
        return elements.size();
    }

    /**
     * 队首元素的绝对到期时刻（DELAY 形态观察读数；受理侧与当前时刻的
     * 比较属调用方）。
     *
     * @return 到期时刻（毫秒）；QUEUE 形态或空队为 0
     */
    public synchronized long headExpiryMs() {
        Element head = elements.isEmpty() ? null : elements.first();
        return head == null ? 0 : head.expiresAtMs;
    }

    /**
     * 全部元素载荷字节之和（驻留治理观察读数）。
     *
     * @return 驻留字节
     */
    public synchronized long totalPayloadBytes() {
        return payloadBytes;
    }

    /**
     * 队首元素载荷读数（截断预览的数据源；不做到期可见性判定）。
     *
     * @return 载荷克隆；空队为 {@code null}
     */
    public synchronized byte[] headPayload() {
        Element head = elements.isEmpty() ? null : elements.first();
        return head == null ? null : head.payload.clone();
    }

    /**
     * 元素列表读数（快照序列化侧）：按队列序导出，载荷逐项克隆。
     *
     * @return 元素状态列表（不可变）
     */
    public synchronized List<ElementState> elementsState() {
        List<ElementState> out = new ArrayList<>(elements.size());
        for (Element e : elements) {
            out.add(new ElementState(e.payload.clone(), e.expiresAtMs));
        }
        return List.copyOf(out);
    }

    /**
     * 去重槽表读数（快照序列化侧）：按会话 id 升序导出（确定性序）。
     *
     * @return 槽状态列表（不可变）
     */
    public synchronized List<SlotState> slotsState() {
        TreeMap<Long, Slot> sorted = new TreeMap<>(slots);
        List<SlotState> out = new ArrayList<>(sorted.size());
        for (Map.Entry<Long, Slot> en : sorted.entrySet()) {
            Slot s = en.getValue();
            byte[] el = s.element() == null ? null : s.element().clone();
            out.add(new SlotState(en.getKey(), s.opSeq(), s.op(), el, s.drained()));
        }
        return List.copyOf(out);
    }

    /**
     * 明细只读快照（{@code CoreEngine.inspect()} 的条目侧装配点）：
     * 双轨等待以 {@code track} 判别导出（1=等容量、2=等元素），队列家族
     * 字段有效、其余家族字段取零值。
     *
     * @param now 采样时刻（毫秒）
     * @return key 明细快照（不可变）
     */
    public synchronized CoreInspection.KeySnapshot snapshot(long now) {
        List<CoreInspection.WaiterSnapshot> waiterSnaps = new ArrayList<>();
        collectWaiters(waiterSnaps, putWaiters, 1, now);
        collectWaiters(waiterSnaps, takeWaiters, 2, now);
        Element head = elements.isEmpty() ? null : elements.first();
        return new CoreInspection.KeySnapshot(key, KeyFamily.QUEUE, false,
                0, 0, 0, 0,
                List.of(), List.copyOf(waiterSnaps),
                0, 0, 0, 0, List.of(),
                null, 0, 0, 0,
                0, 0, 0, false, null, null, null,
                capacity, elements.size(), head == null ? 0 : head.expiresAtMs,
                payloadBytes, head == null ? null : head.payload.clone());
    }

    // ===== 内部实现 =====

    /**
     * 收集单轨等待者读数。
     *
     * @param out   目标列表
     * @param track 等待轨
     * @param kind  轨道判别（1=等容量/2=等元素）
     * @param now   采样时刻（毫秒）
     */
    private static void collectWaiters(List<CoreInspection.WaiterSnapshot> out,
            ArrayDeque<Waiter> track, int kind, long now) {
        for (Waiter w : track) {
            out.add(new CoreInspection.WaiterSnapshot(w.sessionId(), w.requestId(),
                    w.threadId(), w.permits(), w.enqueuedAtMs(),
                    Math.max(0, now - w.enqueuedAtMs()), w.notified(), kind));
        }
    }

    /**
     * 入队：分配到达序、按比较器插入并维护驻留字节。
     *
     * @param e 元素（载荷所有权归条目）
     */
    private void addInOrder(Element e) {
        elements.add(e);
        payloadBytes += e.payload.length;
    }

    /**
     * 命令元素入队（载荷自命令携带的所有权起归条目）。
     *
     * @param cmd 命令（PUT）
     * @param now 判定时刻（毫秒，延时折算基准）
     */
    private void insert(QueueOpCommand cmd, long now) {
        addInOrder(newElement(cmd.element(), expiryOf(cmd, now)));
    }

    /**
     * 队首出队：移除并维护驻留字节；被移除元素的载荷所有权移交调用侧。
     *
     * @param head 队首元素（须为 {@link #elements} 的当前成员）
     */
    private void deliver(Element head) {
        elements.remove(head);
        payloadBytes -= head.payload.length;
    }

    /**
     * 轨内身份定位：返回 (会话,请求) 在轨内的 1 起位次，不在轨回 0。
     *
     * @param track     等待轨
     * @param sessionId 会话
     * @param requestId 请求
     * @return 位次（1 起）或 0
     */
    private static int indexOf(ArrayDeque<Waiter> track, long sessionId, long requestId) {
        int position = 0;
        for (Waiter w : track) {
            position++;
            if (w.sessionId() == sessionId && w.requestId() == requestId) {
                return position;
            }
        }
        return 0;
    }

    /**
     * 轨内自我摘除（唤醒重发获批出队）。
     *
     * @param track     等待轨
     * @param sessionId 会话
     * @param requestId 请求
     */
    private static void removeWaiter(ArrayDeque<Waiter> track, long sessionId, long requestId) {
        track.removeIf(w -> w.sessionId() == sessionId && w.requestId() == requestId);
    }

    /**
     * 回弹续挂：撤销指定在队项的"已通知"标记并保持位次（等待下一次
     * 可见性/容量事件；判例 {@code WaitQueue} 幂等命中的窗口续约语义的
     * core 侧对偶）。
     *
     * @param track     等待轨
     * @param sessionId 会话
     * @param requestId 请求
     */
    private static void requeueAt(ArrayDeque<Waiter> track, long sessionId, long requestId) {
        List<Waiter> rebuilt = new ArrayList<>(track.size());
        for (Waiter w : track) {
            rebuilt.add(w.sessionId() == sessionId && w.requestId() == requestId
                    ? w.withDeadline(0) : w);
        }
        track.clear();
        track.addAll(rebuilt);
    }

    /**
     * 创建元素（到达序经条目发号器分配）。
     *
     * @param payload 载荷字节（所有权移交条目）
     * @param expiry  绝对到期时刻（毫秒；QUEUE 形态传 0）
     * @return 元素
     */
    private Element newElement(byte[] payload, long expiry) {
        return new Element(payload, expiry, elementSeq++);
    }

    /**
     * 延时折算：DELAY 形态 {@code now + max(0, delay_ms)}（加法溢出收纳为
     * {@link Long#MAX_VALUE}）；QUEUE 形态恒 0（到期属性不存在，delay 携带
     * 已由接入层按形状违例拒绝，此处防御忽略）。
     *
     * @param cmd 命令
     * @param now 判定时刻（毫秒，apply 为条目携带时刻）
     * @return 绝对到期时刻
     */
    private long expiryOf(QueueOpCommand cmd, long now) {
        if (kind != LockType.DELAY_QUEUE) {
            return 0;
        }
        long delay = Math.max(0, cmd.delayMs());
        return delay > Long.MAX_VALUE - now ? Long.MAX_VALUE : now + delay;
    }

    /**
     * 出队谓词：QUEUE 形态恒可消费；DELAY 形态要求队首到期时刻不晚于判定
     * 时刻（未到期队首之后的元素不得越过先出——到期序保证该不变量）。
     *
     * @param e   队首元素
     * @param now 判定时刻（毫秒）
     * @return 可消费为 {@code true}
     */
    private boolean visibleAt(Element e, long now) {
        return kind == LockType.QUEUE || e.expiresAtMs <= now;
    }

    /**
     * 取当前可消费的队首元素。
     *
     * @param now 判定时刻（毫秒）
     * @return 可消费队首；无则 {@code null}
     */
    private Element visibleHead(long now) {
        if (elements.isEmpty()) {
            return null;
        }
        Element head = elements.first();
        return visibleAt(head, now) ? head : null;
    }

    /**
     * 挂起入轨：同 (会话,请求) 幂等命中回显位次；双轨合计受等待深度护栏
     * （先于入队动作检查）；新挂起以 threadId 位 0 占位（等待者身份 =
     * (会话,请求)，判例 Latch await）。
     *
     * @param track 目标轨
     * @param cmd   命令
     * @param now   当前时刻（毫秒）
     * @param cfg   限额配置
     * @return QUEUED（位次）或 REJECT_QUEUE_FULL
     */
    private QueueOpResult park(ArrayDeque<Waiter> track, QueueOpCommand cmd,
            long now, CoreConfig cfg) {
        int position = 0;
        for (Waiter w : track) {
            position++;
            if (w.sessionId() == cmd.sessionId() && w.requestId() == cmd.requestId()) {
                return QueueOpResult.queued(position);
            }
        }
        if (putWaiters.size() + takeWaiters.size() >= cfg.maxQueueDepthPerKey()) {
            return QueueOpResult.rejected(Outcome.REJECT_QUEUE_FULL);
        }
        track.addLast(new Waiter(cmd.sessionId(), cmd.requestId(), kind,
                1, 0, now, 0));
        return QueueOpResult.queued(track.size());
    }

    /**
     * 队首推进：目标轨道队首未通知且当前可满足时标记"已通知"并收集。
     *
     * @param track              目标轨（put 或 take）
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 通知响应超时（毫秒）
     * @param notify             通知收集列表
     * @param takeTrack          {@code true}=按元素可见性判定（take 轨）；
     *                           {@code false}=按容量空位判定（put 轨）
     */
    private void wakeTrack(ArrayDeque<Waiter> track, long now, long headReplyTimeoutMs,
            List<Waiter> notify, boolean takeTrack) {
        if (track.isEmpty()) {
            return;
        }
        Waiter head = track.peekFirst();
        if (head.notified()) {
            return; // 已通知待重发：不重复推
        }
        boolean satisfied = takeTrack ? visibleHead(now) != null : elements.size() < capacity;
        if (!satisfied) {
            return; // 队首不满足：全体原地等待（判例许可感知的队首推进）
        }
        track.pollFirst();
        Waiter marked = head.withDeadline(now + headReplyTimeoutMs);
        track.addFirst(marked);
        notify.add(marked);
    }

    /**
     * 单轨超时清扫：已通知且响应截止已过 → 出队；随后对新队首做满足性
     * 收集（通知丢失兜底，与锁/Latch 同机制）。
     *
     * @param track              待扫轨
     * @param now                当前时刻（毫秒）
     * @param headReplyTimeoutMs 通知响应超时（毫秒，新队首标记用）
     * @param notify             通知收集列表
     * @return 是否移除了超时队首
     */
    private boolean sweepTrack(ArrayDeque<Waiter> track, long now,
            long headReplyTimeoutMs, List<Waiter> notify) {
        Waiter head = track.peekFirst();
        if (head == null || !head.notified() || now < head.notifyDeadlineMs()) {
            return false;
        }
        track.pollFirst();
        boolean takeTrack = track == takeWaiters;
        wakeTrack(track, now, headReplyTimeoutMs, notify, takeTrack);
        return true;
    }
}
