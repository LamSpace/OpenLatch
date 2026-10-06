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

package io.github.lamspace.openlatch.core;

import io.github.lamspace.openlatch.core.command.AcquireCommand;
import io.github.lamspace.openlatch.core.command.AtomicOpCommand;
import io.github.lamspace.openlatch.core.command.AtomicRefOpCommand;
import io.github.lamspace.openlatch.core.command.ConditionOp;
import io.github.lamspace.openlatch.core.command.ConditionOpCommand;
import io.github.lamspace.openlatch.core.command.BarrierActionDoneCommand;
import io.github.lamspace.openlatch.core.command.BarrierAwaitCommand;
import io.github.lamspace.openlatch.core.command.BarrierLeaveCommand;
import io.github.lamspace.openlatch.core.command.LatchAwaitCommand;
import io.github.lamspace.openlatch.core.command.LatchCountDownCommand;
import io.github.lamspace.openlatch.core.command.PhaserOpCommand;
import io.github.lamspace.openlatch.core.command.QueueOpCommand;
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.command.RenewCommand;
import io.github.lamspace.openlatch.core.lease.LeaseManager;
import io.github.lamspace.openlatch.core.lock.AtomicEntry;
import io.github.lamspace.openlatch.core.lock.AtomicRefEntry;
import io.github.lamspace.openlatch.core.lock.BarrierEntry;
import io.github.lamspace.openlatch.core.lock.KeyEntry;
import io.github.lamspace.openlatch.core.lock.LatchEntry;
import io.github.lamspace.openlatch.core.lock.LockEntry;
import io.github.lamspace.openlatch.core.lock.LockTable;
import io.github.lamspace.openlatch.core.lock.PhaserEntry;
import io.github.lamspace.openlatch.core.lock.QueueEntry;
import io.github.lamspace.openlatch.core.lock.SemaphoreEntry;
import io.github.lamspace.openlatch.core.lock.Owner;
import io.github.lamspace.openlatch.core.lock.Waiter;
import io.github.lamspace.openlatch.core.snapshot.CoreStateRestore;
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.AtomicOpResult;
import io.github.lamspace.openlatch.core.result.AwaitReleaseResult;
import io.github.lamspace.openlatch.core.result.ConditionOpResult;
import io.github.lamspace.openlatch.core.result.AtomicRefOpResult;
import io.github.lamspace.openlatch.core.result.BarrierActionDoneResult;
import io.github.lamspace.openlatch.core.result.BarrierAwaitResult;
import io.github.lamspace.openlatch.core.result.BarrierLeaveResult;
import io.github.lamspace.openlatch.core.result.LatchAwaitResult;
import io.github.lamspace.openlatch.core.result.LatchCountDownResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.PhaserOpType;
import io.github.lamspace.openlatch.core.result.PhaserOpResult;
import io.github.lamspace.openlatch.core.result.QueueOpResult;
import io.github.lamspace.openlatch.core.result.ReleaseResult;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import io.github.lamspace.openlatch.core.result.RenewResult;
import io.github.lamspace.openlatch.core.session.SessionRegistry;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 锁语义核心门面，是本引擎对外的唯一契约面：全部锁操作（获取、释放、续租）、
 * 会话生命周期与租约到期回收都经此类完成。纯 Java、零外部运行依赖、无网络；
 * 时间可经 {@link Clock} 注入（测试用手工时钟），事件经 {@link CoreEventListener}
 * 向外报告。
 *
 * <p><b>线程模型</b>：所有公共方法均可被多线程并发调用且相互安全。生产环境下的
 * 调用方为多个 Netty IO 线程（业务请求）与单个租约扫描线程（{@link #expireDue}、
 * {@link #sweepNotifiedHeads}）。安全性基于两级机制：跨 key 状态由并发容器
 * （{@link LockTable}、{@link SessionRegistry}、{@link LeaseManager} 各自内部自同步）
 * 承载；单 key 状态迁移在对应 {@link KeyEntry} 的条目锁内完成，任一调用路径
 * 最多持有一个条目锁，不存在跨条目持锁，故无锁顺序死锁风险。
 *
 * <p><b>事件回调</b>：{@link CoreEventListener#notifyHead} 一律在条目锁之外触发
 * （通知列表先在锁内收集，出锁后统一回调），回调实现不得假设任何持锁上下文。
 *
 * <p><b>惰性到期契约</b>：租约到期不会立即释放锁——到期时刻仅记录在
 * {@link LeaseManager} 到期堆中，须由调用方周期性调用 {@link #expireDue}
 * 才真正回收。两次扫描之间已过期的锁仍被视为持有。
 */
public final class CoreEngine {

    /** 限额与租约配置（不可变）。 */
    private final CoreConfig config;
    /** 时间源，所有到期/超时判断均以此为准。 */
    private final Clock clock;
    /** 事件出口，接收队首通知事件（条目锁外触发）。 */
    private final CoreEventListener listener;
    /** key → 状态条目映射与条目生命周期（按家族承载锁/Semaphore/Latch/ATOMIC 条目）。 */
    private final LockTable lockTable = new LockTable();
    /** 租约到期堆，供 {@link #expireDue} 扫描。 */
    private final LeaseManager leaseManager = new LeaseManager();
    /** 会话登记表，会话校验与断连清理的权威。 */
    private final SessionRegistry sessions = new SessionRegistry();
    /** 租约凭证发号器，授予新持有时自增取值。 */
    private final AtomicLong leaseTokenCounter = new AtomicLong(1);
    /** 快照重建守卫位：{@link #restoreFrom} 至多生效一次的标记（装配静默期写，无并发访问）。 */
    private boolean snapshotRestored;

    /**
     * 构造核心引擎。
     *
     * @param config   限额与租约配置
     * @param clock    时间源，测试可用手工时钟推进租约
     * @param listener 事件出口，接收队首通知事件
     */
    public CoreEngine(CoreConfig config, Clock clock, CoreEventListener listener) {
        this.config = Objects.requireNonNull(config);
        this.clock = Objects.requireNonNull(clock);
        this.listener = Objects.requireNonNull(listener);
    }

    /**
     * 快照状态重建：以 {@link CoreStateRestore}
     * 一次性注入复制状态全集，恢复后本引擎对继承锁的行为与从未被截断的
     * 原生演化路径一致。仅供快照加载使用——集群状态机在恢复/安装快照时以
     * <b>全新引擎</b>调用本方法一次；非快照恢复路径 MUST NOT 调用。
     *
     * <p><b>恢复前置（防御校验）</b>：仅接受"零状态且未重建过"的引擎——
     * 守卫位未置、发号器未消耗、锁表为空，违者抛
     * {@link IllegalStateException}。在运行中引擎上重建会撕裂既有持有，
     * 该误用模式被机械拒绝而非依赖调用方自觉。
     *
     * <p><b>注入内容</b>（与原生演化终态逐项对齐）：
     * <ol>
     *   <li>条目按家族重建：锁条目经 {@link LockEntry#restored}、Semaphore
     *       条目经 {@link SemaphoreEntry#restored}（持有计数即许可数、池
     *       余量按总量−持有和推导）、Latch 条目经 {@link LatchEntry#restored}
     *       （计数直写、无租约）、ATOMIC 条目经 {@link AtomicEntry#restored}
     *       （值/版本戳/去重槽直写、无租约无持有者）——均直写快照原值，
     *       不经状态迁移规则、不经 {@link Clock}，等待队列恒空；</li>
     *   <li>会话登记：{@code sessions} 全集逐个登记（内部 sid 由调用方在
     *       构造前经 {@link #sessionOpened()} 预生成亦可——本方法幂等于
     *       登记表 {@code putIfAbsent} 语义）；持有者所属会话触及的 key
     *       一并登记，使 {@link #sessionClosed} 对继承持有的清理完整；</li>
     *   <li>到期堆回填：每个继承条目按（key、凭证、到期时刻）offer 堆记录，
     *       使 {@link #expireDue} 能回收快照继承的租约（陈旧校验语义与
     *       原生路径相同）；</li>
     *   <li>发号水位：租约凭证发号器置为输入的 {@code nextLeaseToken}
     *       （快照内已发出的最大凭证 +1 起），后续授予既不复用继承凭证，
     *       也与未截断副本对同一尾部日志
     *       发出逐笔相同的凭证（跨副本一致依赖此水印）。</li>
     * </ol>
     *
     * <p><b>线程模型</b>：须在装配静默期（任何业务方法并发调用开始前）单线程
     * 完成；本方法不对并发业务调用作防护，混合时序属契约违例。
     *
     * @param restore 重建输入（条目 + 会话全集）；null 抛 {@link NullPointerException}
     * @throws IllegalStateException      引擎非零状态或已重建过
     * @throws IllegalArgumentException   输入自洽性非法（由输入对象构造保证，正常不可达）
     */
    public void restoreFrom(CoreStateRestore restore) {
        Objects.requireNonNull(restore);
        if (snapshotRestored || leaseTokenCounter.get() != 1L || !lockTable.values().isEmpty()) {
            throw new IllegalStateException(
                    "restoreFrom is allowed once on a fresh zero-state engine");
        }
        for (CoreStateRestore.Entry en : restore.entries()) {
            if (en.lockType() == LockType.ATOMIC_LONG || en.lockType() == LockType.ATOMIC_INTEGER
                    || en.lockType() == LockType.ATOMIC_BOOLEAN) {
                // 原子条目：无租约、无持有者、无常驻回收——值/版本戳/去重槽
                // 直写快照原值（重发可判性随槽存续，恢复不重演操作规则）。
                CoreStateRestore.AtomicState as = en.atomic();
                AtomicEntry ae = AtomicEntry.restored(en.key(), en.lockType(), as.initial(),
                        as.value(), as.version(), as.slotSession(), as.slotOpSeq(),
                        as.slotApplied(), as.slotOldValue(), as.slotValue(), as.slotVersion());
                lockTable.computeIfAbsent(en.key(), k -> ae);
                continue;
            }
            if (en.lockType() == LockType.ATOMIC_REFERENCE) {
                // 有值引用条目：与标量形态同判例——无租约、无持有者、无常驻
                // 回收；载荷两态（null/零长度）与去重槽直写快照原值，尺寸不
                // 复核（钳制属接入层，恢复不重演判定）。
                CoreStateRestore.AtomicRefState rs = en.atomicRef();
                AtomicRefEntry re = AtomicRefEntry.restored(en.key(), rs.initial(),
                        rs.value(), rs.version(), rs.slotSession(), rs.slotOpSeq(),
                        rs.slotApplied(), rs.slotOldValue(), rs.slotValue(),
                        rs.slotVersion());
                lockTable.computeIfAbsent(en.key(), k -> re);
                continue;
            }
            if (en.lockType() == LockType.QUEUE || en.lockType() == LockType.DELAY_QUEUE) {
                // 队列条目：容量/元素（含到期时刻）/去重槽直写快照原值
                // （恢复不重演入队出队判定）；双轨挂起为 Leader 本地态恒空，
                // 尺寸不复核（钳制属接入层，判例引用/原子形态）。
                CoreStateRestore.QueueState qs = en.queue();
                QueueEntry qe = QueueEntry.restored(en.key(), en.lockType(), qs.capacity(),
                        qs.elements(), qs.slots());
                lockTable.computeIfAbsent(en.key(), k -> qe);
                continue;
            }
            if (en.lockType() == LockType.PHASER) {
                // 相位器条目：账簿直写（不经迁移规则——恢复不重演注册/
                // 到场判定）；挂起等待集恒空（Leader 本地态，不入快照，
                // 客户端重挂补登记——纯谓词等待重挂无损耗）。
                CoreStateRestore.PhaserState ps = en.phaser();
                PhaserEntry pe = PhaserEntry.restored(en.key(), ps.phase(), ps.registered(),
                        ps.arrived(), ps.parties(), ps.arrivals(),
                        ps.prevPhase(), ps.prevArrivals());
                lockTable.computeIfAbsent(en.key(), k -> pe);
                continue;
            }
            if (en.lockType() == LockType.BARRIER) {
                // 循环屏障条目：世代复制态直写（不经迁移规则——恢复不重演
                // 到场判定）；在队等待队列恒空（Leader 本地态，不入快照）。
                // 了结记录中指向已消亡会话的账簿项由装配侧（installSnapshot）
                // 剔除——其重发路径在会话校验处即被拒，账簿永不触达。
                CoreStateRestore.BarrierState bs = en.barrier();
                BarrierEntry be = BarrierEntry.restored(en.key(), bs.parties(), bs.generation(),
                        bs.arrivals(), bs.actionSession(), bs.actionRequest(),
                        bs.completedGeneration(),
                        switch (bs.completedResult()) {
                            case 1 -> io.github.lamspace.openlatch.core.result.BarrierFinal.TRIPPED;
                            case 2 -> io.github.lamspace.openlatch.core.result.BarrierFinal.BROKEN;
                            default -> null;
                        },
                        bs.completedArrivals(), bs.completedExecutor());
                lockTable.computeIfAbsent(en.key(), k -> be);
                continue;
            }
            if (en.lockType() == LockType.LATCH) {
                // 屏障条目：无租约、无持有者（计数直写；等待者/参与者为
                // Leader 本地态，恢复后恒空，后续操作重新登记）。
                LatchEntry le = LatchEntry.restored(en.key(), en.latchTotal(), en.latchCount());
                lockTable.computeIfAbsent(en.key(), k -> le);
                continue;
            }
            if (en.lockType() == LockType.SEMAPHORE) {
                Map<Owner, Integer> holders = new HashMap<>();
                int held = 0;
                for (CoreStateRestore.Holder h : en.holders()) {
                    holders.put(new Owner(h.sessionId(), h.threadId()), h.count());
                    held += h.count();
                    sessions.register(h.sessionId());
                    sessions.touchIfPresent(h.sessionId(), en.key());
                }
                SemaphoreEntry se = SemaphoreEntry.restored(en.key(), en.permitsTotal(),
                        en.permitsTotal() - held, holders, en.leaseToken(), en.leaseMs(),
                        en.expiresAtMs());
                lockTable.computeIfAbsent(en.key(), k -> se);
                leaseManager.offer(en.key(), en.leaseToken(), en.expiresAtMs());
                continue;
            }
            boolean isRead = en.lockType() == LockType.READ;
            Owner writer = null;
            int writeCount = 0;
            Map<Owner, Integer> readers = new HashMap<>();
            for (CoreStateRestore.Holder h : en.holders()) {
                Owner owner = new Owner(h.sessionId(), h.threadId());
                if (isRead) {
                    readers.put(owner, h.count());
                } else {
                    writer = owner;
                    writeCount = h.count();
                }
                sessions.register(h.sessionId());
                sessions.touchIfPresent(h.sessionId(), en.key());
            }
            LockEntry entry = LockEntry.restored(en.key(), en.lockType() != LockType.SIMPLE,
                    writer, writeCount, readers, en.leaseToken(), en.leaseMs(), en.expiresAtMs());
            lockTable.computeIfAbsent(en.key(), k -> entry);
            leaseManager.offer(en.key(), en.leaseToken(), en.expiresAtMs());
        }
        // 无持有者的登记会话也要在场（会话校验与后续授予依赖登记表）。
        for (Long sid : restore.sessions()) {
            sessions.register(sid);
        }
        // 引擎守卫已保证零状态（cur==1），水位取值即权威下一发号。
        leaseTokenCounter.updateAndGet(cur -> Math.max(cur, restore.nextLeaseToken()));
        snapshotRestored = true;
    }

    /**
     * 下一枚租约凭证的快照读数（不发号）：授予新持有时将签发 {@code >= }
     * 本读数的凭证。供快照生成侧读取发号水位，
     * 与 {@link #restoreFrom} 的水位输入对偶。
     *
     * @return 当前发号器值（首次授予将返回的凭证）
     */
    public long nextLeaseToken() {
        return leaseTokenCounter.get();
    }

    /**
     * 登记新会话（连接握手成功时由上层调用一次）。
     *
     * @return 新登记会话的 sessionId，为正随机数，重复概率可忽略
     */
    public long sessionOpened() {
        long id = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);
        sessions.register(id);
        return id;
    }

    /**
     * 关闭会话：释放该会话的全部持锁（写侧与读侧）、摘除其全部等待项，
     * 并对因此可前进的队首触发通知。幂等：重复调用或关闭未登记会话
     * 均无副作用（首次调用后登记表已移除该会话）。
     *
     * <p>这是断连清理的唯一入口，与 {@link #acquire} 中的会话校验原子互斥：
     * 要么获取请求先登记成功、关闭时一并清理，要么关闭先生效、获取被拒。
     *
     * <p><b>返回值</b>：本次关闭的家族级终态报告 {@link SessionCleanup}——
     * 因"离场即破障"打破世代的 BARRIER 条目 key 集合（当前世代含该会话
     * 到场记录者）与因"隐式配额摘除缩小应到集合"推进相位的 PHASER 条目
     * key 集合。单机路径无需消费（破障/合拢广播已随 {@code notify} 收集
     * 经监听器发出），集群路径由应用点写入回执供 Leader 对存活等待者广播。
     *
     * @param sessionId 要关闭的会话
     * @return 家族终态报告（打破世代的屏障 key 集 + 推进相位的相位器 key 集）
     */
    public SessionCleanup sessionClosed(long sessionId) {
        List<String> brokenBarriers = List.of();
        List<String> advancedPhasers = List.of();
        Set<String> keys = sessions.remove(sessionId);
        if (keys == null) {
            return new SessionCleanup(brokenBarriers, advancedPhasers);
        }
        long now = clock.nowMs();
        for (String key : keys) {
            KeyEntry e = lockTable.get(key);
            if (e == null) {
                continue;
            }
            List<Waiter> notify = new ArrayList<>();
            boolean broke;
            boolean phaserTripped;
            synchronized (e) {
                long genBefore = e instanceof BarrierEntry be ? be.generation() : -1L;
                long phaseBefore = e instanceof PhaserEntry pe ? pe.phase() : -1L;
                e.removeSession(sessionId, now, config.headReplyTimeoutMs(), notify);
                broke = e instanceof BarrierEntry be && be.generation() != genBefore;
                phaserTripped = e instanceof PhaserEntry pe && pe.phase() != phaseBefore;
                if (e.isEmpty()) {
                    lockTable.remove(key, e);
                }
            }
            if (broke) {
                if (brokenBarriers.isEmpty()) {
                    brokenBarriers = new ArrayList<>();
                }
                brokenBarriers.add(key);
            }
            if (phaserTripped) {
                if (advancedPhasers.isEmpty()) {
                    advancedPhasers = new ArrayList<>();
                }
                advancedPhasers.add(key);
            }
            fireNotify(notify, key);
        }
        return new SessionCleanup(brokenBarriers, advancedPhasers);
    }

    /**
     * {@link #sessionClosed(long)} 的家族终态报告：会话关闭引发的
     * "离场即破障"（BARRIER）与"隐式摘除推进"（PHASER）两类跨副本
     * 需向 Leader 侧等待簿记广播的 key 集合。单机路径仅集群消费，
     * 单机监听器经 {@code notify} 收集已自行送达。
     *
     * @param brokenBarriers 被打破世代的循环屏障 key 列表（无则空表）
     * @param advancedPhasers 被推进相位的相位器 key 列表（无则空表）
     */
    public record SessionCleanup(List<String> brokenBarriers, List<String> advancedPhasers) {
    }

    /**
     * 获取锁：校验会话与 key 合法性后，授予、排队或拒绝。
     *
     * <p><b>校验顺序</b>（首个不满足者即为返回值）：
     * <ol>
     *   <li>会话已登记，否则 {@link Outcome#REJECT_SESSION}（预检）；</li>
     *   <li>key 非空，否则 {@link Outcome#REJECT_KEY_EMPTY}；</li>
     *   <li>key 的 UTF-8 字节长度不超过 {@code maxKeyLength}，否则
     *       {@link Outcome#REJECT_KEY_TOO_LONG}；</li>
     *   <li>条目锁内家族判定：key 已有他家族条目则回
     *       {@link Outcome#REJECT_TYPE_MISMATCH}（先于会话登记，条目与
     *       触及集零扰动）；</li>
     *   <li>条目锁内再次权威校验会话仍存活（与 {@link #sessionClosed} 原子互斥），
     *       失败仍回 {@link Outcome#REJECT_SESSION}。</li>
     * </ol>
     *
     * <p><b>租约</b>：请求租约为 0 时取默认租约，并一律夹取到
     * {@code [minLeaseMs, maxLeaseMs]} 之间。授予新持有时签发新租约凭证；
     * 重入获取（写侧或读侧）不换新凭证：持有计数加一、同一凭证、
     * 租约整段刷新；读锁加入已有读者时复用现有凭证。
     *
     * <p><b>排队语义</b>：不存在快路径——锁被占用，或虽无持有者但等待队列
     * 非空（队首已通知、待重发窗口——规则 3 禁止越过在队者）——且
     * {@code queueIfBusy} 为真时入队，返回 {@link Outcome#QUEUED} 与 1 起的
     * 队列位次；同一 {@code (sessionId, requestId)} 重复请求幂等去重——
     * 不二次入队，返回当前位次。队列满返回 {@link Outcome#REJECT_QUEUE_FULL}；
     * 同条件下 {@code queueIfBusy} 为假（立即式）返回 {@link Outcome#DENIED}。
     *
     * <p><b>条目生命周期</b>：条目按需创建；操作完成后若无持有者且无等待者，
     * 立即从锁表移除，避免空条目滞留。
     *
     * @param cmd 获取锁命令，会话必须已登记（否则返回 {@code REJECT_SESSION}）
     * @return 获取结果：{@link Outcome#GRANTED} 时携带租约凭证与实际租约，
     *         {@link Outcome#QUEUED} 时携带队列位次，其余为拒绝原因
     */
    public AcquireResult acquire(AcquireCommand cmd) {
        long now = clock.nowMs();
        if (!sessions.contains(cmd.sessionId())) {
            return new AcquireResult(Outcome.REJECT_SESSION, 0, 0, 0);
        }
        String key = cmd.key();
        if (key == null || key.isEmpty()) {
            return new AcquireResult(Outcome.REJECT_KEY_EMPTY, 0, 0, 0);
        }
        if (key.getBytes(StandardCharsets.UTF_8).length > config.maxKeyLength()) {
            return new AcquireResult(Outcome.REJECT_KEY_TOO_LONG, 0, 0, 0);
        }

        // LATCH/BARRIER/ATOMIC 不经获取通道：ACQUIRE 携带这些类型
        // 属请求形状错误，协议层门控之后由本守卫兜底。
        if (cmd.lockType() == LockType.LATCH || cmd.lockType() == LockType.BARRIER
                || familyOf(cmd.lockType()) == KeyFamily.ATOMIC
                || familyOf(cmd.lockType()) == KeyFamily.QUEUE
                || familyOf(cmd.lockType()) == KeyFamily.PHASER) {
            return new AcquireResult(Outcome.REJECT_TYPE_MISMATCH, 0, 0, 0);
        }
        // v9：await 折叠形状守卫（接入层唯一裁决后的 core 防御兜底）——condition
        // 仅承载于 REENTRANT/SIMPLE/FAIR 三互斥形态；READ/WRITE 或非 LOCK 家族
        // 携带 condition 属类型不匹配，条目状态与会话触及集零扰动。
        if (cmd.condition() != null && (cmd.lockType() == LockType.READ
                || cmd.lockType() == LockType.WRITE
                || familyOf(cmd.lockType()) != KeyFamily.LOCK)) {
            return new AcquireResult(Outcome.REJECT_TYPE_MISMATCH, 0, 0, 0);
        }
        KeyFamily family = familyOf(cmd.lockType());
        // Semaphore 建条目预检：条目不存在时总量主张必须 > 0。
        // 竞态良性：他者抢先建条目后本请求按"既有条目断言"规则处理。
        if (family == KeyFamily.SEMAPHORE && cmd.permitsTotal() <= 0 && lockTable.get(key) == null) {
            return new AcquireResult(Outcome.REJECT_SEMAPHORE_TOTAL, 0, 0, 0);
        }
        boolean reentrant = cmd.lockType() != LockType.SIMPLE;
        long effectiveLeaseMs = clampLease(cmd.requestedLeaseMs());

        while (true) {
            KeyEntry e = lockTable.computeIfAbsent(key, k -> newEntry(family, k, reentrant, cmd));
            AcquireResult result;
            List<Waiter> notifyToFire = null;
            synchronized (e) {
                if (lockTable.get(key) != e) {
                    continue; // 条目在等待期间被移除，重试
                }
                // 家族判定先于会话登记：跨家族请求对条目状态与会话触及集零扰动。
                if (e.family() != family) {
                    return new AcquireResult(Outcome.REJECT_TYPE_MISMATCH, 0, 0, 0);
                }
                // 权威会话校验 + 原子登记，与 sessionClosed 的 remove 原子互斥。
                if (!sessions.touchIfPresent(cmd.sessionId(), key)) {
                    if (e.isEmpty()) {
                        lockTable.remove(key, e);
                    }
                    return new AcquireResult(Outcome.REJECT_SESSION, 0, 0, 0);
                }
                // 条目内规则按实现类分派（锁规则集 / Semaphore 规则集；v9 折叠
                // 形态走 awaitFold——释放+登记同关键区，队首通知经 notify 收集）。
                List<Waiter> foldNotify = cmd.condition() == null ? null : new ArrayList<>();
                result = switch (e) {
                    case LockEntry le -> cmd.condition() != null
                            ? le.awaitFold(cmd, now, leaseTokenCounter::getAndIncrement,
                                    effectiveLeaseMs, config, foldNotify)
                            : le.acquire(cmd, now, leaseTokenCounter::getAndIncrement,
                                    effectiveLeaseMs, config);
                    case SemaphoreEntry se -> se.acquire(cmd, now, leaseTokenCounter::getAndIncrement,
                            effectiveLeaseMs, config);
                    // 家族判定已保证同族，此处为家族尚无实现条目时的收口分支。
                    default -> new AcquireResult(Outcome.REJECT_TYPE_MISMATCH, 0, 0, 0);
                };
                if (result.outcome() == Outcome.GRANTED) {
                    leaseManager.offer(key, result.leaseToken(), now + result.grantedLeaseMs());
                }
                if (e.isEmpty()) {
                    lockTable.remove(key, e);
                }
                notifyToFire = foldNotify;
            }
            if (notifyToFire != null && !notifyToFire.isEmpty()) {
                fireNotify(notifyToFire, key);
            }
            return result;
        }
    }

    /**
     * 请求锁类型 → 条目家族。锁家族全部类型（REENTRANT/SIMPLE/READ/WRITE，
     * 及 FAIR 别名）落 {@link KeyFamily#LOCK}；SEMAPHORE/LATCH
     * 类型接入时在此增行（编译器以 switch 穷尽性强制更新）。
     *
     * @param lockType 请求的锁类型
     * @return 所属条目家族
     */
    private static KeyFamily familyOf(LockType lockType) {
        return switch (lockType) {
            case REENTRANT, SIMPLE, READ, WRITE, FAIR -> KeyFamily.LOCK;
            case SEMAPHORE -> KeyFamily.SEMAPHORE;
            case LATCH -> KeyFamily.LATCH;
            case ATOMIC_LONG, ATOMIC_INTEGER, ATOMIC_BOOLEAN, ATOMIC_REFERENCE -> KeyFamily.ATOMIC;
            case BARRIER -> KeyFamily.BARRIER;
            case QUEUE, DELAY_QUEUE -> KeyFamily.QUEUE;
            case PHASER -> KeyFamily.PHASER;
        };
    }

    /**
     * 按家族创建条目。锁家族条目构造与既有 {@code new LockEntry(k, reentrant)}
     * 逐参数一致；Semaphore 条目以请求的 {@code permitsTotal} 定型许可总量
     * （建条目预检已保证 &gt; 0）；Latch 家族不可达——屏障条目由屏障
     * 专属命令直接装配，ACQUIRE 携带 LATCH 在入口即拒。
     *
     * @param family    目标家族
     * @param key       锁键
     * @param reentrant 可重入性（仅锁家族取用，由首次请求类型定型）
     * @param cmd       建条目请求（Semaphore 读取 {@code permitsTotal}）
     * @return 新条目
     * @throws IllegalStateException 家族未接入（正常路径不可达）
     */
    private static KeyEntry newEntry(KeyFamily family, String key, boolean reentrant, AcquireCommand cmd) {
        return switch (family) {
            case LOCK -> new LockEntry(key, reentrant);
            case SEMAPHORE -> new SemaphoreEntry(key, cmd.permitsTotal());
            // LATCH 条目只能经屏障专属命令（latchAwait/countDown）创建，
            // ACQUIRE 携带 LATCH 类型在入口即拒（见 acquire 首行守卫）。
            case LATCH -> throw new IllegalStateException(
                    "latch entries are created only via latch channels");
            // ATOMIC 条目只能经原子命令通道（atomicOp）创建，
            // ACQUIRE 携带原子类型在入口即拒（见 acquire 守卫）。
            case ATOMIC -> throw new IllegalStateException(
                    "atomic entries are created only via atomic channels");
            // BARRIER 条目只能经循环屏障命令通道（barrierAwait）创建，
            // ACQUIRE 携带屏障定型在入口即拒（见 acquire 守卫）。
            case BARRIER -> throw new IllegalStateException(
                    "barrier entries are created only via barrier channels");
            // 队列条目只能经队列命令通道（queueOp）创建（形态/容量由首次
            // 写入定型），ACQUIRE 携队列定型在入口即拒（见 acquire 守卫）。
            case QUEUE -> throw new IllegalStateException(
                    "queue entries are created only via queue channels");
            // 相位器条目只能经相位器命令通道（phaserOp 的 REGISTER 建条目
            // 路径）创建，ACQUIRE 携 phaser 定型在入口即拒（见 acquire 守卫）。
            case PHASER -> throw new IllegalStateException(
                    "phaser entries are created only via phaser channels");
        };
    }

    /**
     * 释放锁：写侧或读侧持有计数减一，归零时锁完全释放并对新队首触发通知。
     *
     * <p><b>判定顺序</b>（首个不满足者即为返回值）：
     * <ol>
     *   <li>会话已登记，否则 {@link ReleaseStatus#REJECT_SESSION}；</li>
     *   <li>该 key 有条目且存在持有者，否则 {@link ReleaseStatus#NOT_HELD}；</li>
     *   <li>凭证与当前租约匹配，否则 {@link ReleaseStatus#INVALID_TOKEN}
     *       （如租约已到期被回收后重放旧凭证）；</li>
     *   <li>该 {@code (sessionId, threadId)} 确为持有者，否则仍回
     *       {@link ReleaseStatus#NOT_HELD}（防御性归属校验，正常路径下
     *       凭证匹配即归属匹配，此分支理论不可达）。</li>
     * </ol>
     *
     * <p>可重入锁需逐层释放：每次调用只减一层计数，
     * {@code fullyReleased} 仅在计数归零（锁完全释放）时为 {@code true}。
     *
     * @param cmd 释放锁命令，携带获取时签发的租约凭证；key 须非 {@code null}
     *            （协议层已校验），否则抛 {@link NullPointerException}
     * @return 释放结果：状态与是否完全释放
     */
    public ReleaseResult release(ReleaseCommand cmd) {
        long now = clock.nowMs();
        if (!sessions.contains(cmd.sessionId())) {
            return new ReleaseResult(ReleaseStatus.REJECT_SESSION, false);
        }
        KeyEntry e = lockTable.get(cmd.key());
        if (e == null) {
            return new ReleaseResult(ReleaseStatus.NOT_HELD, false);
        }
        List<Waiter> notify = new ArrayList<>();
        ReleaseResult result;
        synchronized (e) {
            // 释放按条目家族分派：锁计数逐层释放，Semaphore 按许可归还，
            // 其余家族无锁释放语义回 NOT_HELD。
            result = switch (e) {
                case LockEntry le -> le.release(cmd, now, config.headReplyTimeoutMs(), notify);
                case SemaphoreEntry se -> se.release(cmd, now, config.headReplyTimeoutMs(), notify);
                default -> new ReleaseResult(ReleaseStatus.NOT_HELD, false);
            };
            if (e.isEmpty()) {
                lockTable.remove(cmd.key(), e);
            }
        }
        fireNotify(notify, cmd.key());
        return result;
    }

    /**
     * 续租：延长当前租约的到期时刻。
     *
     * <p><b>判定顺序</b>：会话未登记回 {@link ReleaseStatus#REJECT_SESSION}；
     * 该 key 无条目回 {@link ReleaseStatus#NOT_HELD}；条目内凭证为 0（无持有者）
     * 回 {@link ReleaseStatus#NOT_HELD}；凭证不匹配回
     * {@link ReleaseStatus#INVALID_TOKEN}。
     *
     * <p>校验通过后，期望租约同样经 0 取默认与上下限夹取，以新值刷新到期时刻，
     * 并向到期堆登记新记录。旧堆记录不作删除，由 {@link #expireDue} 的陈旧
     * 校验跳过。
     *
     * @param cmd 续租命令，携带获取时签发的租约凭证与期望租约时长（0 表示默认）；
     *            key 须非 {@code null}（协议层已校验），否则抛
     *            {@link NullPointerException}
     * @return 续租结果：{@link ReleaseStatus#OK} 时携带新的到期时刻
     */
    public RenewResult renew(RenewCommand cmd) {
        long now = clock.nowMs();
        if (!sessions.contains(cmd.sessionId())) {
            return new RenewResult(ReleaseStatus.REJECT_SESSION, 0);
        }
        KeyEntry e = lockTable.get(cmd.key());
        if (e == null) {
            return new RenewResult(ReleaseStatus.NOT_HELD, 0);
        }
        synchronized (e) {
            // 续租同释放按家族分派；Semaphore 续租刷新全体共享租约，
            // 其余家族对本命令无租约语义回 NOT_HELD。
            RenewResult result = switch (e) {
                case LockEntry le -> le.renew(cmd, now, clampLease(cmd.requestedLeaseMs()));
                case SemaphoreEntry se -> se.renew(cmd, now, clampLease(cmd.requestedLeaseMs()));
                default -> new RenewResult(ReleaseStatus.NOT_HELD, 0);
            };
            if (result.status() == ReleaseStatus.OK) {
                leaseManager.offer(cmd.key(), e.leaseToken(), result.newExpiresAtMs());
            }
            return result;
        }
    }

    /**
     * 等待屏障：校验会话与 key 后，
     * 已归零立即通过、挂起排队或拒绝。
     *
     * <p><b>校验顺序</b>（首个不满足者即为结果）：会话预检 → key 校验 →
     * 条目定位：key 无条目时 MUST 携带 {@code total > 0} 定型创建（否则
     * {@link Outcome#REJECT_LATCH_TOTAL}）；条目锁内回查存活、家族判定
     * （锁/Semaphore 条目 → {@link Outcome#REJECT_TYPE_MISMATCH}）、权威
     * 会话校验与触及登记（与 {@link #sessionClosed} 原子互斥）→
     * {@link LatchEntry#await} 规则集。参与者散尽后条目随既有回收路径移除。
     *
     * @param cmd 等待命令
     * @return 等待结果：GRANTED=已归零通过；QUEUED=挂起（1 起位次）
     */
    public LatchAwaitResult latchAwait(LatchAwaitCommand cmd) {
        long now = clock.nowMs();
        if (!sessions.contains(cmd.sessionId())) {
            return new LatchAwaitResult(Outcome.REJECT_SESSION, 0);
        }
        Outcome keyBad = validateKey(cmd.key());
        if (keyBad != null) {
            return new LatchAwaitResult(keyBad, 0);
        }
        String key = cmd.key();
        while (true) {
            KeyEntry e = lockTable.get(key);
            if (e == null) {
                if (cmd.total() <= 0) {
                    return new LatchAwaitResult(Outcome.REJECT_LATCH_TOTAL, 0);
                }
                e = lockTable.computeIfAbsent(key, k -> new LatchEntry(k, cmd.total()));
            }
            synchronized (e) {
                if (lockTable.get(key) != e) {
                    continue; // 条目竞态移除，重试
                }
                if (e.family() != KeyFamily.LATCH) {
                    return new LatchAwaitResult(Outcome.REJECT_TYPE_MISMATCH, 0);
                }
                if (!sessions.touchIfPresent(cmd.sessionId(), key)) {
                    if (e.isEmpty()) {
                        lockTable.remove(key, e);
                    }
                    return new LatchAwaitResult(Outcome.REJECT_SESSION, 0);
                }
                LatchAwaitResult result = ((LatchEntry) e).await(cmd, now, config);
                if (e.isEmpty()) {
                    lockTable.remove(key, e);
                }
                return result;
            }
        }
    }

    /**
     * 倒计数：条目定位与会话校验同
     * {@link #latchAwait}；生效扣减后计数首次归零时对全部等待者广播
     * 通知（锁外经 {@link CoreEventListener} 触发）。归零屏障上的
     * countDown 为无操作（一次性语义）。
     *
     * @param cmd 倒计数命令（{@code count = 0} 携带 {@code total} 为纯初始化）
     * @return 倒计数结果：GRANTED 携带生效后的剩余计数
     */
    public LatchCountDownResult countDown(LatchCountDownCommand cmd) {
        long now = clock.nowMs();
        if (!sessions.contains(cmd.sessionId())) {
            return new LatchCountDownResult(Outcome.REJECT_SESSION, 0);
        }
        Outcome keyBad = validateKey(cmd.key());
        if (keyBad != null) {
            return new LatchCountDownResult(keyBad, 0);
        }
        String key = cmd.key();
        while (true) {
            KeyEntry e = lockTable.get(key);
            if (e == null) {
                if (cmd.total() <= 0) {
                    return new LatchCountDownResult(Outcome.REJECT_LATCH_TOTAL, 0);
                }
                e = lockTable.computeIfAbsent(key, k -> new LatchEntry(k, cmd.total()));
            }
            List<Waiter> notify = new ArrayList<>();
            LatchCountDownResult result;
            synchronized (e) {
                if (lockTable.get(key) != e) {
                    continue;
                }
                if (e.family() != KeyFamily.LATCH) {
                    return new LatchCountDownResult(Outcome.REJECT_TYPE_MISMATCH, 0);
                }
                if (!sessions.touchIfPresent(cmd.sessionId(), key)) {
                    if (e.isEmpty()) {
                        lockTable.remove(key, e);
                    }
                    return new LatchCountDownResult(Outcome.REJECT_SESSION, 0);
                }
                result = ((LatchEntry) e).countDown(cmd.count(), cmd.total(), cmd.sessionId(),
                        now, config.headReplyTimeoutMs(), notify);
                if (e.isEmpty()) {
                    lockTable.remove(key, e);
                }
            }
            fireNotify(notify, key);
            return result;
        }
    }

    /**
     * 原子变量操作：ATOMIC 家族的唯一命令入口（无等待、无租约、即时裁决）。
     *
     * <p><b>校验顺序</b>（首个不满足者即为结果）：会话预检（
     * {@link Outcome#REJECT_SESSION}）→ key 形状校验 → 条目定位：
     * {@code GET} 对不存在的 key 直接回 {@code (0, 0)} 且 MUST NOT 建条目；
     * 写命令对不存在的 key 懒建（竞态良性：他者抢先建条目后本请求按
     * "既有条目断言/形态判定"规则处理）→ 条目锁内家族判定（他家族 →
     * {@link Outcome#REJECT_TYPE_MISMATCH}）、会话权威复校（原子条目不入
     * 会话触及集，本复校与 {@link #sessionClosed} 无互斥义务——值的存续
     * 与会话生死无关）→ {@link AtomicEntry#op} 规则集（形态互拒、布尔值域、
     * 初值断言、去重重放、操作执行）。
     *
     * <p><b>会话登记差异</b>：与锁/Semaphore/Latch 通道不同，原子操作
     * MUST NOT 调用 {@code touchIfPresent}——值不绑定归属，会话关闭时
     * 不得遍历或扰动原子条目（条目常驻亦无回收依据）。
     *
     * <p><b>通知路径</b>：恒无——全部操作即时应答，不产生
     * {@link CoreEventListener} 事件，不入队首/等待机制。
     *
     * @param cmd 原子操作命令（会话须已登记；{@code kind} 须为三原子类型之一）
     * @return 操作结果：{@link Outcome#GRANTED} 携带应答四元组，
     *         或会话/key/家族/值域/初值类拒绝
     * @throws IllegalArgumentException {@code cmd.kind()} 非原子形态
     */
    public AtomicOpResult atomicOp(AtomicOpCommand cmd) {
        if (familyOf(cmd.kind()) != KeyFamily.ATOMIC) {
            throw new IllegalArgumentException("not an atomic kind: " + cmd.kind());
        }
        if (cmd.kind() == LockType.ATOMIC_REFERENCE) {
            throw new IllegalArgumentException(
                    "reference kind requires atomicRefOp channel");
        }
        if (!sessions.contains(cmd.sessionId())) {
            return AtomicOpResult.rejected(Outcome.REJECT_SESSION);
        }
        Outcome keyBad = validateKey(cmd.key());
        if (keyBad != null) {
            return AtomicOpResult.rejected(keyBad);
        }
        String key = cmd.key();
        while (true) {
            KeyEntry e = lockTable.get(key);
            if (e == null) {
                if (cmd.op() == AtomicOp.GET) {
                    // 读数零迁移：不存在的 key 即 (0, 0)，不建条目。
                    return new AtomicOpResult(Outcome.GRANTED, false, 0, 0, 0);
                }
                e = lockTable.computeIfAbsent(key, k -> new AtomicEntry(k, cmd.kind(),
                        cmd.initialValue()));
            }
            synchronized (e) {
                if (lockTable.get(key) != e) {
                    continue; // 条目竞态变更，重试
                }
                if (e.family() != KeyFamily.ATOMIC) {
                    return AtomicOpResult.rejected(Outcome.REJECT_TYPE_MISMATCH);
                }
                if (!(e instanceof AtomicEntry)) {
                    // 同 key 已定型为有值引用形态——标量↔引用跨形态互拒（家族同位、
                    // 条目类型不同，判例同族跨标量形态互拒）。
                    return AtomicOpResult.rejected(Outcome.REJECT_TYPE_MISMATCH);
                }
                if (!sessions.contains(cmd.sessionId())) {
                    return AtomicOpResult.rejected(Outcome.REJECT_SESSION);
                }
                return ((AtomicEntry) e).op(cmd);
            }
        }
    }

    /**
     * 有值引用原子操作：ATOMIC 家族引用形态的唯一命令入口（与
     * {@link #atomicOp} 标量通道平行——无等待、无租约、即时裁决，判定
     * 顺序与校验口径逐项同构，值域换不透明字节）。
     *
     * <p><b>校验顺序</b>（首个不满足者即为结果）：会话预检 → key 形状
     * 校验 → 条目定位：{@code GET} 对不存在的 key 直接回 {@code (null, 0)}
     * 且 MUST NOT 建条目；写命令对不存在的 key 以主张载荷懒建引用条目
     * （竞态良性：他者抢先建条目后按"既有条目断言/形态判定"规则处理）
     * → 条目锁内家族与形态判定（他家族或已定型标量形态 →
     * {@link Outcome#REJECT_TYPE_MISMATCH}）、会话权威复校 →
     * {@link AtomicRefEntry#op} 规则集（ADD 值域外拒绝、初值 presence 断言、
     * GET 短路、去重重放、操作执行）。
     *
     * <p><b>载荷尺寸</b>：本门面与条目 MUST NOT 复核载荷字节数——
     * 钳制属接入层（超限命令永不入引擎/日志），见
     * {@link AtomicRefEntry} 类注释。
     *
     * @param cmd 引用形态命令（会话须已登记；载荷数组传入后不得再修改）
     * @return 操作结果：{@link Outcome#GRANTED} 携带载荷应答四元组，
     *         或会话/key/家族/值域/初值类拒绝
     */
    public AtomicRefOpResult atomicRefOp(AtomicRefOpCommand cmd) {
        if (!sessions.contains(cmd.sessionId())) {
            return AtomicRefOpResult.rejected(Outcome.REJECT_SESSION);
        }
        Outcome keyBad = validateKey(cmd.key());
        if (keyBad != null) {
            return AtomicRefOpResult.rejected(keyBad);
        }
        String key = cmd.key();
        while (true) {
            KeyEntry e = lockTable.get(key);
            if (e == null) {
                if (cmd.op() == AtomicOp.GET) {
                    // 读数零迁移：不存在的 key 即 (null, 0)，不建条目。
                    return new AtomicRefOpResult(Outcome.GRANTED, false, null, null, 0);
                }
                e = lockTable.computeIfAbsent(key,
                        k -> new AtomicRefEntry(k, LockType.ATOMIC_REFERENCE, cmd.initial()));
            }
            synchronized (e) {
                if (lockTable.get(key) != e) {
                    continue; // 条目竞态变更，重试
                }
                if (e.family() != KeyFamily.ATOMIC) {
                    return AtomicRefOpResult.rejected(Outcome.REJECT_TYPE_MISMATCH);
                }
                if (!(e instanceof AtomicRefEntry)) {
                    // 同 key 已定型标量形态——跨形态互拒，零扰动。
                    return AtomicRefOpResult.rejected(Outcome.REJECT_TYPE_MISMATCH);
                }
                if (!sessions.contains(cmd.sessionId())) {
                    return AtomicRefOpResult.rejected(Outcome.REJECT_SESSION);
                }
                return ((AtomicRefEntry) e).op(cmd);
            }
        }
    }

    /**
     * 队列操作：QUEUE 家族唯一命令入口。判定顺序——会话存在 → key 合法 →
     * 条目定位（缺条目时分派表决定零迁移读数或定型创建）→ 家族互拒 →
     * 权威会话校验与触及登记 → 条目内判定。缺条目分派表：PEEK/SIZE/DRAIN
     * 为零迁移读数（分别回 null/0/空列表，不建条目——判例 ATOMIC GET）；
     * 非阻塞 TAKE 回 DENIED（poll 空语义，不建条目）；PUT 与阻塞 TAKE 须携
     * 非零容量主张——主张 ≤0 回 {@link Outcome#REJECT_QUEUE_CAPACITY}
     * （协调面无无界队列），非零即以该容量与形态定型创建条目（判例
     * Semaphore 总量主张建条目预检）并紧接执行本操作。触及集登记为
     * 等待/去重槽的清理路径供给（会话关闭经 removeSession 摘除双轨挂起与
     * 该会话槽），不构成元素归属表达——元素绑定 key 而非会话。唤醒收集经
     * {@code fireNotify} 在条目锁外触发（与锁/Latch/Barrier 同机制）；
     * 队列条目常驻不回收（isEmpty 恒 false，无移除分支）。
     *
     * @param cmd 队列命令（形状合法性与尺寸钳制已由接入层受理，条目与
     *            引擎 MUST NOT 复核载荷尺寸/容量上限/批量预算）
     * @return 操作结果（择用规则见 {@link QueueOpResult}）
     * @throws IllegalArgumentException {@code cmd.kind()} 非队列形态
     * @throws NullPointerException     {@code cmd} 为 null
     */
    public QueueOpResult queueOp(QueueOpCommand cmd) {
        if (familyOf(cmd.kind()) != KeyFamily.QUEUE) {
            throw new IllegalArgumentException("not a queue kind: " + cmd.kind());
        }
        long now = clock.nowMs();
        if (!sessions.contains(cmd.sessionId())) {
            return QueueOpResult.rejected(Outcome.REJECT_SESSION);
        }
        Outcome keyBad = validateKey(cmd.key());
        if (keyBad != null) {
            return QueueOpResult.rejected(keyBad);
        }
        String key = cmd.key();
        while (true) {
            KeyEntry e = lockTable.get(key);
            if (e == null) {
                switch (cmd.op()) {
                    case PEEK -> {
                        return QueueOpResult.grantedPeek(null, 0);
                    }
                    case SIZE -> {
                        return QueueOpResult.grantedSize(0, 0);
                    }
                    case DRAIN -> {
                        return QueueOpResult.grantedDrain(List.of(), 0);
                    }
                    case TAKE -> {
                        if (!cmd.blocking()) {
                            return QueueOpResult.rejected(Outcome.DENIED);
                        }
                    }
                    default -> {
                        // PUT 与阻塞 TAKE 落入下方定型创建。
                    }
                }
                if (cmd.capacity() <= 0) {
                    return QueueOpResult.rejected(Outcome.REJECT_QUEUE_CAPACITY);
                }
                e = lockTable.computeIfAbsent(key,
                        k -> QueueEntry.created(k, cmd.kind(), cmd.capacity()));
            }
            List<Waiter> notify = new ArrayList<>();
            QueueOpResult result;
            synchronized (e) {
                if (lockTable.get(key) != e) {
                    continue; // 条目竞态变更，重试
                }
                if (e.family() != KeyFamily.QUEUE || !(e instanceof QueueEntry qe)) {
                    return QueueOpResult.rejected(Outcome.REJECT_TYPE_MISMATCH);
                }
                if (!sessions.touchIfPresent(cmd.sessionId(), key)) {
                    return QueueOpResult.rejected(Outcome.REJECT_SESSION);
                }
                result = qe.op(cmd, now, config, config.headReplyTimeoutMs(), notify);
            }
            fireNotify(notify, key);
            return result;
        }
    }

    /**
     * 循环屏障到场（含旧世代重发）：BARRIER 家族的主命令入口。
     *
     * <p><b>校验顺序</b>（首个不满足者即为结果）：会话预检 → key 校验 →
     * 条目定位：key 无条目时 MUST 携带 {@code parties > 0} 定型创建（否则
     * {@link Outcome#REJECT_BARRIER_PARTIES}）；条目锁内回查存活、家族判定
     * （他家族 → {@link Outcome#REJECT_TYPE_MISMATCH}）、权威会话校验与
     * 触及登记（与 {@link #sessionClosed} 原子互斥——会话死亡时其当前世代
     * 到场记录连带破障，见 {@link BarrierEntry#removeSession}）→
     * {@link BarrierEntry#await} 规则集。条目定型后存续不回收。
     *
     * <p><b>通知路径</b>：当回合拢（无动作形态）时在本调用内收集放行广播，
     * 锁外经 {@link CoreEventListener} 触发——与 {@link #countDown} 的归零
     * 广播同机制；挂起与动作待决形态不产生通知。
     *
     * @param cmd 到场命令
     * @return 到场结果（世代号、位次与执行者标记回显见结果记录）
     */
    public BarrierAwaitResult barrierAwait(BarrierAwaitCommand cmd) {
        long now = clock.nowMs();
        if (!sessions.contains(cmd.sessionId())) {
            return BarrierAwaitResult.rejected(Outcome.REJECT_SESSION, 0);
        }
        Outcome keyBad = validateKey(cmd.key());
        if (keyBad != null) {
            return BarrierAwaitResult.rejected(keyBad, 0);
        }
        String key = cmd.key();
        while (true) {
            KeyEntry e = lockTable.get(key);
            if (e == null) {
                if (cmd.parties() <= 0) {
                    return BarrierAwaitResult.rejected(Outcome.REJECT_BARRIER_PARTIES, 0);
                }
                e = lockTable.computeIfAbsent(key, k -> new BarrierEntry(k, cmd.parties()));
            }
            List<Waiter> notify = new ArrayList<>();
            BarrierAwaitResult result;
            synchronized (e) {
                if (lockTable.get(key) != e) {
                    continue; // 条目竞态移除，重试
                }
                if (e.family() != KeyFamily.BARRIER) {
                    return BarrierAwaitResult.rejected(Outcome.REJECT_TYPE_MISMATCH, 0);
                }
                if (!sessions.touchIfPresent(cmd.sessionId(), key)) {
                    if (e.isEmpty()) {
                        lockTable.remove(key, e);
                    }
                    return BarrierAwaitResult.rejected(Outcome.REJECT_SESSION, 0);
                }
                result = ((BarrierEntry) e).await(cmd, now, config,
                        config.headReplyTimeoutMs(), notify);
            }
            fireNotify(notify, key);
            return result;
        }
    }

    /**
     * 循环屏障离场（超时/中断/显式破障）：条目定位与会话校验同
     * {@link #barrierAwait}；破障通知在条目锁外触发。key 无条目时为幂等
     * 无操作（纯破障主张对不存在的屏障无对象可破）；他家族条目回
     * {@link Outcome#REJECT_TYPE_MISMATCH}（零扰动）。
     *
     * @param cmd 离场命令
     * @return 离场结果（恒 {@link Outcome#GRANTED} 或拒绝类）
     */
    public BarrierLeaveResult barrierLeave(BarrierLeaveCommand cmd) {
        long now = clock.nowMs();
        if (!sessions.contains(cmd.sessionId())) {
            return BarrierLeaveResult.rejected(Outcome.REJECT_SESSION);
        }
        Outcome keyBad = validateKey(cmd.key());
        if (keyBad != null) {
            return BarrierLeaveResult.rejected(keyBad);
        }
        String key = cmd.key();
        KeyEntry e = lockTable.get(key);
        if (e == null) {
            return BarrierLeaveResult.ok(); // 无条目：纯破障主张幂等无操作
        }
        List<Waiter> notify = new ArrayList<>();
        BarrierLeaveResult result;
        synchronized (e) {
            if (lockTable.get(key) != e) {
                // 条目竞态移除：无到场记录即无世代可破，幂等无操作（与
                // "条目不存在"同口径；到场账簿随条目存续）。
                return BarrierLeaveResult.ok();
            }
            if (e.family() != KeyFamily.BARRIER) {
                return BarrierLeaveResult.rejected(Outcome.REJECT_TYPE_MISMATCH);
            }
            if (!sessions.contains(cmd.sessionId())) {
                return BarrierLeaveResult.rejected(Outcome.REJECT_SESSION);
            }
            result = ((BarrierEntry) e).leave(cmd, now, config.headReplyTimeoutMs(), notify);
        }
        fireNotify(notify, key);
        return result;
    }

    /**
     * 循环屏障动作了结（执行者回报）：条目定位与会话校验同
     * {@link #barrierAwait}；合拢生效时放行广播在条目锁外触发。key 无
     * 条目或家族不符分别回 {@link Outcome#REJECT_BARRIER_ACTION} /
     * {@link Outcome#REJECT_TYPE_MISMATCH}。
     *
     * @param cmd 了结命令（会话 + 世代号）
     * @return 了结结果（GRANTED=合拢生效或幂等重复；BARRIER_BROKEN=世代
     *         已破；REJECT_BARRIER_ACTION=回报不被受理）
     */
    public BarrierActionDoneResult barrierActionDone(BarrierActionDoneCommand cmd) {
        long now = clock.nowMs();
        if (!sessions.contains(cmd.sessionId())) {
            return new BarrierActionDoneResult(Outcome.REJECT_SESSION, 0, false);
        }
        Outcome keyBad = validateKey(cmd.key());
        if (keyBad != null) {
            return new BarrierActionDoneResult(keyBad, 0, false);
        }
        String key = cmd.key();
        KeyEntry e = lockTable.get(key);
        if (e == null) {
            return BarrierActionDoneResult.rejected();
        }
        List<Waiter> notify = new ArrayList<>();
        BarrierActionDoneResult result;
        synchronized (e) {
            if (lockTable.get(key) != e) {
                return BarrierActionDoneResult.rejected();
            }
            if (e.family() != KeyFamily.BARRIER) {
                return new BarrierActionDoneResult(Outcome.REJECT_TYPE_MISMATCH, 0, false);
            }
            if (!sessions.contains(cmd.sessionId())) {
                return new BarrierActionDoneResult(Outcome.REJECT_SESSION, 0, false);
            }
            result = ((BarrierEntry) e).actionDone(cmd, now, config.headReplyTimeoutMs(), notify);
        }
        fireNotify(notify, key);
        return result;
    }

    /**
     * BARRIER 条目复制态导出（影子表镜像与跨副本摘要的权威读口）：
     * 条目锁内一次拷贝；条目不存在、家族不符或已被回收返回
     * {@code null}（调用方按"镜像零扰动"处理）。只读，MUST NOT
     * 改变任何状态。
     *
     * @param key 屏障键
     * @return 复制态快照；非 BARRIER 条目或不存在为 {@code null}
     */
    public BarrierEntry.ReplicatedState barrierReplicatedState(String key) {
        if (key == null) {
            return null;
        }
        KeyEntry e = lockTable.get(key);
        if (e == null) {
            return null;
        }
        synchronized (e) {
            if (lockTable.get(key) != e) {
                return null;
            }
            return e instanceof BarrierEntry be ? be.replicatedState() : null;
        }
    }

    /**
     * 队列条目的复制态导出（v7，判例 {@link #barrierReplicatedState}）：
     * 形态/容量/元素列表（队列序）/每会话去重槽表（会话升序）的自包含
     * 快照，供影子表镜像与快照序列化消费。条目锁内拷贝，弱一致于条目间、
     * 自洽于条目内。
     *
     * @param key 队列键
     * @return 复制态快照；非队列条目或不存在为 {@code null}
     */
    public QueueEntry.ReplicatedState queueReplicatedState(String key) {
        if (key == null) {
            return null;
        }
        KeyEntry e = lockTable.get(key);
        if (e == null) {
            return null;
        }
        synchronized (e) {
            if (lockTable.get(key) != e) {
                return null;
            }
            return e instanceof QueueEntry qe ? qe.replicatedState() : null;
        }
    }

    /**
     * 相位器操作（受理通道）：校验顺序为会话 → key → 条目定位（REGISTER
     * 无条目即创建，判例 v7 {@code queueOp} 分派表——非 REGISTER 命中不
     * 存在条目回 {@link Outcome#REJECT_PHASER_NO_ENTRY}，MUST NOT 隐式
     * 建条目）→ 家族判定 → 会话触及 → 条目规则。配额上限
     * （{@code max-parties-per-phaser}）的判定在<b>本通道</b>完成（受理点
     * 唯一裁决，判例"钳制属接入层"——单机形态即本门面、集群形态即 Leader
     * 受理预检）；条目应用侧（{@link #phaserApply}）不消费该上限。
     * 等待集登记、唤醒收集与深度护栏（{@code maxQueueDepthPerKey} 合并
     * 口径）在条目锁内完成，唤醒经事件出口在条目锁外触发。
     *
     * @param cmd 相位器命令（形状与尺寸已由接入层受理）
     * @return 操作结果（三计数为裁决时刻账簿快照）
     */
    public PhaserOpResult phaserOp(PhaserOpCommand cmd) {
        return phaserOp(cmd, true);
    }

    /**
     * 相位器操作（复制应用通道）：集群状态机应用点专用——除 MUST NOT
     * 执行配额上限判定外与 {@link #phaserOp} 逐项同判（已提交条目在任何
     * 节点配置下照常回放，账簿跨副本确定，判例 v7 队列"容量不复核"纪律）。
     * {@code ARRIVE_AND_AWAIT} 的等待半程在本通道 MUST NOT 登记条目内
     * （"集群引擎恒不登记等待项"不变式，判例 acquire 的排队裁决落点与
     * v9 条件双拓扑）：回执 QUEUED 后由 Leader 侧网关在应用副作用中登记
     * {@code PhaserRegistry}（等待簿记与推送投递归服务端，账簿归条目）。
     *
     * @param cmd 相位器命令
     * @return 操作结果
     */
    public PhaserOpResult phaserApply(PhaserOpCommand cmd) {
        return phaserOp(cmd, false);
    }

    /**
     * 相位器通道实现（受理/应用两路共用，{@code enforceCap} 判别是否
     * 执行配额上限——唯一分岔点，其余规则严格同源）。
     *
     * @param cmd        命令
     * @param enforceCap 是否受理配额上限判定
     * @return 操作结果
     */
    private PhaserOpResult phaserOp(PhaserOpCommand cmd, boolean enforceCap) {
        long now = clock.nowMs();
        if (!sessions.contains(cmd.sessionId())) {
            return PhaserOpResult.rejected(Outcome.REJECT_SESSION);
        }
        Outcome keyBad = validateKey(cmd.key());
        if (keyBad != null) {
            return PhaserOpResult.rejected(keyBad);
        }
        String key = cmd.key();
        while (true) {
            KeyEntry e = lockTable.get(key);
            if (e == null) {
                if (cmd.op() != PhaserOpType.REGISTER) {
                    return PhaserOpResult.rejected(Outcome.REJECT_PHASER_NO_ENTRY);
                }
                if (enforceCap && cmd.parties() > config.maxPartiesPerPhaser()) {
                    return PhaserOpResult.rejected(Outcome.REJECT_PHASER_PARTIES);
                }
                e = lockTable.computeIfAbsent(key, PhaserEntry::new);
            }
            List<Waiter> notify = new ArrayList<>();
            PhaserOpResult result;
            synchronized (e) {
                if (lockTable.get(key) != e) {
                    continue; // 条目竞态变更，重试
                }
                if (!(e instanceof PhaserEntry pe)) {
                    return PhaserOpResult.rejected(Outcome.REJECT_TYPE_MISMATCH);
                }
                if (!sessions.touchIfPresent(cmd.sessionId(), key)) {
                    return PhaserOpResult.rejected(Outcome.REJECT_SESSION);
                }
                if (enforceCap && cmd.op() == PhaserOpType.REGISTER
                        && (long) pe.registered() + cmd.parties() > config.maxPartiesPerPhaser()) {
                    return PhaserOpResult.rejected(Outcome.REJECT_PHASER_PARTIES);
                }
                result = pe.phaserOp(cmd, now, config, notify, !enforceCap);
            }
            fireNotify(notify, key);
            return result;
        }
    }

    /**
     * 相位器条目复制态导出（影子表镜像与跨副本摘要的权威读口，判例
     * {@link #barrierReplicatedState}）。条目锁内一次拷贝；不存在或非
     * PHASER 家族返回 {@code null}。只读，MUST NOT 改变任何状态。
     *
     * @param key 相位器键
     * @return 复制态快照；非 PHASER 条目或不存在为 {@code null}
     */
    public PhaserEntry.ReplicatedState phaserReplicatedState(String key) {
        if (key == null) {
            return null;
        }
        KeyEntry e = lockTable.get(key);
        if (e == null) {
            return null;
        }
        synchronized (e) {
            if (lockTable.get(key) != e) {
                return null;
            }
            return e instanceof PhaserEntry pe ? pe.replicatedState() : null;
        }
    }

    /**
     * 单 key 相位器挂起等待明细（Leader 本地视图；无条目或非 PHASER
     * 家族为空列表，弱一致读数、零扰动）。
     *
     * @param key 相位器键
     * @return 等待项视图列表（登记到达序）
     */
    public List<PhaserEntry.WaiterView> phaserWaiters(String key) {
        KeyEntry e = key == null ? null : lockTable.get(key);
        return e instanceof PhaserEntry pe ? pe.waitersSnapshot() : List.of();
    }

    /**
     * 单 key 相位器注册配额明细（复制态账簿视图；无条目或非 PHASER
     * 家族为空列表，弱一致读数、零扰动）。
     *
     * @param key 相位器键
     * @return 配额行列表（账簿插入序=注册先后）
     */
    public List<PhaserEntry.PartyView> phaserParties(String key) {
        KeyEntry e = key == null ? null : lockTable.get(key);
        return e instanceof PhaserEntry pe ? pe.partiesSnapshot() : List.of();
    }

    /**
     * 抓取时刻单键注册 party 峰值（{@code phaser.parties.registered.max}
     * gauge 单机口径；集群形态读 Leader 本地镜像，同"单键峰值、抓取时刻
     * 采样"语义）。弱一致遍历、纯读零扰动；MUST NOT 用于任何裁决路径。
     *
     * @return 各 phaser key 注册总数的最大值；无条目为 0
     */
    public int maxPhaserRegistered() {
        int max = 0;
        for (KeyEntry e : lockTable.values()) {
            if (e instanceof PhaserEntry pe) {
                int n = pe.registered();
                if (n > max) {
                    max = n;
                }
            }
        }
        return max;
    }

    /**
     * key 形状校验的共享出口：空与超长分别回
     * {@link Outcome#REJECT_KEY_EMPTY} / {@link Outcome#REJECT_KEY_TOO_LONG}，
     * 合法返回 {@code null}。
     *
     * @param key 待校验键
     * @return 拒绝结果；合法为 {@code null}
     */
    private Outcome validateKey(String key) {
        if (key == null || key.isEmpty()) {
            return Outcome.REJECT_KEY_EMPTY;
        }
        if (key.getBytes(StandardCharsets.UTF_8).length > config.maxKeyLength()) {
            return Outcome.REJECT_KEY_TOO_LONG;
        }
        return null;
    }

    /**
     * 到期扫描：强制释放所有已过期租约并对被释放 key 触发队首通知。
     * 这是惰性到期契约的执行端——租约到期本身不触发任何动作，须由调用方
     * （生产环境为租约扫描线程）周期性调用本方法回收。
     *
     * <p><b>陈旧校验</b>：到期堆只入不删（释放/续租均不删堆记录），
     * 故取出的堆记录可能已陈旧。仅当堆记录的凭证与到期时刻均与条目
     * 当前值一致时才执行强制释放；续租或重新授予后的旧记录因凭证或
     * 时刻已变而被安全跳过，不会误杀新租约。
     *
     * <p>强制释放清除该 key 的全部持有者（写侧与读侧）与租约，语义见
     * {@link LockEntry#forceExpire}。
     *
     * @return 本次因到期释放的锁数量
     */
    public int expireDue() {
        long now = clock.nowMs();
        int count = 0;
        for (LeaseManager.HeapEntry he : leaseManager.drainExpired(now)) {
            KeyEntry e = lockTable.get(he.key());
            if (e == null) {
                continue;
            }
            List<Waiter> notify = new ArrayList<>();
            synchronized (e) {
                // 陈旧校验：堆记录的凭证与到期时刻均与条目当前值一致才视为有效。
                if (e.leaseToken() == he.leaseToken() && e.leaseExpiresAtMs() == he.expiresAtMs()) {
                    e.forceExpire(now, config.headReplyTimeoutMs(), notify);
                    count++;
                    if (e.isEmpty()) {
                        lockTable.remove(he.key(), e);
                    }
                }
            }
            fireNotify(notify, he.key());
        }
        return count;
    }

    /**
     * 只读统计观察面：弱一致遍历
     * {@link LockTable} 全部条目聚合 held/等待者/队深与登记会话数。
     *
     * <p><b>held 判据</b>：条目锁内读 {@link KeyEntry#leaseToken()}，非零
     * 即"当前持有中"——锁与 Semaphore 无持有者时凭证归零（{@code clearLease}），
     * Latch 恒零（契约读数占位），LATCH 家族因此天然排除。条目不携带单一
     * 协议类型（REENTRANT/SIMPLE/FAIR 同族互通、READ/WRITE 是请求维度），
     * 聚合维度取家族。
     *
     * <p><b>并发语义</b>：每条目一次条目锁内读数（与写路径的互斥粒度一致、
     * 持锁时长为常数），条目间无原子性——返回值是采样时刻的弱一致快照；
     * MUST NOT 据此做授予判定，仅供观测。本方法 MUST NOT 改变引擎任何状态。
     *
     * @return 统计快照（不可变值对象）
     */
    public CoreStats stats() {
        int heldLocks = 0;
        int heldSemaphores = 0;
        int totalWaiters = 0;
        int maxQueueDepth = 0;
        for (KeyEntry e : lockTable.values()) {
            int waiters;
            int conditionWaiters = 0;
            boolean held;
            synchronized (e) {
                waiters = e.waiterCount();
                // v9 口径：条件等待者是等待者——计入等待总数合计；
                // maxQueueDepth 保持"等待队列深度"纯口径不抬升（四口径互引，
                // metrics-observability 能力条款）。
                if (e instanceof LockEntry le) {
                    conditionWaiters = le.conditionWaiterCount();
                }
                held = e.leaseToken() != 0;
            }
            if (held) {
                switch (e.family()) {
                    case LOCK -> heldLocks++;
                    case SEMAPHORE -> heldSemaphores++;
                    default -> {
                        // LATCH/ATOMIC/BARRIER/QUEUE 无 held 语义（leaseToken 恒 0，此分支不可达，防御占位）
                    }
                }
            }
            totalWaiters += waiters + conditionWaiters;
            if (waiters > maxQueueDepth) {
                maxQueueDepth = waiters;
            }
        }
        return new CoreStats(heldLocks, heldSemaphores, totalWaiters, maxQueueDepth, sessions.size());
    }

    /**
     * 抓取时刻单键条件等待峰值（单机形态 {@code condition.waiters.max} gauge
     * 口径；集群形态读 Leader 本地登记表 {@code ConditionRegistry.maxCountCurrent()}，
     * 两形态同"单键峰值、抓取时刻采样"语义）。弱一致遍历、纯读零扰动。
     *
     * @return 各锁 key 条件等待数的最大值；无登记为 0
     */
    public int maxConditionWaiters() {
        int max = 0;
        for (KeyEntry e : lockTable.values()) {
            if (e instanceof LockEntry le) {
                int n = le.conditionWaiterCount();
                if (n > max) {
                    max = n;
                }
            }
        }
        return max;
    }

    /**
     * 队列元素深度观察读数（v7，{@code elements.depth.max} gauge 单机口径）：
     * 全部队列 key 的当前元素数最大值（驻留口径，延时形态含未到期项）。
     * 弱一致遍历（逐条目锁内读深度、条目间无全局原子性），MUST NOT 用于
     * 任何裁决路径。
     *
     * @return 单键最大元素深度；无队列返回 0
     */
    public int maxElementsDepth() {
        int max = 0;
        for (KeyEntry e : lockTable.values()) {
            if (e instanceof QueueEntry qe) {
                int d = qe.depth();
                if (d > max) {
                    max = d;
                }
            }
        }
        return max;
    }

    /**
     * 队列就绪唤醒（v7，单机形态延时扫描的引擎入口）：逐队列条目在条目
     * 锁内尝试唤醒 take 轨队首（DELAY 形态要求队首到期不晚于引擎时钟、
     * QUEUE 形态要求存在可消费元素——谓词收口在条目内），收集的通知经
     * 事件出口在条目锁外触发。由服务端调度线程周期调用（集群形态由
     * Leader 侧就绪驱动经影子表判定，不经本方法）。
     *
     * @return 本轮唤醒的等待项数
     */
    public int wakeQueueReady() {
        long now = clock.nowMs();
        int count = 0;
        for (KeyEntry e : lockTable.values()) {
            if (!(e instanceof QueueEntry qe)) {
                continue;
            }
            List<Waiter> notify = new ArrayList<>();
            synchronized (qe) {
                count += qe.wakeReadyHeads(now, config.headReplyTimeoutMs(), notify);
            }
            fireNotify(notify, e.key());
        }
        return count;
    }

    /**
     * 明细只读观察面：弱一致遍历
     * {@link LockTable} 全部条目，逐条目在条目锁内产出不可变明细快照
     * （持有者/等待队列/租约/许可/屏障计数），聚合为 {@link CoreInspection}。
     *
     * <p><b>并发语义</b>：与 {@link #stats()} 同一纪律——每条目一次条目锁内
     * 拷贝（持锁时长为字段复制级别，与写路径互斥粒度一致）、条目间无原子性；
     * 返回值是采样时刻的弱一致快照，单条目内部自洽（位次连续、持有列表
     * 与租约读数同锁内取得）。MUST NOT 据此做授予判定，仅供上层管理协议
     * 装配观察应答；本方法 MUST NOT 改变引擎任何状态。
     *
     * <p><b>时钟口径</b>：一次性读取 {@code clock.nowMs()} 作为采样基准，
     * 各条目的已等待时长与剩余租约均以该时刻折算。
     *
     * @return 明细快照（不可变值对象，条目遍历序）
     */
    public CoreInspection inspect() {
        long now = clock.nowMs();
        List<CoreInspection.KeySnapshot> snapshots = new ArrayList<>();
        for (KeyEntry e : lockTable.values()) {
            synchronized (e) {
                // 家族分派与命令路径同构：各条目类的 snapshot 在条目锁内拷贝
                // 自身状态；default 为家族新增但观察面未接入时的防御收口。
                switch (e) {
                    case LockEntry le -> snapshots.add(le.snapshot(now));
                    case SemaphoreEntry se -> snapshots.add(se.snapshot(now));
                    case LatchEntry la -> snapshots.add(la.snapshot(now));
                    case AtomicEntry ae -> snapshots.add(ae.snapshot(now));
                    case AtomicRefEntry re -> snapshots.add(re.snapshot(now));
                    case BarrierEntry be -> snapshots.add(be.snapshot(now));
                    case QueueEntry qe -> snapshots.add(qe.snapshot(now));
                    case PhaserEntry pe -> snapshots.add(pe.snapshot(now));
                    default -> {
                        // 未知实现不入快照（理论不可达：条目类已穷尽）。
                    }
                }
            }
        }
        return new CoreInspection(List.copyOf(snapshots), sessions.size(), now);
    }

    /**
     * 单 key 的明细只读快照（{@link #inspect()} 的定点形态，供管理协议
     * {@code ADMIN_KEY_DETAIL} 装配）：条目锁内拷贝，弱一致、纯读、零扰动。
     *
     * @param key 锁键（{@code null} 视为不存在）
     * @return 该条目的自洽快照；条目不存在（含已被回收）返回 {@code null}
     */
    public CoreInspection.KeySnapshot inspectKey(String key) {
        if (key == null) {
            return null;
        }
        KeyEntry e = lockTable.get(key);
        if (e == null) {
            return null;
        }
        long now = clock.nowMs();
        synchronized (e) {
            // 条目可能在取锁瞬间被回收（isEmpty 移除路径）：回查成员身份，
            // 与命令路径同纪律：非当前值即视作不存在。
            if (lockTable.get(key) != e) {
                return null;
            }
            return switch (e) {
                case LockEntry le -> le.snapshot(now);
                case SemaphoreEntry se -> se.snapshot(now);
                case LatchEntry la -> la.snapshot(now);
                case AtomicEntry ae -> ae.snapshot(now);
                case AtomicRefEntry re -> re.snapshot(now);
                case BarrierEntry be -> be.snapshot(now);
                case QueueEntry qe -> qe.snapshot(now);
                case PhaserEntry pe -> pe.snapshot(now);
                default -> null;
            };
        }
    }

    /**
     * 队首响应超时清扫：移除"已通知但在 {@code headReplyTimeoutMs} 内
     * 未重发获取请求"的队首等待者，并对新队首补发通知。
     *
     * <p>这是 {@code AWAIT_NOTIFY} 推送丢失（连接断开、写出失败等）的兜底：
     * 队首被通知后进入"待重发"状态并记录响应截止时刻；截止前重发则正常
     * 授予，超时未重发则视为放弃，出队并让下一个等待者获得通知机会。
     *
     * @return 本次因超时移除的队首数量
     */
    public int sweepNotifiedHeads() {
        long now = clock.nowMs();
        int count = 0;
        for (KeyEntry e : lockTable.values()) {
            List<Waiter> notify = new ArrayList<>();
            synchronized (e) {
                if (e.sweepNotifiedHead(now, config.headReplyTimeoutMs(), notify)) {
                    count++;
                    if (e.isEmpty()) {
                        lockTable.remove(e.key(), e);
                    }
                }
            }
            fireNotify(notify, e.key());
        }
        return count;
    }

    /**
     * 租约夹取：请求租约为 0 时替换为默认租约，再夹取到
     * {@code [minLeaseMs, maxLeaseMs]} 之间。授予与续租共用此规则，
     * 保证实际生效租约永远在配置限额内。
     *
     * @param requested 请求的租约时长（毫秒），{@code 0} 表示使用默认租约
     * @return 实际生效的租约时长（毫秒）
     */
    private long clampLease(long requested) {
        long v = requested == 0 ? config.defaultLeaseMs() : requested;
        return Math.max(config.minLeaseMs(), Math.min(config.maxLeaseMs(), v));
    }

    /**
     * 在条目锁之外统一触发收集到的队首通知事件。通知列表由条目内操作
     * （释放、到期、清扫等）在持锁期间填充，本方法负责出锁后逐个回调，
     * 避免回调实现中的任何行为反向影响条目锁的持有。
     *
     * 条件 signal 家族操作：condition 家族命令唯一门面（v9）。零复制日志——
     * 三操作是纯 Leader 本地裁决的搬运/摘除（判例双源："等待不入日志"与 topic
     * 零日志豁免，类目化为"signal 是事件不是状态"），MUST NOT 进入状态机提交
     * 路径与提交通道（边界条款由 replicated-state-machine 能力承载；await 的
     * 释放半程另经折叠 ACQUIRE 条目走既有提交通道，见 {@link #acquire}）。
     * 判定顺序——会话存在 → key 合法 → 条件名合法（接入层唯一裁决，此处防御
     * 兜底）→ 条目定位（无条目：LEAVE→OK 幂等无操作；SIGNAL/SIGNAL_ALL→
     * NOT_HELD）→ 家族判定（非 LOCK→REJECT_TYPE_MISMATCH，LEAVE 同口径统一
     * 家族门）→ 条目锁内分派 op → 通知列表在条目锁外统一触发。
     *
     * @param cmd signal 家族命令（形状已经接入层裁决）
     * @return 即时回执（OK / NOT_HELD / REJECT_*），恒无挂起形态
     */
    public ConditionOpResult conditionOp(ConditionOpCommand cmd) {
        long now = clock.nowMs();
        ConditionOpResult.ConditionOpEcho echo = switch (cmd.op()) {
            case SIGNAL -> ConditionOpResult.ConditionOpEcho.SIGNAL;
            case SIGNAL_ALL -> ConditionOpResult.ConditionOpEcho.SIGNAL_ALL;
            case LEAVE -> ConditionOpResult.ConditionOpEcho.LEAVE;
        };
        if (!sessions.contains(cmd.sessionId())) {
            return ConditionOpResult.rejected(ConditionOpResult.Status.REJECT_SESSION, echo);
        }
        Outcome keyBad = validateKey(cmd.key());
        if (keyBad != null) {
            return ConditionOpResult.rejected(keyBad == Outcome.REJECT_KEY_TOO_LONG
                    ? ConditionOpResult.Status.REJECT_KEY_TOO_LONG
                    : ConditionOpResult.Status.REJECT_KEY_EMPTY, echo);
        }
        if (cmd.condition() == null || cmd.condition().isEmpty()) {
            return ConditionOpResult.rejected(ConditionOpResult.Status.REJECT_KEY_EMPTY, echo);
        }
        if (cmd.condition().getBytes(StandardCharsets.UTF_8).length > config.maxKeyLength()) {
            return ConditionOpResult.rejected(ConditionOpResult.Status.REJECT_KEY_TOO_LONG, echo);
        }
        KeyEntry e = lockTable.get(cmd.key());
        if (e == null) {
            // 无条目即无登记：LEAVE 幂等无操作；signal 家族权限前提（持有归属）不存在。
            return cmd.op() == ConditionOp.LEAVE
                    ? ConditionOpResult.ok(echo)
                    : ConditionOpResult.rejected(ConditionOpResult.Status.NOT_HELD, echo);
        }
        List<Waiter> notify = new ArrayList<>();
        ConditionOpResult result;
        synchronized (e) {
            if (lockTable.get(cmd.key()) != e) {
                // 条目竞态移除：与"无条目"同口径收束。
                result = cmd.op() == ConditionOp.LEAVE
                        ? ConditionOpResult.ok(echo)
                        : ConditionOpResult.rejected(ConditionOpResult.Status.NOT_HELD, echo);
            } else if (e.family() != KeyFamily.LOCK) {
                result = ConditionOpResult.rejected(
                        ConditionOpResult.Status.REJECT_TYPE_MISMATCH, echo);
            } else {
                LockEntry le = (LockEntry) e;
                result = switch (cmd.op()) {
                    case SIGNAL -> le.signal(cmd, now, config, notify);
                    case SIGNAL_ALL -> le.signalAll(cmd, now, config, notify);
                    case LEAVE -> le.leave(cmd);
                };
            }
        }
        if (!notify.isEmpty()) {
            fireNotify(notify, cmd.key());
        }
        return result;
    }

    /**
     * await 折叠的释放半程应用（集群形态 LOCK_ACQUIRE_ENTRY 携带 condition 的
     * 应用点入口，v9）：跨副本确定地执行 {@link LockEntry#awaitFoldRelease}
     * （持有归属一步清零并清租约+队首通知评估；无条目/非持有零操作幂等），
     * 叠加会话/key/家族守卫。登记半程 MUST NOT 在此发生——它由 Leader 受理
     * 预检点的本地结构承载（"登记先于释放可见"不变式，lock-server 能力条款），
     * 应用面无本地副作用可分岔（digest 守卫由 replicated-state-machine 能力钉定）。
     *
     * @param cmd 折叠获取命令（{@code condition} 非空；形状已由接入层裁决）
     * @return 守卫结果与是否发生释放；拒绝形态含会话失效/家族不匹配
     */
    public AwaitReleaseResult awaitFoldRelease(AcquireCommand cmd) {
        long now = clock.nowMs();
        if (!sessions.contains(cmd.sessionId())) {
            return new AwaitReleaseResult(Outcome.REJECT_SESSION, false);
        }
        Outcome keyBad = validateKey(cmd.key());
        if (keyBad != null) {
            return new AwaitReleaseResult(keyBad, false);
        }
        if (cmd.lockType() == LockType.READ || cmd.lockType() == LockType.WRITE
                || familyOf(cmd.lockType()) != KeyFamily.LOCK) {
            return new AwaitReleaseResult(Outcome.REJECT_TYPE_MISMATCH, false);
        }
        KeyEntry e = lockTable.get(cmd.key());
        if (e == null) {
            return new AwaitReleaseResult(Outcome.GRANTED, false);
        }
        List<Waiter> notify = new ArrayList<>();
        boolean released;
        synchronized (e) {
            if (lockTable.get(cmd.key()) != e) {
                return new AwaitReleaseResult(Outcome.GRANTED, false);
            }
            if (e.family() != KeyFamily.LOCK) {
                return new AwaitReleaseResult(Outcome.REJECT_TYPE_MISMATCH, false);
            }
            released = ((LockEntry) e).awaitFoldRelease(cmd.sessionId(), cmd.threadId(),
                    now, config, notify);
        }
        if (!notify.isEmpty()) {
            fireNotify(notify, cmd.key());
        }
        return new AwaitReleaseResult(Outcome.GRANTED, released);
    }

    /**
     * 单 key 条件等待者计数观察（管理/指标口径；无条目或非 LOCK 家族为 0，
     * 弱一致读数）。与等待队列口径（{@link #stats}/{@code waiterCount}）互斥
     * 不重复——已搬运项属队列口径。
     *
     * @param key 锁键
     * @return 当前条件等待者数
     */
    public int conditionWaiterCount(String key) {
        KeyEntry e = lockTable.get(key);
        return e instanceof LockEntry le ? le.conditionWaiterCount() : 0;
    }

    /**
     * 单 key 条件等待明细只读快照（各集按建立序、集内按到达序；无条目或非
     * LOCK 家族为空列表，弱一致读数、零扰动）。
     *
     * @param key 锁键
     * @return 条件等待者明细视图列表
     */
    public List<LockEntry.ConditionWaiterView> conditionWaiters(String key) {
        KeyEntry e = lockTable.get(key);
        return e instanceof LockEntry le ? le.conditionWaiterViews() : List.of();
    }

    /**
     * 队首通知统一触发：在条目锁外逐项投递监听器。
     *
     * @param notify 待通知的队首等待者列表
     * @param key    锁键，随事件一并报告
     */
    private void fireNotify(List<Waiter> notify, String key) {
        for (Waiter w : notify) {
            listener.notifyHead(w.sessionId(), w.requestId(), key);
        }
    }
}
