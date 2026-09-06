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
import io.github.lamspace.openlatch.core.command.LatchAwaitCommand;
import io.github.lamspace.openlatch.core.command.LatchCountDownCommand;
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.command.RenewCommand;
import io.github.lamspace.openlatch.core.lease.LeaseManager;
import io.github.lamspace.openlatch.core.lock.KeyEntry;
import io.github.lamspace.openlatch.core.lock.LatchEntry;
import io.github.lamspace.openlatch.core.lock.LockEntry;
import io.github.lamspace.openlatch.core.lock.LockTable;
import io.github.lamspace.openlatch.core.lock.SemaphoreEntry;
import io.github.lamspace.openlatch.core.lock.Owner;
import io.github.lamspace.openlatch.core.lock.Waiter;
import io.github.lamspace.openlatch.core.snapshot.CoreStateRestore;
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.LatchAwaitResult;
import io.github.lamspace.openlatch.core.result.LatchCountDownResult;
import io.github.lamspace.openlatch.core.result.Outcome;
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
    /** key → 状态条目映射与条目生命周期（Phase 3 T1 后按家族承载锁/Semaphore/Latch 条目）。 */
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
     * 快照状态重建（详设 §7.1，S4/design D1）：以 {@link CoreStateRestore}
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
     *       （计数直写、无租约）——均直写快照原值，不经状态迁移规则、不经
     *       {@link Clock}，等待队列恒空；</li>
     *   <li>会话登记：{@code sessions} 全集逐个登记（内部 sid 由调用方在
     *       构造前经 {@link #sessionOpened()} 预生成亦可——本方法幂等于
     *       登记表 {@code putIfAbsent} 语义）；持有者所属会话触及的 key
     *       一并登记，使 {@link #sessionClosed} 对继承持有的清理完整；</li>
     *   <li>到期堆回填：每个继承条目按（key、凭证、到期时刻）offer 堆记录，
     *       使 {@link #expireDue} 能回收快照继承的租约（spec"到期扫描覆盖
     *       继承租约"；陈旧校验语义与原生路径相同）；</li>
     *   <li>发号水位：租约凭证发号器置为输入的 {@code nextLeaseToken}
     *       （快照内已发出的最大凭证 +1 起），后续授予既不复用继承凭证
     *       （spec"发号不复用继承凭证"），也与未截断副本对同一尾部日志
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
     * 本读数的凭证。供快照生成侧读取发号水位（详设 §7.1 / s4 design D10），
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
     * @param sessionId 要关闭的会话
     */
    public void sessionClosed(long sessionId) {
        Set<String> keys = sessions.remove(sessionId);
        if (keys == null) {
            return;
        }
        long now = clock.nowMs();
        for (String key : keys) {
            KeyEntry e = lockTable.get(key);
            if (e == null) {
                continue;
            }
            List<Waiter> notify = new ArrayList<>();
            synchronized (e) {
                e.removeSession(sessionId, now, config.headReplyTimeoutMs(), notify);
                if (e.isEmpty()) {
                    lockTable.remove(key, e);
                }
            }
            fireNotify(notify, key);
        }
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

        // LATCH 不经获取通道（详设 §2.1"走独立通道"）：ACQUIRE 携带 LATCH
        // 类型属请求形状错误，协议层 v3 门控之后由本守卫兜底。
        if (cmd.lockType() == LockType.LATCH) {
            return new AcquireResult(Outcome.REJECT_TYPE_MISMATCH, 0, 0, 0);
        }
        KeyFamily family = familyOf(cmd.lockType());
        // Semaphore 建条目预检：条目不存在时总量主张必须 > 0（design D1）。
        // 竞态良性：他者抢先建条目后本请求按"既有条目断言"规则处理。
        if (family == KeyFamily.SEMAPHORE && cmd.permitsTotal() <= 0 && lockTable.get(key) == null) {
            return new AcquireResult(Outcome.REJECT_SEMAPHORE_TOTAL, 0, 0, 0);
        }
        boolean reentrant = cmd.lockType() != LockType.SIMPLE;
        long effectiveLeaseMs = clampLease(cmd.requestedLeaseMs());

        while (true) {
            KeyEntry e = lockTable.computeIfAbsent(key, k -> newEntry(family, k, reentrant, cmd));
            synchronized (e) {
                if (lockTable.get(key) != e) {
                    continue; // 条目在等待期间被移除，重试（design.md D4）
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
                // 条目内规则按实现类分派（锁规则集 / Semaphore 规则集，详设 §2.3）。
                AcquireResult result = switch (e) {
                    case LockEntry le -> le.acquire(cmd, now, leaseTokenCounter::getAndIncrement,
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
                return result;
            }
        }
    }

    /**
     * 请求锁类型 → 条目家族。锁家族全部类型（REENTRANT/SIMPLE/READ/WRITE，
     * 及 Phase 3 起的 FAIR 别名）落 {@link KeyFamily#LOCK}；SEMAPHORE/LATCH
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
        };
    }

    /**
     * 按家族创建条目。锁家族条目构造与既有 {@code new LockEntry(k, reentrant)}
     * 逐参数一致；Semaphore 条目以请求的 {@code permitsTotal} 定型许可总量
     * （建条目预检已保证 &gt; 0）；Latch 家族 P3-05 接入前不可达。
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
     * 等待屏障（Phase 3 详设 §2.4 / P3-05）：校验会话与 key 后，
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
                    continue; // 条目竞态移除，重试（design.md D4 同机制）
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
     * 倒计数（Phase 3 详设 §2.4 / P3-05）：条目定位与会话校验同
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
     * 只读统计观察面（Phase 3 T2，spec"只读统计观察面"）：弱一致遍历
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
            boolean held;
            synchronized (e) {
                waiters = e.waiterCount();
                held = e.leaseToken() != 0;
            }
            if (held) {
                switch (e.family()) {
                    case LOCK -> heldLocks++;
                    case SEMAPHORE -> heldSemaphores++;
                    default -> {
                        // LATCH 无 held 语义（leaseToken 恒 0，此分支不可达，防御占位）
                    }
                }
            }
            totalWaiters += waiters;
            if (waiters > maxQueueDepth) {
                maxQueueDepth = waiters;
            }
        }
        return new CoreStats(heldLocks, heldSemaphores, totalWaiters, maxQueueDepth, sessions.size());
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
     * @param notify 待通知的队首等待者列表
     * @param key    锁键，随事件一并报告
     */
    private void fireNotify(List<Waiter> notify, String key) {
        for (Waiter w : notify) {
            listener.notifyHead(w.sessionId(), w.requestId(), key);
        }
    }
}
