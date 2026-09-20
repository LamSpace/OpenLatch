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

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.core.CoreEngine;
import io.github.lamspace.openlatch.core.KeyFamily;
import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.AtomicOp;
import io.github.lamspace.openlatch.core.command.BarrierActionDoneCommand;
import io.github.lamspace.openlatch.core.command.BarrierAwaitCommand;
import io.github.lamspace.openlatch.core.command.BarrierLeaveCommand;
import io.github.lamspace.openlatch.core.result.BarrierActionDoneResult;
import io.github.lamspace.openlatch.core.result.BarrierAwaitResult;
import io.github.lamspace.openlatch.core.result.BarrierFinal;
import io.github.lamspace.openlatch.core.result.BarrierLeaveResult;
import io.github.lamspace.openlatch.core.command.AcquireCommand;
import io.github.lamspace.openlatch.core.command.AtomicOpCommand;
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.command.RenewCommand;
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.AtomicOpResult;
import io.github.lamspace.openlatch.core.result.ReleaseResult;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import io.github.lamspace.openlatch.core.result.RenewResult;
import io.github.lamspace.openlatch.core.snapshot.CoreStateRestore;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.AtomicOpPayload;
import io.github.lamspace.openlatch.protocol.raft.ApplyStatus;
import io.github.lamspace.openlatch.protocol.raft.RaftEntryType;
import io.github.lamspace.openlatch.protocol.raft.RaftLogEntry;
import io.github.lamspace.openlatch.protocol.raft.AcquirePayload;
import io.github.lamspace.openlatch.protocol.raft.ExpirePayload;
import io.github.lamspace.openlatch.protocol.raft.ReleasePayload;
import io.github.lamspace.openlatch.protocol.raft.RenewPayload;
import io.github.lamspace.openlatch.protocol.raft.BarrierActionDonePayload;
import io.github.lamspace.openlatch.protocol.raft.BarrierAwaitPayload;
import io.github.lamspace.openlatch.protocol.raft.BarrierLeavePayload;
import io.github.lamspace.openlatch.protocol.raft.LatchCountDownPayload;
import io.github.lamspace.openlatch.protocol.raft.SessionPayload;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.protocol.raft.SnapshotBarrierArrival;
import io.github.lamspace.openlatch.protocol.raft.SnapshotHolder;
import io.github.lamspace.openlatch.protocol.raft.SnapshotLock;
import io.github.lamspace.openlatch.protocol.raft.SnapshotState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 复制状态机内核：Raft 日志条目 → {@link CoreEngine} 的应用与
 * {@link ShadowTable} 双写核算的唯一入口。
 *
 * <p><b>职责与边界</b>：本类只做"条目 → 状态迁移"，不接触网络、不感知角色；
 * 回执（{@link ApplyResult}）是非复制的应答辅助信息，Leader 用它完成客户端
 * 应答（{@code ReplicationGateway}），Follower 应用同一条目产生同样的
 * {@link CoreEngine} 迁移但回执无人消费。锁语义完全下沉 {@link CoreEngine}，
 * 本类是它的集群调用方——集群路径对引擎的每一次可变调用都发生在
 * {@link #apply} 内（"引擎状态变更唯一漏斗"不变式）。
 *
 * <p><b>时间语义</b>：应用期间经 {@link EntryClock} 注入条目携带时刻，
 * 授予/续租的到期 = 条目时刻 + 租期，到期条目回放以条目时刻求到期集——
 * 物理时钟不进入任何状态迁移，同一序列在任何副本任何时刻重放结果一致。
 *
 * <p><b>会话映射</b>：逻辑会话 id（{@code (nodeId<<32)|localSeq}）在
 * {@code sidMap} 登记后映射到本副本引擎的内部 sid；引擎随机 sid 不出本节点，
 * 跨副本对齐只经影子表（digest 以逻辑 id 表达）。
 *
 * <p><b>幂等性</b>：条目按日志序各应用一次（Ratis 提交后串行应用），但应用
 * 语义仍按幂等设计——SESSION_OPEN 重复登记为无操作，SESSION_CLOSE 对未登记
 * 会话无操作，到期条目由引擎的"凭证+到期时刻"陈旧校验兜底（ABA 安全）。
 *
 * <p><b>快照通道</b>：{@link #snapshotState()} 在
 * applyLock 内产出一致性状态（影子表 proto + 引擎发号水位）；
 * {@link #installSnapshot} 以<b>全新引擎</b>经 {@code CoreEngine.restoreFrom}
 * 整体替换状态并重建 {@code sidMap}——回灌重放路线在"快照含历史释放空洞"
 * 下无法复现凭证序列，发号水位使重建副本与未截断副本对同一
 * 尾部日志发出逐笔相同的凭证（digest 跨快照切割点可比）。引擎替换只发生在
 * applyLock 内、应用/安装线程域，业务投影（{@link ShadowTable#heldEntries()}）
 * 经影子表 {@code load} 的原子替换获得一致视图。
 *
 * <p><b>线程模型</b>：{@link #apply} 仅由状态机应用线程（单线程、条目间无并发，
 * Ratis {@code StateMachineUpdater}）调用；{@code applyLock} 兜底
 * 串行并保护影子表一致性快照。构造后 {@link #shadow()}/{@link #digest()} 的
 * 无锁投影读取（{@link ShadowTable#heldEntries()} 等）允许发生在 Leader 业务线程。
 */
public final class LockStateMachineCore {

    /** 日志器：条目不可解析/未知类型等"不应发生"的应用异常。 */
    private static final Logger log = LoggerFactory.getLogger(LockStateMachineCore.class);

    /** apply 串行化兜底锁（兼保护 engine/shadow 原子推进）。 */
    private final Object applyLock = new Object();
    /** core 限额与租约配置（不可变）。 */
    private final CoreConfig config;
    /** 条目时刻时间源，注入引擎。 */
    private final EntryClock clock = new EntryClock();
    /** 复制状态影子表（逻辑归属视图 + 无锁预检索引）。 */
    private final ShadowTable shadow = new ShadowTable();
    /** 逻辑会话 id → 本副本引擎内部 sid（引擎随机 sid 不出节点；安装快照时整体重建）。 */
    private final Map<Long, Long> sidMap = new HashMap<>();
    /**
     * 锁语义核心。集群路径的可变入口为 {@link #apply}（条目迁移）与
     * {@link #installSnapshot}（快照整体替换，applyLock 内换入全新引擎）——
     * 二者之外不得变更引擎状态。
     */
    private CoreEngine engine;

    /** 应用失败计数（解析异常/未知类型），诊断与测试断言用。 */
    private volatile long applyFailures;

    /**
     * 构造内核并装配引擎。
     *
     * @param config core 限额与租约配置；null 抛 {@link NullPointerException}
     */
    public LockStateMachineCore(CoreConfig config) {
        this.config = java.util.Objects.requireNonNull(config);
        this.engine = newEngine();
    }

    /**
     * 装配一个零状态引擎：集群引擎对锁/Semaphore/Latch 恒不登记等待项
     * （其 {@code notifyHead} 事件源只存在于单机路径，收到即违例记 WARN）；
     * BARRIER 家族是唯一豁免——其到场是进日志的复制态命令，各副本引擎
     * 确定性建队并以队列作账簿投影，放行/破障的推送职责在 Leader 侧
     * 网关，该家族在此收到 notify 属按设计吞声。{@link #installSnapshot}
     * 换入新引擎时复用。
     *
     * @return 全新 {@link CoreEngine}（config/clock 同源）
     */
    private CoreEngine newEngine() {
        return new CoreEngine(config, clock, (sid, rid, key) -> {
            // 循环屏障例外：其到场经日志重放在各副本引擎内确定性建队，
            // 合拢/破障广播的推送职责在 Leader 侧网关（WaitQueue 簿记），
            // 引擎监听点对该家族的 notify 事件按设计吞声；其余家族的
            // 等待项登记仍属"集群引擎恒不登记等待项"不变式的违例。
            if (familyOfKey(key) == KeyFamily.BARRIER) {
                return;
            }
            log.warn("unexpected notifyHead from cluster engine: sid={}, key={}", sid, key);
        });
    }

    /**
     * key 当前条目的家族（监听器吞声判定用；引擎装配未完成或条目不存在
     * 回 {@code null}）。只读，零扰动。
     *
     * @param key 锁键
     * @return 条目家族；不存在为 {@code null}
     */
    private KeyFamily familyOfKey(String key) {
        CoreEngine e = engine;
        if (e == null || key == null) {
            return null;
        }
        var snap = e.inspectKey(key);
        return snap == null ? null : snap.family();
    }

    /**
     * 自引擎导出指定 key 的 BARRIER 复制态并折算为影子表镜像输入
     * （(内部 sid→逻辑 id) 经 {@code sidMap} 反向映射；引擎内部 sid
     * 不出本节点，digest 以逻辑 id 表达）。条目不存在或非屏障回
     * {@code null}（调用方零扰动）。
     *
     * @param key 屏障键
     * @return 镜像输入；不可得为 {@code null}
     */
    private ShadowTable.BarrierMirrorData barrierMirrorOf(String key) {
        return barrierMirrorOf(key, null);
    }

    /**
     * {@link #barrierMirrorOf(String)} 的映射覆写形态：SESSION_CLOSE 应用点
     * 必须传入摘除 {@code sidMap} 条目<strong>之前</strong>拍下的
     * 内部 sid→逻辑 id 快照——否则死亡会话在账簿中的内部 sid 无从折算，
     * 随机引擎 sid 会泄入镜像并撕裂 digest（跨副本回放不一致）。
     *
     * @param key         屏障键
     * @param toLogicalBy 预置折算表；{@code null} 表示现场自 {@code sidMap} 构建
     * @return 镜像输入；不可得为 {@code null}
     */
    private ShadowTable.BarrierMirrorData barrierMirrorOf(String key,
            Map<Long, Long> toLogicalBy) {
        var st = engine.barrierReplicatedState(key);
        if (st == null) {
            return null;
        }
        java.util.Map<Long, Long> toLogical = toLogicalBy != null
                ? toLogicalBy : new HashMap<>();
        if (toLogicalBy == null) {
            for (var en : sidMap.entrySet()) {
                toLogical.put(en.getValue(), en.getKey());
            }
        }
        java.util.function.Function<io.github.lamspace.openlatch.core.lock.BarrierEntry.Arrival,
                ShadowTable.ArrivalRef> ref = a -> new ShadowTable.ArrivalRef(
                        toLogical.getOrDefault(a.sessionId(), a.sessionId()), a.requestId());
        long actionLogical = st.actionSession() == 0 ? 0
                : toLogical.getOrDefault(st.actionSession(), st.actionSession());
        long execLogical = st.completedExecutor() == 0 ? 0
                : toLogical.getOrDefault(st.completedExecutor(), st.completedExecutor());
        int resultCode = st.completedResult() == BarrierFinal.TRIPPED ? 1
                : st.completedResult() == BarrierFinal.BROKEN ? 2 : 0;
        return new ShadowTable.BarrierMirrorData(st.parties(), st.generation(),
                st.currentArrivals().stream().map(ref).toList(), actionLogical, st.actionRequest(),
                st.completedGeneration(), resultCode,
                st.completedArrivals().stream().map(ref).toList(), execLogical);
    }

    /**
     * 镜像指定 key 的屏障复制态（各 BARRIER 应用点与破障传播后的统一
     * 刷新落点；导出不可得时零扰动）。
     *
     * @param key 屏障键
     */
    private void mirrorBarrier(String key) {
        mirrorBarrier(key, null);
    }

    /**
     * 镜像指定 key 的屏障复制态（映射表覆写形态，SESSION_CLOSE 传播专用）。
     *
     * @param key         屏障键
     * @param toLogicalBy 预置内部→逻辑折算表，可为 {@code null}
     */
    private void mirrorBarrier(String key, Map<Long, Long> toLogicalBy) {
        ShadowTable.BarrierMirrorData d = barrierMirrorOf(key, toLogicalBy);
        if (d != null) {
            shadow.barrierMirror(key, d);
        }
    }

    /**
     * 应用一条已解析的复制条目（由状态机应用线程调用）。
     *
     * <p>判定顺序：条目类型分发（未知类型 → {@link ApplyStatus#INTERNAL_ERROR}）；
     * 载荷解析失败同判 {@code INTERNAL_ERROR}（MUST NOT 抛出——抛出会经 Ratis
     * 关闭整个复制服务，坏条目应显式失败并被观测）。会话未登记时写请求拒入
     * （{@link ApplyStatus#REJECT_SESSION}）且不产生任何状态迁移。
     *
     * @param entry 复制条目；null 抛 {@link NullPointerException}
     * @return 应用回执（非复制状态；Leader 侧用于应答客户端）
     */
    public ApplyResult apply(RaftLogEntry entry) {
        synchronized (applyLock) {
            EntryClock.setApplyNow(0);
            try {
                long t = entry.getWallClockMs();
                EntryClock.setApplyNow(t);
                return switch (entry.getType()) {
                    case SESSION_OPEN -> applySessionOpen(entry);
                    case SESSION_CLOSE -> applySessionClose(entry);
                    case LOCK_ACQUIRE_ENTRY -> applyAcquire(entry);
                    case LOCK_RELEASE_ENTRY -> applyRelease(entry);
                    case LEASE_RENEW_ENTRY -> applyRenew(entry);
                    case LEASE_EXPIRE_ENTRY -> applyExpire(entry, t);
                    case LATCH_COUNT_DOWN_ENTRY -> applyLatchCountDown(entry);
                    case ATOMIC_OP_ENTRY -> applyAtomicOp(entry);
                    case BARRIER_AWAIT_ENTRY -> applyBarrierAwait(entry);
                    case BARRIER_LEAVE_ENTRY -> applyBarrierLeave(entry);
                    case BARRIER_ACTION_DONE_ENTRY -> applyBarrierActionDone(entry);
                    case NOOP -> ok(0).build();
                    default -> error("unknown entry type " + entry.getType(), entry);
                };
            } catch (InvalidProtocolBufferException | RuntimeException e) {
                return error("apply failed: type=" + entry.getType(), entry, e);
            } finally {
                EntryClock.clearApplyNow();
            }
        }
    }

    /**
     * 序列化形态的应用入口（测试与追赶工具共用）：解析失败以
     * {@link ApplyStatus#INTERNAL_ERROR} 回执。
     *
     * @param entryBytes {@link RaftLogEntry} 序列化字节
     * @return {@link ApplyResult} 序列化字节
     */
    public byte[] applyEntry(byte[] entryBytes) {
        try {
            return apply(RaftLogEntry.parseFrom(entryBytes)).toByteArray();
        } catch (InvalidProtocolBufferException e) {
            applyFailures++;
            log.error("undecodable raft entry", e);
            return ApplyResult.newBuilder().setStatus(ApplyStatus.INTERNAL_ERROR).build().toByteArray();
        }
    }

    /**
     * SESSION_OPEN：登记逻辑会话并在本副本引擎注册内部 sid。重复登记
     * （快照回灌后追赶重叠等）判存跳过，幂等。
     *
     * @param entry 条目（载荷为 {@link SessionPayload}）
     * @return OK 回执
     * @throws InvalidProtocolBufferException 载荷不可解析（调用方转 INTERNAL_ERROR）
     */
    private ApplyResult applySessionOpen(RaftLogEntry entry) throws InvalidProtocolBufferException {
        SessionPayload p = SessionPayload.parseFrom(entry.getCommandPayload());
        long sid = p.getSessionId();
        if (!shadow.hasSession(sid)) {
            sidMap.put(sid, engine.sessionOpened());
            shadow.addSession(sid);
        }
        return ok(0).build();
    }

    /**
     * SESSION_CLOSE：摘登记、关引擎会话（释放其全部持锁）、镜像影子表。
     * 未登记会话为无操作（幂等）。
     *
     * @param entry 条目（载荷为 {@link SessionPayload}）
     * @return OK 回执
     * @throws InvalidProtocolBufferException 载荷不可解析（调用方转 INTERNAL_ERROR）
     */
    private ApplyResult applySessionClose(RaftLogEntry entry) throws InvalidProtocolBufferException {
        SessionPayload p = SessionPayload.parseFrom(entry.getCommandPayload());
        long sid = p.getSessionId();
        java.util.List<String> freed;
        List<String> brokenBarriers = List.of();
        Map<Long, Long> toLogical = new HashMap<>();
        if (shadow.hasSession(sid)) {
            // 折算表快照 MUST 先于 sidMap 摘除——死亡会话的内部 sid 仍需按
            // 逻辑 id 入镜像（账簿残留到世代滚出为止，digest 不得携带随机 sid）。
            for (var en : sidMap.entrySet()) {
                toLogical.put(en.getValue(), en.getKey());
            }
            shadow.removeSession(sid);
            Long local = sidMap.remove(sid);
            if (local != null) {
                brokenBarriers = engine.sessionClosed(local);
            }
            freed = shadow.dropSessionHolders(sid);
        } else {
            freed = List.of();
        }
        ApplyResult.Builder b = ok(0).addAllFreedKeys(freed);
        // 离场即破障经 SESSION_CLOSE 传播：破障世代已入引擎了结记录，
        // 镜像刷新 + 回执携带被破 key 供 Leader 向存活等待者广播。
        for (String bk : brokenBarriers) {
            mirrorBarrier(bk, toLogical);
            b.addBarrierReleasedKeys(bk);
        }
        return b.build();
    }

    /**
     * LOCK_ACQUIRE_ENTRY：以 {@code queueIfBusy=false} 调引擎（集群等待队列不
     * 进引擎），授予时镜像影子表；需排队时回
     * {@link ApplyStatus#DENIED}（排队裁决由 Leader 侧在应用回调中完成）。
     *
     * @param entry 条目（载荷为 {@link AcquirePayload}）
     * @return 回执：OK（携带凭证/租期/到期）或 DENIED/REJECT_SESSION/INTERNAL_ERROR
     * @throws InvalidProtocolBufferException 载荷不可解析（调用方转 INTERNAL_ERROR）
     */
    private ApplyResult applyAcquire(RaftLogEntry entry) throws InvalidProtocolBufferException {
        AcquirePayload p = AcquirePayload.parseFrom(entry.getCommandPayload());
        Long local = sidMap.get(p.getSessionId());
        if (local == null) {
            return ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build();
        }
        var req = p.getRequest();
        LockType lockType = toCoreLockType(req.getLockType().getNumber());
        if (lockType == null) {
            return ApplyResult.newBuilder().setStatus(ApplyStatus.INTERNAL_ERROR).build();
        }
        AcquireResult r = engine.acquire(new AcquireCommand(
                local, p.getRequestId(), req.getKey(), lockType,
                req.getThreadId(), req.getLeaseMs(), false,
                RequestDispatcher.normalizedPermits(req.getPermits()), req.getPermitsTotal()));
        return switch (r.outcome()) {
            case GRANTED -> {
                long expiresAt = entry.getWallClockMs() + r.grantedLeaseMs();
                // Semaphore 授予按许可数镜像持有增量；锁家族恒 1。
                int holderDelta = lockType == LockType.SEMAPHORE
                        ? RequestDispatcher.normalizedPermits(req.getPermits()) : 1;
                shadow.grantDelta(p.getSessionId(), req.getThreadId(), req.getKey(),
                        req.getLockType().getNumber(), r.leaseToken(), r.grantedLeaseMs(), expiresAt,
                        holderDelta, req.getPermitsTotal());
                yield ApplyResult.newBuilder()
                        .setStatus(ApplyStatus.OK)
                        .setLeaseToken(r.leaseToken())
                        .setGrantedLeaseMs(r.grantedLeaseMs())
                        .setLeaseExpiresAtMs(expiresAt)
                        .build();
            }
            case DENIED -> ApplyResult.newBuilder().setStatus(ApplyStatus.DENIED).build();
            case REJECT_SESSION -> ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build();
            // v3 形状拒绝（家族误用/总量断言）：回执 INVALID_REQUEST。
            case REJECT_TYPE_MISMATCH, REJECT_SEMAPHORE_TOTAL, REJECT_LATCH_TOTAL ->
                    ApplyResult.newBuilder().setStatus(ApplyStatus.INVALID_REQUEST).build();
            // 引擎集群路径 queueIfBusy=false 且恒无等待项：QUEUED/QUEUE_FULL 不可达，
            // key 非法已由接入预检拒绝——抵达此处即说明上游校验被绕过，显式失败。
            default -> error("unreachable acquire outcome " + r.outcome(), entry);
        };
    }

    /**
     * LOCK_RELEASE_ENTRY：引擎释放（凭证/归属判定在引擎内），OK 时镜像影子表
     * 计数回退（可重入逐层释放，与 {@code fullyReleased} 判定等价收敛）。
     *
     * @param entry 条目（载荷为 {@link ReleasePayload}）
     * @return 回执：状态映射与 {@code fullyReleased} 标志
     * @throws InvalidProtocolBufferException 载荷不可解析（调用方转 INTERNAL_ERROR）
     */
    private ApplyResult applyRelease(RaftLogEntry entry) throws InvalidProtocolBufferException {
        ReleasePayload p = ReleasePayload.parseFrom(entry.getCommandPayload());
        Long local = sidMap.get(p.getSessionId());
        if (local == null) {
            return ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build();
        }
        var req = p.getRequest();
        ReleaseResult r = engine.release(new ReleaseCommand(
                local, req.getKey(), req.getLeaseToken(), req.getThreadId(),
                RequestDispatcher.normalizedPermits(req.getPermits())));
        if (r.status() == ReleaseStatus.OK) {
            // 引擎归还多少镜像回退多少：锁恒 1、Semaphore 为归还许可数。
            shadow.release(p.getSessionId(), req.getThreadId(), req.getKey(), r.releasedCount());
        }
        ApplyStatus st = switch (r.status()) {
            case OK -> ApplyStatus.OK;
            case NOT_HELD -> ApplyStatus.NOT_HELD;
            case INVALID_TOKEN -> ApplyStatus.INVALID_TOKEN;
            case REJECT_SESSION -> ApplyStatus.REJECT_SESSION;
            // 超额归还：参数与持有不符，回执 INVALID_REQUEST。
            case OVER_RELEASE -> ApplyStatus.INVALID_REQUEST;
        };
        ApplyResult.Builder b = ApplyResult.newBuilder().setStatus(st).setFullyReleased(r.fullyReleased());
        if (r.status() == ReleaseStatus.OK && r.fullyReleased()) {
            b.addFreedKeys(req.getKey());
        }
        return b.build();
    }

    /**
     * LEASE_RENEW_ENTRY：引擎续租（凭证判定在引擎内），OK 时镜像影子表刷新
     * 到期时刻（实际租期 = 新到期 − 条目时刻）。
     *
     * @param entry 条目（载荷为 {@link RenewPayload}）
     * @return 回执：状态映射与新到期时刻（OK 时）
     * @throws InvalidProtocolBufferException 载荷不可解析（调用方转 INTERNAL_ERROR）
     */
    private ApplyResult applyRenew(RaftLogEntry entry) throws InvalidProtocolBufferException {
        RenewPayload p = RenewPayload.parseFrom(entry.getCommandPayload());
        Long local = sidMap.get(p.getSessionId());
        if (local == null) {
            return ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build();
        }
        var req = p.getRequest();
        RenewResult r = engine.renew(new RenewCommand(
                local, req.getKey(), req.getLeaseToken(), req.getLeaseMs()));
        if (r.status() == ReleaseStatus.OK) {
            shadow.renew(req.getKey(), r.newExpiresAtMs(), r.newExpiresAtMs() - entry.getWallClockMs());
        }
        ApplyStatus st = switch (r.status()) {
            case OK -> ApplyStatus.OK;
            case NOT_HELD -> ApplyStatus.NOT_HELD;
            case INVALID_TOKEN -> ApplyStatus.INVALID_TOKEN;
            case REJECT_SESSION -> ApplyStatus.REJECT_SESSION;
            // 续租通道永不产出此码（释放专属），编译器穷尽性占位。
            case OVER_RELEASE -> ApplyStatus.INTERNAL_ERROR;
        };
        return ApplyResult.newBuilder()
                .setStatus(st)
                .setLeaseExpiresAtMs(r.newExpiresAtMs())
                .build();
    }

    /**
     * LEASE_EXPIRE_ENTRY：token 守卫通过后（条目 token == 当前持有 且
     * 到期时刻 ≤ 条目时刻），以条目时刻驱动 {@code engine.expireDue()} 并镜像
     * 影子表清扫——释放集由"复制状态 + 条目时刻"唯一确定，跨副本判定恒等。
     * 守卫不通过（已易主/尚未到期/已不存在）时整条空操作。守卫匹配时 {@code expireDue()} 顺带收敛同一时刻到期的
     * 其他 key（它们各有条目，回放先后互为空操作，终态一致）。
     *
     * @param entry       条目（载荷为 {@link ExpirePayload}，key + 被扫到期凭证）
     * @param entryTimeMs 条目携带时刻（守卫与清扫的"现在"）
     * @return OK 回执（实际释放时 {@code freed_keys} 携带空出的 key，供队首唤醒）
     * @throws InvalidProtocolBufferException 载荷不可解析（调用方转 INTERNAL_ERROR）
     */
    private ApplyResult applyExpire(RaftLogEntry entry, long entryTimeMs) throws InvalidProtocolBufferException {
        ExpirePayload p = ExpirePayload.parseFrom(entry.getCommandPayload());
        ShadowTable.HeldRef ref = shadow.heldRef(p.getKey());
        // 回放守卫（过期条目不误杀新持有者）：条目 token 与当前持有
        // 不匹配、或该 key 在条目时刻尚未到期 → 整条空操作。守卫输入全部是
        // 复制状态 + 条目携带时刻，跨副本判定恒等；匹配时以条目时刻驱动
        // engine.expireDue()（其"凭证+到期时刻"双陈旧校验顺带收敛同刻到期的
        // 其他 key——它们各有自己的到期条目，收敛结果一致）。
        if (ref == null || ref.leaseToken() != p.getLeaseToken() || ref.expiresAtMs() > entryTimeMs) {
            return ok(0).build();
        }
        engine.expireDue();
        return ok(0).addAllFreedKeys(shadow.expireUpTo(entryTimeMs)).build();
    }

    /**
     * 协议锁类型数值 → core 枚举（两侧枚举序对齐：0 REENTRANT / 1 SIMPLE /
     * 2 READ / 3 WRITE / 4 FAIR / 5 SEMAPHORE / 6 LATCH / 7–9 原子形态）；
     * 越界回 {@code null}。新类型在进入本路径前已被接入层版本门控拦截
     * （握手中继/提案预检），此处仅保持映射完备。
     *
     * @param number 协议 {@code LockType} 数值
     * @return core 锁类型，越界为 {@code null}
     */
    private static LockType toCoreLockType(int number) {
        LockType[] vs = LockType.values();
        return number >= 0 && number < vs.length ? vs[number] : null;
    }

    /**
     * 构造 OK 回执（可选携带到期时刻）。
     *
     * @param expiresAtMs 到期时刻，无则传 {@code 0}
     * @return 回执构造器
     */
    private static ApplyResult.Builder ok(long expiresAtMs) {
        return ApplyResult.newBuilder().setStatus(ApplyStatus.OK).setLeaseExpiresAtMs(expiresAtMs);
    }

    /**
     * 记录应用异常并返回 INTERNAL_ERROR 回执（计数供观测，抛出被禁止——见 {@link #apply}）。
     *
     * @param msg   错误说明
     * @param entry 出错条目（记日志用）
     * @return INTERNAL_ERROR 回执
     */
    private ApplyResult error(String msg, RaftLogEntry entry) {
        return error(msg, entry, null);
    }

    /**
     * LATCH_COUNT_DOWN_ENTRY：引擎倒计数（创建/断言/扣减全在引擎内，家族误用与
     * 总量断言拒回 {@link ApplyStatus#INVALID_REQUEST}），OK 时镜像影子表
     * （条目创建与剩余计数回写、参与会话登记）并随回执携带 {@code remaining}
     * 供 Leader 侧归零广播判定。
     *
     * @param entry 条目（载荷为 {@link LatchCountDownPayload}）
     * @return 回执
     * @throws InvalidProtocolBufferException 载荷不可解析（调用方转 INTERNAL_ERROR）
     */
    private ApplyResult applyLatchCountDown(RaftLogEntry entry) throws InvalidProtocolBufferException {
        LatchCountDownPayload p = LatchCountDownPayload.parseFrom(entry.getCommandPayload());
        Long local = sidMap.get(p.getSessionId());
        var req = p.getRequest();
        if (local == null) {
            return ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build();
        }
        io.github.lamspace.openlatch.core.result.LatchCountDownResult r = engine.countDown(
                new io.github.lamspace.openlatch.core.command.LatchCountDownCommand(
                        local, req.getKey(), req.getCount(), req.getTotal()));
        return switch (r.outcome()) {
            case GRANTED -> {
                shadow.latchApplied(req.getKey(), p.getSessionId(),
                        req.getTotal() != 0 ? req.getTotal() : shadow.latchTotalOf(req.getKey()),
                        r.remaining());
                yield ApplyResult.newBuilder()
                        .setStatus(ApplyStatus.OK)
                        .setLatchRemaining(r.remaining())
                        .build();
            }
            case REJECT_SESSION -> ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build();
            case REJECT_LATCH_TOTAL, REJECT_TYPE_MISMATCH, REJECT_KEY_EMPTY, REJECT_KEY_TOO_LONG ->
                    ApplyResult.newBuilder().setStatus(ApplyStatus.INVALID_REQUEST).build();
            default -> error("unreachable latch countDown outcome " + r.outcome(), entry);
        };
    }

    /**
     * ATOMIC_OP_ENTRY：引擎原子操作（形态互拒/值域/断言/去重全在引擎内，
     * 应答四元组由应用结果导出——同槽重放在任何副本上得出逐位一致回执），
     * OK 时镜像影子表（懒建与槽更新、GET 零迁移不建不刷）。家族误用、
     * 初值断言与值域越界拒回 {@link ApplyStatus#INVALID_REQUEST}；
     * CAS 家族"未落值"是 {@code OK + atomic_applied=false} 的有效读数。
     *
     * @param entry 条目（载荷为 {@link AtomicOpPayload}）
     * @return 回执（OK 携带应答四元组）
     * @throws InvalidProtocolBufferException 载荷不可解析（调用方转 INTERNAL_ERROR）
     */
    private ApplyResult applyAtomicOp(RaftLogEntry entry) throws InvalidProtocolBufferException {
        AtomicOpPayload p = AtomicOpPayload.parseFrom(entry.getCommandPayload());
        Long local = sidMap.get(p.getSessionId());
        if (local == null) {
            return ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build();
        }
        var req = p.getRequest();
        io.github.lamspace.openlatch.core.LockType kind =
                toCoreAtomicKind(req.getLockType().getNumber());
        AtomicOp op = toCoreAtomicOp(req.getOp());
        if (kind == null || op == null) {
            // 非原子形态或未知 op：形状非法（ACQUIRE 面类型不得进入本通道）。
            return ApplyResult.newBuilder().setStatus(ApplyStatus.INVALID_REQUEST).build();
        }
        AtomicOpResult r = engine.atomicOp(new AtomicOpCommand(local, p.getRequestId(),
                req.getKey(), kind, op, req.getOperand(), req.getExpected(),
                req.getExpectedVersion(), req.getInitialValue(), req.getOpSeq()));
        return switch (r.outcome()) {
            case GRANTED -> {
                shadow.atomicApplied(req.getKey(), req.getLockType().getNumber(),
                        p.getSessionId(), req.getOpSeq(), req.getInitialValue(),
                        op == AtomicOp.GET, r.applied(), r.oldValue(), r.value(), r.version());
                yield ApplyResult.newBuilder()
                        .setStatus(ApplyStatus.OK)
                        .setAtomicApplied(r.applied())
                        .setAtomicOldValue(r.oldValue())
                        .setAtomicValue(r.value())
                        .setAtomicVersion(r.version())
                        .build();
            }
            case REJECT_SESSION -> ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build();
            case REJECT_TYPE_MISMATCH, REJECT_ATOMIC_INIT, REJECT_ATOMIC_RANGE,
                    REJECT_KEY_EMPTY, REJECT_KEY_TOO_LONG ->
                    ApplyResult.newBuilder().setStatus(ApplyStatus.INVALID_REQUEST).build();
            default -> error("unreachable atomic op outcome " + r.outcome(), entry);
        };
    }

    /**
     * BARRIER_AWAIT_ENTRY：引擎到场（parties 断言、世代了结记录幂等、
     * 合拢与执行者指定全在引擎内——重放与同槽去重在条目内复制状态上
     * 得出，跨副本一致），随后镜像影子表。回执携带世代/位次/执行者/
     * settled 广播标志；{@code REJECT_QUEUE_FULL} 映射
     * {@link ApplyStatus#QUEUE_FULL}（Leader 侧据此不登记等待队列项），
     * parties 断言与家族误用拒回 {@link ApplyStatus#INVALID_REQUEST}。
     *
     * @param entry 条目（载荷为 {@link BarrierAwaitPayload}）
     * @return 回执
     * @throws InvalidProtocolBufferException 载荷不可解析（调用方转 INTERNAL_ERROR）
     */
    private ApplyResult applyBarrierAwait(RaftLogEntry entry) throws InvalidProtocolBufferException {
        BarrierAwaitPayload p = BarrierAwaitPayload.parseFrom(entry.getCommandPayload());
        Long local = sidMap.get(p.getSessionId());
        if (local == null) {
            return ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build();
        }
        var req = p.getRequest();
        BarrierAwaitResult r = engine.barrierAwait(new BarrierAwaitCommand(
                local, p.getRequestId(), req.getKey(), req.getParties(), req.getCarriesAction()));
        return switch (r.outcome()) {
            case GRANTED, QUEUED, BARRIER_BROKEN, REJECT_QUEUE_FULL -> {
                if (r.outcome() != io.github.lamspace.openlatch.core.result.Outcome.REJECT_QUEUE_FULL) {
                    mirrorBarrier(req.getKey());
                }
                ApplyStatus st = switch (r.outcome()) {
                    case GRANTED -> ApplyStatus.OK;
                    case QUEUED -> ApplyStatus.QUEUED;
                    case BARRIER_BROKEN -> ApplyStatus.BARRIER_BROKEN;
                    default -> ApplyStatus.QUEUE_FULL;
                };
                yield ApplyResult.newBuilder()
                        .setStatus(st)
                        .setQueuePosition(r.queuePosition())
                        .setBarrierGeneration(r.generation())
                        .setBarrierExecutor(r.executor())
                        .setBarrierParties(r.parties())
                        .setBarrierSettled(r.settled())
                        .build();
            }
            case REJECT_SESSION -> ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build();
            case REJECT_BARRIER_PARTIES, REJECT_TYPE_MISMATCH, REJECT_KEY_EMPTY,
                    REJECT_KEY_TOO_LONG ->
                    ApplyResult.newBuilder().setStatus(ApplyStatus.INVALID_REQUEST).build();
            default -> error("unreachable barrier await outcome " + r.outcome(), entry);
        };
    }

    /**
     * BARRIER_LEAVE_ENTRY：引擎离场（超时/中断/显式破障的统一通道，
     * 离场即破障的裁决在引擎内），镜像刷新后回执。
     * {@code generationBroke} 经 {@code barrier_settled} 位透传供 Leader
     * 广播；幂等无操作亦回 OK（在带语义，非错误）。
     *
     * @param entry 条目（载荷为 {@link BarrierLeavePayload}）
     * @return 回执
     * @throws InvalidProtocolBufferException 载荷不可解析（调用方转 INTERNAL_ERROR）
     */
    private ApplyResult applyBarrierLeave(RaftLogEntry entry) throws InvalidProtocolBufferException {
        BarrierLeavePayload p = BarrierLeavePayload.parseFrom(entry.getCommandPayload());
        Long local = sidMap.get(p.getSessionId());
        if (local == null) {
            return ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build();
        }
        var req = p.getRequest();
        BarrierLeaveResult r = engine.barrierLeave(new BarrierLeaveCommand(
                local, req.getKey(), req.getAwaitRequestId()));
        return switch (r.outcome()) {
            case GRANTED -> {
                mirrorBarrier(req.getKey());
                yield ApplyResult.newBuilder()
                        .setStatus(ApplyStatus.OK)
                        .setBarrierSettled(r.generationBroke())
                        .build();
            }
            case REJECT_SESSION -> ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build();
            case REJECT_TYPE_MISMATCH, REJECT_KEY_EMPTY, REJECT_KEY_TOO_LONG ->
                    ApplyResult.newBuilder().setStatus(ApplyStatus.INVALID_REQUEST).build();
            default -> error("unreachable barrier leave outcome " + r.outcome(), entry);
        };
    }

    /**
     * BARRIER_ACTION_DONE_ENTRY：引擎动作了结（执行者指定校验、世代定型
     * 合拢与幂等在引擎内），镜像刷新后回执。非指定执行者或未知世代
     * 拒回 {@link ApplyStatus#INVALID_REQUEST}；执行者动作期间世代已破
     * 回在带裁决 {@link ApplyStatus#BARRIER_BROKEN}。
     *
     * @param entry 条目（载荷为 {@link BarrierActionDonePayload}）
     * @return 回执
     * @throws InvalidProtocolBufferException 载荷不可解析（调用方转 INTERNAL_ERROR）
     */
    private ApplyResult applyBarrierActionDone(RaftLogEntry entry) throws InvalidProtocolBufferException {
        BarrierActionDonePayload p = BarrierActionDonePayload.parseFrom(entry.getCommandPayload());
        Long local = sidMap.get(p.getSessionId());
        if (local == null) {
            return ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build();
        }
        var req = p.getRequest();
        BarrierActionDoneResult r = engine.barrierActionDone(new BarrierActionDoneCommand(
                local, req.getKey(), req.getGeneration()));
        return switch (r.outcome()) {
            case GRANTED, BARRIER_BROKEN -> {
                mirrorBarrier(req.getKey());
                yield ApplyResult.newBuilder()
                        .setStatus(r.outcome() == io.github.lamspace.openlatch.core.result.Outcome.GRANTED
                                ? ApplyStatus.OK : ApplyStatus.BARRIER_BROKEN)
                        .setBarrierGeneration(r.generation())
                        .setBarrierSettled(r.settled())
                        .build();
            }
            case REJECT_SESSION -> ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build();
            case REJECT_BARRIER_ACTION, REJECT_TYPE_MISMATCH, REJECT_KEY_EMPTY,
                    REJECT_KEY_TOO_LONG ->
                    ApplyResult.newBuilder().setStatus(ApplyStatus.INVALID_REQUEST).build();
            default -> error("unreachable barrier actionDone outcome " + r.outcome(), entry);
        };
    }

    /**
     * 协议形态数值 → core 原子形态；非三原子类型（0–6 与越界）回
     * {@code null}（调用方以形状非法拒绝）。两侧枚举序对齐。
     *
     * @param number 协议 {@code LockType} 数值
     * @return core 原子形态，非原子为 {@code null}
     */
    private static io.github.lamspace.openlatch.core.LockType toCoreAtomicKind(int number) {
        return switch (number) {
            case 7 -> io.github.lamspace.openlatch.core.LockType.ATOMIC_LONG;
            case 8 -> io.github.lamspace.openlatch.core.LockType.ATOMIC_INTEGER;
            case 9 -> io.github.lamspace.openlatch.core.LockType.ATOMIC_BOOLEAN;
            default -> null;
        };
    }

    /**
     * 协议 {@code AtomicOp} → core {@link AtomicOp}；未知数值回
     * {@code null}（调用方以形状非法拒绝）。
     *
     * @param wireOp 协议操作枚举
     * @return core 操作枚举，未知为 {@code null}
     */
    private static AtomicOp toCoreAtomicOp(io.github.lamspace.openlatch.protocol.AtomicOp wireOp) {
        return switch (wireOp) {
            case ATOMIC_GET -> AtomicOp.GET;
            case ATOMIC_SET -> AtomicOp.SET;
            case ATOMIC_GET_AND_SET -> AtomicOp.GET_AND_SET;
            case ATOMIC_ADD -> AtomicOp.ADD;
            case ATOMIC_CAS -> AtomicOp.CAS;
            case ATOMIC_CAS_STAMPED -> AtomicOp.CAS_STAMPED;
            default -> null;
        };
    }

    /**
     * {@link #error(String, RaftLogEntry)} 的带因变体。
     *
     * @param msg   错误说明
     * @param entry 出错条目（记日志用）
     * @param cause 触发异常，可为 {@code null}
     * @return INTERNAL_ERROR 回执
     */
    private ApplyResult error(String msg, RaftLogEntry entry, Throwable cause) {
        applyFailures++;
        log.error("{} (seq={}, type={})", msg, entry.getSeq(), entry.getType(), cause);
        return ApplyResult.newBuilder().setStatus(ApplyStatus.INTERNAL_ERROR).build();
    }

    /**
     * 复制状态全量摘要（跨副本一致性比对基准；内部经 {@code applyLock}
     * 与推进互斥，可安全在应用线程外调用）。
     *
     * @return SHA-256 hex 摘要
     */
    public String digest() {
        synchronized (applyLock) {
            return shadow.digest();
        }
    }

    /**
     * 影子表引用（Leader 预检查/到期扫描/等待队列联动消费）。
     *
     * @return 影子表
     */
    public ShadowTable shadow() {
        return shadow;
    }

    /**
     * 逻辑会话 id → 本副本引擎 sid 的映射快照（Leader 侧把引擎事件翻译回
     * 逻辑 id 用；复制语义不依赖此映射）。
     *
     * @param logicalSessionId 逻辑会话 id
     * @return 引擎 sid，未登记为 {@code null}
     */
    public Long engineSidOf(long logicalSessionId) {
        synchronized (applyLock) {
            return sidMap.get(logicalSessionId);
        }
    }

    /**
     * 应用失败计数（诊断；每次 {@link ApplyStatus#INTERNAL_ERROR} 递增）。
     *
     * @return 累计失败数
     */
    public long applyFailures() {
        return applyFailures;
    }

    /**
     * 产出当前复制状态的一致性快照形态：
     * applyLock 内取影子表 proto 并嵌入引擎发号水位（
     * {@code next_lease_token}——缺它则重建副本对同一尾部日志
     * 发出与未截断副本不同的凭证，跨副本 digest 永久分叉）。
     *
     * <p><b>并发语义</b>：与 {@link #apply} 互斥于同一 {@code applyLock}，
     * 产出即"某应用时刻的完整状态"（无撕裂）；返回后状态照常演化，演化
     * 由快照位点之后的日志承载（切割点不变性）。
     *
     * @return 不可变的 {@link SnapshotState}（锁条目按声明序 + 会话集 + 水位）
     */
    SnapshotState snapshotState() {
        synchronized (applyLock) {
            return shadow.toProto().toBuilder()
                    .setNextLeaseToken(engine.nextLeaseToken())
                    .build();
        }
    }

    /**
     * 安装一份快照并整体替换状态：启动加载（本地
     * 最新快照）与追赶安装（Leader 流式下发）共用本通道。
     *
     * <p><b>原子性</b>：applyLock 内完成"全新引擎重建（
     * {@code CoreEngine.restoreFrom}，含发号水位落位与到期堆回填）→
     * {@code sidMap} 按快照会话集重建（逻辑 id → 新内部 sid）→ 影子表
     * {@link ShadowTable#load} 整体替换"，三步之间对外不可见半更新状态。
     *
     * <p><b>前置契约</b>：仅由状态机应用/安装线程（Ratis 保证与
     * {@link #apply} 同线程域或 {@code pause} 隔离）调用；调用后本内核的
     * 已应用位点由调用方（{@link LockStateMachine}）同步设置。快照中持有者
     * 引用未登记会话、锁类型越界等损坏形态以异常抛出（MUST NOT 静默装坏
     * 状态——启动期异常即拒绝启动，安装期异常由 Ratis 重试）。
     *
     * @param state 快照状态（{@link #snapshotState()} 的对偶产物）
     * @throws IllegalStateException 快照自洽性损坏（持有者缺会话登记、类型越界）
     */
    void installSnapshot(SnapshotState state) {
        synchronized (applyLock) {
            CoreEngine fresh = newEngine();
            Map<Long, Long> newSidMap = new HashMap<>();
            for (long logical : state.getSessionsList()) {
                newSidMap.put(logical, fresh.sessionOpened());
            }
            List<CoreStateRestore.Entry> entries = new ArrayList<>(state.getLocksCount());
            for (SnapshotLock l : state.getLocksList()) {
                LockType type = toCoreLockType(l.getLockTypeValue());
                if (type == null) {
                    throw new IllegalStateException(
                            "snapshot entry has invalid lock type: key=" + l.getKey());
                }
                List<CoreStateRestore.Holder> holders = new ArrayList<>(l.getHoldersCount());
                for (SnapshotHolder h : l.getHoldersList()) {
                    Long internal = newSidMap.get(h.getSessionId());
                    if (internal == null) {
                        throw new IllegalStateException("snapshot holder session not registered: "
                                + h.getSessionId() + " (key=" + l.getKey() + ")");
                    }
                    holders.add(new CoreStateRestore.Holder(internal, h.getThreadId(), h.getCount()));
                }
                // v4：原子条目携状态组重建（值/版本戳/去重槽直写，断言不变量
                // 在 CoreStateRestore.Entry 构造内复核）；其余家族状态组恒 null。
                // v5：屏障状态组重建——账簿逻辑 id 折算内部 sid；了结记录中
                // 未登记（已消亡）会话的账簿项剔除（其重发在会话校验即拒，
                // 引擎侧永不触达，保留反而引入悬空身份）。
                CoreStateRestore.BarrierState barrier = null;
                if (l.getLockTypeValue() == io.github.lamspace.openlatch.protocol.LockType
                        .LOCK_TYPE_BARRIER_VALUE) {
                    java.util.function.Function<SnapshotBarrierArrival,
                            io.github.lamspace.openlatch.core.lock.BarrierEntry.Arrival> conv =
                            a -> new io.github.lamspace.openlatch.core.lock.BarrierEntry.Arrival(
                                    newSidMap.get(a.getSessionId()), a.getRequestId());
                    List<io.github.lamspace.openlatch.core.lock.BarrierEntry.Arrival> cur =
                            new ArrayList<>();
                    for (SnapshotBarrierArrival a : l.getBarrierCurrentArrivalsList()) {
                        if (newSidMap.containsKey(a.getSessionId())) {
                            cur.add(conv.apply(a));
                        }
                    }
                    List<io.github.lamspace.openlatch.core.lock.BarrierEntry.Arrival> comp =
                            new ArrayList<>();
                    for (SnapshotBarrierArrival a : l.getBarrierCompletedArrivalsList()) {
                        if (newSidMap.containsKey(a.getSessionId())) {
                            comp.add(conv.apply(a));
                        }
                    }
                    long completedExecutor = l.getBarrierCompletedExecutor() == 0 ? 0
                            : newSidMap.getOrDefault(l.getBarrierCompletedExecutor(), 0L);
                    barrier = new CoreStateRestore.BarrierState(l.getBarrierParties(),
                            l.getBarrierGeneration(), cur,
                            newSidMap.getOrDefault(l.getBarrierActionSession(), 0L),
                            l.getBarrierActionRequest(), l.getBarrierCompletedGeneration(),
                            l.getBarrierCompletedResult(), comp, completedExecutor);
                }
                CoreStateRestore.AtomicState atomic = null;
                if (ShadowTable.isAtomicType(l.getLockTypeValue())) {
                    atomic = new CoreStateRestore.AtomicState(l.getAtomicInitial(),
                            l.getAtomicValue(), l.getAtomicVersion(),
                            l.getAtomicSlotSession(), l.getAtomicSlotOpSeq(),
                            l.getAtomicSlotApplied(), l.getAtomicSlotOldValue(),
                            l.getAtomicSlotValue(), l.getAtomicSlotVersion());
                }
                entries.add(new CoreStateRestore.Entry(l.getKey(), type, l.getLeaseToken(),
                        l.getLeaseMs(), l.getExpiresAtMs(), holders,
                        l.getPermitsTotal(), l.getLatchTotal(), l.getLatchCount(),
                        atomic, barrier));
            }
            // 发号水位：老快照缺字段（值为 0）按"继承最大凭证 +1"兜底，自洽校验
            // 在 CoreStateRestore 构造内完成（水位不大于任何凭证即拒绝）。
            long watermark = state.getNextLeaseToken();
            if (watermark < 1) {
                watermark = maxEntryToken(entries) + 1;
            }
            fresh.restoreFrom(new CoreStateRestore(entries, List.copyOf(newSidMap.values()),
                    watermark));
            this.engine = fresh;
            this.sidMap.clear();
            this.sidMap.putAll(newSidMap);
            this.shadow.load(state);
        }
    }

    /**
     * 重建条目列表中的最大租约凭证（缺水位快照的兜底计算）。
     *
     * @param entries 重建条目
     * @return 最大凭证；空列表为 {@code 0}
     */
    private static long maxEntryToken(List<CoreStateRestore.Entry> entries) {
        long max = 0;
        for (CoreStateRestore.Entry e : entries) {
            max = Math.max(max, e.leaseToken());
        }
        return max;
    }
}
