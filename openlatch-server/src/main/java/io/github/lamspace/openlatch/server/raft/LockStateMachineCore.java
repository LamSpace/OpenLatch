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
import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.command.AcquireCommand;
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.command.RenewCommand;
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.ReleaseResult;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import io.github.lamspace.openlatch.core.result.RenewResult;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.ApplyStatus;
import io.github.lamspace.openlatch.protocol.raft.RaftEntryType;
import io.github.lamspace.openlatch.protocol.raft.RaftLogEntry;
import io.github.lamspace.openlatch.protocol.raft.AcquirePayload;
import io.github.lamspace.openlatch.protocol.raft.ExpirePayload;
import io.github.lamspace.openlatch.protocol.raft.ReleasePayload;
import io.github.lamspace.openlatch.protocol.raft.RenewPayload;
import io.github.lamspace.openlatch.protocol.raft.SessionPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 复制状态机内核：Raft 日志条目 → {@link CoreEngine} 的应用与
 * {@link ShadowTable} 双写核算的唯一入口（详设 §4.2/§4.3；S1 PoC 内核转正，
 * design D1/D2/D9/D12）。
 *
 * <p><b>职责与边界</b>：本类只做"条目 → 状态迁移"，不接触网络、不感知角色；
 * 回执（{@link ApplyResult}）是非复制的应答辅助信息，Leader 用它完成客户端
 * 应答（{@code ReplicationGateway}），Follower 应用同一条目产生同样的
 * {@link CoreEngine} 迁移但回执无人消费。锁语义完全下沉 {@link CoreEngine}，
 * 本类是它的集群调用方——集群路径对引擎的每一次可变调用都发生在
 * {@link #apply} 内（design D12 的"引擎状态变更唯一漏斗"不变式）。
 *
 * <p><b>时间语义（§4.3）</b>：应用期间经 {@link EntryClock} 注入条目携带时刻，
 * 授予/续租的到期 = 条目时刻 + 租期，到期条目回放以条目时刻求到期集——
 * 物理时钟不进入任何状态迁移，同一序列在任何副本任何时刻重放结果一致。
 *
 * <p><b>会话映射</b>：逻辑会话 id（{@code (nodeId<<32)|localSeq}，§5.2）在
 * {@code sidMap} 登记后映射到本副本引擎的内部 sid；引擎随机 sid 不出本节点，
 * 跨副本对齐只经影子表（digest 以逻辑 id 表达）。
 *
 * <p><b>幂等性</b>：条目按日志序各应用一次（Ratis 提交后串行应用），但应用
 * 语义仍按幂等设计——SESSION_OPEN 重复登记为无操作，SESSION_CLOSE 对未登记
 * 会话无操作，到期条目由引擎的"凭证+到期时刻"陈旧校验兜底（ABA 安全）。
 *
 * <p><b>线程模型</b>：{@link #apply} 仅由状态机应用线程（单线程、条目间无并发，
 * Ratis {@code StateMachineUpdater}，design D10）调用；{@code applyLock} 兜底
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
    /** 逻辑会话 id → 本副本引擎内部 sid（引擎随机 sid 不出节点）。 */
    private final Map<Long, Long> sidMap = new HashMap<>();
    /** 锁语义核心，构造即装配、集群路径唯一可变入口为 {@link #apply}。 */
    private final CoreEngine engine;

    /** 应用失败计数（解析异常/未知类型），诊断与测试断言用。 */
    private volatile long applyFailures;

    /**
     * 构造内核并装配引擎。
     *
     * @param config core 限额与租约配置；null 抛 {@link NullPointerException}
     */
    public LockStateMachineCore(CoreConfig config) {
        this.config = java.util.Objects.requireNonNull(config);
        // 集群引擎恒不登记等待项（design D9）：notifyHead 事件源（队首迁移）
        // 只存在于单机路径，此处收到即说明集群路径误登记了等待项，记 WARN。
        this.engine = new CoreEngine(config, clock, (sid, rid, key) ->
                log.warn("unexpected notifyHead from cluster engine: sid={}, key={}", sid, key));
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
        if (shadow.hasSession(sid)) {
            shadow.removeSession(sid);
            Long local = sidMap.remove(sid);
            if (local != null) {
                engine.sessionClosed(local);
            }
            freed = shadow.dropSessionHolders(sid);
        } else {
            freed = List.of();
        }
        return ok(0).addAllFreedKeys(freed).build();
    }

    /**
     * LOCK_ACQUIRE_ENTRY：以 {@code queueIfBusy=false} 调引擎（集群等待队列不
     * 进引擎，design D9），授予时镜像影子表；需排队时回
     * {@link ApplyStatus#DENIED}（排队裁决由 Leader 侧在应用回调中完成，§4.5/D3）。
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
                req.getThreadId(), req.getLeaseMs(), false));
        return switch (r.outcome()) {
            case GRANTED -> {
                long expiresAt = entry.getWallClockMs() + r.grantedLeaseMs();
                shadow.grant(p.getSessionId(), req.getThreadId(), req.getKey(),
                        req.getLockType().getNumber(), r.leaseToken(), r.grantedLeaseMs(), expiresAt);
                yield ApplyResult.newBuilder()
                        .setStatus(ApplyStatus.OK)
                        .setLeaseToken(r.leaseToken())
                        .setGrantedLeaseMs(r.grantedLeaseMs())
                        .setLeaseExpiresAtMs(expiresAt)
                        .build();
            }
            case DENIED -> ApplyResult.newBuilder().setStatus(ApplyStatus.DENIED).build();
            case REJECT_SESSION -> ApplyResult.newBuilder().setStatus(ApplyStatus.REJECT_SESSION).build();
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
                local, req.getKey(), req.getLeaseToken(), req.getThreadId()));
        if (r.status() == ReleaseStatus.OK) {
            shadow.release(p.getSessionId(), req.getThreadId(), req.getKey());
        }
        ApplyStatus st = switch (r.status()) {
            case OK -> ApplyStatus.OK;
            case NOT_HELD -> ApplyStatus.NOT_HELD;
            case INVALID_TOKEN -> ApplyStatus.INVALID_TOKEN;
            case REJECT_SESSION -> ApplyStatus.REJECT_SESSION;
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
     * 守卫不通过（已易主/尚未到期/已不存在）时整条空操作（spec ABA 场景，
     * §4.3/P2-09）。守卫匹配时 {@code expireDue()} 顺带收敛同一时刻到期的
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
        // 回放守卫（spec"过期条目不误杀新持有者"）：条目 token 与当前持有
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
     * 2 READ / 3 WRITE）；越界回 {@code null}。
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
}
