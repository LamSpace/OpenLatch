package io.github.lamspace.openlatch.poc.harness;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.core.CoreEngine;
import io.github.lamspace.openlatch.core.CoreEventListener;
import io.github.lamspace.openlatch.core.LockType;
import io.github.lamspace.openlatch.core.command.AcquireCommand;
import io.github.lamspace.openlatch.core.command.ReleaseCommand;
import io.github.lamspace.openlatch.core.result.AcquireResult;
import io.github.lamspace.openlatch.core.result.Outcome;
import io.github.lamspace.openlatch.core.result.ReleaseResult;
import io.github.lamspace.openlatch.core.result.ReleaseStatus;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.ApplyResult;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.AcquirePayload;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.RaftEntryType;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.RaftLogEntry;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.ReleasePayload;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.ResultStatus;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.SessionPayload;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.ShadowHolder;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.ShadowLock;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.ShadowState;

import java.util.HashMap;
import java.util.Map;

/**
 * 共享状态机内核（详设 §4.2/§4.3 的 PoC 形态，两候选唯一复用点）。
 *
 * <p>条目 → {@link CoreEngine} 的应用、影子表维护、快照序列化/回灌全部在此，
 * 适配器只做库的搬运。{@code CoreEngine} 零改动接入（design D3）：
 * 租约时刻经 {@link EntryClock} 取条目携带时刻；engine 内部随机 sid 以
 * 逻辑会话 id → 本地 sid 的映射层隔离（PoC 发现：leaseToken 为 engine
 * 内部单调计数器，同序应用天然确定，无需映射）。
 *
 * <p>线程模型：{@link #applyEntry} 由库 applier 线程调用；{@code applyLock}
 * 兜底串行（库 applier 若非单线程，本身即门槛四证据）。
 */
public final class LockStateMachineCore {

    /** apply 串行化锁（兼保护 engine/shadow 一致性快照）。 */
    private final Object applyLock = new Object();
    private final CoreConfig config;
    private final EntryClock clock = new EntryClock();
    private final ShadowTable shadow = new ShadowTable();
    /** 逻辑会话 id → 本节点 engine sid（engine 随机 sid 不出节点）。 */
    private final Map<Long, Long> sidMap = new HashMap<>();
    private CoreEngine engine;

    /** 快照回灌重建次数与失败计数（报告指标）。 */
    public volatile long rebuildCount;
    public volatile long rebuildFailures;

    /**
     * @param config core 限额与租约配置
     */
    public LockStateMachineCore(CoreConfig config) {
        this.config = config;
        this.engine = newEngine();
    }

    private CoreEngine newEngine() {
        CoreEventListener noopListener = (sid, rid, key) -> {
            // PoC 不转发 AWAIT_NOTIFY：排队语义仅冒烟用例观察响应码。
        };
        return new CoreEngine(config, clock, noopListener);
    }

    /**
     * 应用一条复制条目（由库 applier 线程调用）。
     *
     * @param entryBytes RaftLogEntry 序列化
     * @return ApplyResult 序列化（供回执）
     */
    public byte[] applyEntry(byte[] entryBytes) {
        synchronized (applyLock) {
            EntryClock.setApplyNow(0);
            try {
                RaftLogEntry entry = RaftLogEntry.parseFrom(entryBytes);
                EntryClock.setApplyNow(entry.getWallClockMs());
                return switch (entry.getType()) {
                    case SESSION_OPEN -> applySessionOpen(entry);
                    case SESSION_CLOSE -> applySessionClose(entry);
                    case LOCK_ACQUIRE_ENTRY -> applyAcquire(entry);
                    case LOCK_RELEASE_ENTRY -> applyRelease(entry);
                    case NOOP -> result(ResultStatus.R_OK, 0, 0, 0).build().toByteArray();
                    default -> result(ResultStatus.R_ERROR, 0, 0, 0).build().toByteArray();
                };
            } catch (InvalidProtocolBufferException | RuntimeException e) {
                return result(ResultStatus.R_ERROR, 0, 0, 0).build().toByteArray();
            } finally {
                EntryClock.clearApplyNow();
            }
        }
    }

    private byte[] applySessionOpen(RaftLogEntry entry) throws InvalidProtocolBufferException {
        SessionPayload p = SessionPayload.parseFrom(entry.getCommandPayload());
        long sidT = p.getSessionId();
        if (!shadow.hasSession(sidT)) {
            long local = engine.sessionOpened();
            sidMap.put(sidT, local);
            shadow.addSession(sidT);
        }
        return result(ResultStatus.R_OK, 0, 0, 0).build().toByteArray();
    }

    private byte[] applySessionClose(RaftLogEntry entry) throws InvalidProtocolBufferException {
        SessionPayload p = SessionPayload.parseFrom(entry.getCommandPayload());
        long sidT = p.getSessionId();
        Long local = sidMap.remove(sidT);
        if (local != null) {
            engine.sessionClosed(local);
        }
        shadow.removeSession(sidT);
        return result(ResultStatus.R_OK, 0, 0, 0).build().toByteArray();
    }

    private byte[] applyAcquire(RaftLogEntry entry) throws InvalidProtocolBufferException {
        AcquirePayload p = AcquirePayload.parseFrom(entry.getCommandPayload());
        Long local = sidMap.get(p.getSessionId());
        if (local == null) {
            return result(ResultStatus.R_REJECT_SESSION, 0, 0, 0).build().toByteArray();
        }
        AcquireResult r = engine.acquire(new AcquireCommand(
                local, p.getRequestId(), p.getKey(), LockType.values()[p.getLockType()],
                p.getThreadId(), p.getRequestedLeaseMs(), p.getQueueIfBusy()));
        long now = entry.getWallClockMs();
        switch (r.outcome()) {
            case GRANTED -> shadow.grant(p.getSessionId(), p.getThreadId(), p.getKey(),
                    p.getLockType(), r.leaseToken(), r.grantedLeaseMs(), now + r.grantedLeaseMs());
            default -> { }
        }
        return switch (r.outcome()) {
            case GRANTED -> result(ResultStatus.R_OK, r.leaseToken(), r.grantedLeaseMs(), 0).build().toByteArray();
            case QUEUED -> result(ResultStatus.R_QUEUED, 0, 0, r.queuePosition()).build().toByteArray();
            case DENIED -> result(ResultStatus.R_DENIED, 0, 0, 0).build().toByteArray();
            case REJECT_SESSION -> result(ResultStatus.R_REJECT_SESSION, 0, 0, 0).build().toByteArray();
            default -> result(ResultStatus.R_ERROR, 0, 0, 0).build().toByteArray();
        };
    }

    private byte[] applyRelease(RaftLogEntry entry) throws InvalidProtocolBufferException {
        ReleasePayload p = ReleasePayload.parseFrom(entry.getCommandPayload());
        Long local = sidMap.get(p.getSessionId());
        if (local == null) {
            return result(ResultStatus.R_REJECT_SESSION, 0, 0, 0).build().toByteArray();
        }
        ReleaseResult r = engine.release(new ReleaseCommand(
                local, p.getKey(), p.getLeaseToken(), p.getThreadId()));
        if (r.status() == ReleaseStatus.OK) {
            // 影子表按"每次成功释放计数 -1"回退，与 engine 的 fullyReleased 判定等价收敛。
            shadow.release(p.getSessionId(), p.getThreadId(), p.getKey());
        }
        return switch (r.status()) {
            case OK -> result(ResultStatus.R_OK, 0, 0, 0).build().toByteArray();
            case NOT_HELD -> result(ResultStatus.R_NOT_HELD, 0, 0, 0).build().toByteArray();
            case INVALID_TOKEN -> result(ResultStatus.R_INVALID_TOKEN, 0, 0, 0).build().toByteArray();
            default -> result(ResultStatus.R_ERROR, 0, 0, 0).build().toByteArray();
        };
    }

    private static ApplyResult.Builder result(ResultStatus st, long token, long leaseMs, int pos) {
        return ApplyResult.newBuilder()
                .setStatusValue(st.getNumber()).setLeaseToken(token)
                .setLeaseMs(leaseMs).setQueuePosition(pos);
    }

    /** 快照载体：当前影子表 + 位点（term/index 由适配器补全后序列化）。 */
    public SnapshotBundle snapshotBundle(long term, long index) {
        synchronized (applyLock) {
            return new SnapshotBundle(term, index, shadow.serialize());
        }
    }

    /** 快照 bundle：位点 + 影子表字节。 */
    public record SnapshotBundle(long term, long index, byte[] shadowBytes) { }

    /**
     * 安装快照：重建 engine 与影子表（design D9；通用重建入口属 P2-16，
     * PoC 以"回灌授予序列"近似，token 非连续时显式失败并计入摩擦档案）。
     *
     * @return 回灌条目数
     */
    public long installSnapshot(byte[] shadowBytes) {
        synchronized (applyLock) {
            rebuildCount++;
            try {
                ShadowState st = ShadowState.parseFrom(shadowBytes);
                engine = newEngine();
                sidMap.clear();
                shadow.load(st);
                for (long sidT : st.getSessionsList()) {
                    sidMap.put(sidT, engine.sessionOpened());
                }
                // 影子表按授予序（token 单调）回灌：逐条直调 engine（不走 applyEntry，
                // 避免与快照后增量回放重复计数）。
                for (ShadowLock l : st.getLocksList()) {
                    long entryTime = l.getExpiresAt() - l.getLeaseMs();
                    EntryClock.setApplyNow(entryTime);
                    for (ShadowHolder h : l.getHoldersList()) {
                        Long local = sidMap.get(h.getSessionId());
                        AcquireResult r = engine.acquire(new AcquireCommand(
                                local, l.getLeaseToken(), l.getKey(),
                                LockType.values()[l.getLockType()], h.getThreadId(),
                                l.getLeaseMs(), false));
                        if (r.outcome() != Outcome.GRANTED || r.leaseToken() != l.getLeaseToken()) {
                            // 快照含历史释放空洞：回灌无法复现 token 序列（PoC 显式失败）。
                            rebuildFailures++;
                            throw new IllegalStateException(
                                    "snapshot rebuild token mismatch at key=" + l.getKey());
                        }
                    }
                }
                return st.getLocksCount();
            } catch (InvalidProtocolBufferException e) {
                rebuildFailures++;
                throw new IllegalStateException("bad snapshot payload", e);
            } finally {
                EntryClock.clearApplyNow();
            }
        }
    }

    /** 当前全量摘要（DUMP 比对）。 */
    public String digest() {
        synchronized (applyLock) {
            return shadow.digest();
        }
    }

    /** 影子表只读引用（适配器快照写入用）。 */
    public ShadowTable shadow() {
        return shadow;
    }
}
