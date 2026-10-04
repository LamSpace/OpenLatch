package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.LatchCountDownRequest;
import io.github.lamspace.openlatch.protocol.LeaseRenewRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.raft.AcquirePayload;
import io.github.lamspace.openlatch.protocol.raft.AtomicOpPayload;
import io.github.lamspace.openlatch.protocol.raft.ExpirePayload;
import io.github.lamspace.openlatch.protocol.raft.BarrierActionDonePayload;
import io.github.lamspace.openlatch.protocol.raft.BarrierAwaitPayload;
import io.github.lamspace.openlatch.protocol.raft.BarrierLeavePayload;
import io.github.lamspace.openlatch.protocol.raft.LatchCountDownPayload;
import io.github.lamspace.openlatch.protocol.raft.QueueOpPayload;
import io.github.lamspace.openlatch.protocol.raft.RaftEntryType;
import io.github.lamspace.openlatch.protocol.raft.RaftLogEntry;
import io.github.lamspace.openlatch.protocol.raft.ReleasePayload;
import io.github.lamspace.openlatch.protocol.raft.RenewPayload;
import io.github.lamspace.openlatch.protocol.raft.SessionPayload;

import com.google.protobuf.ByteString;

/**
 * 测试用复制条目样本工厂：集中构造各类型复制条目，供确定性回放测试、
 * 网关集成测试与到期用例复用（避免各测试类内联重复构造）。
 */
final class RaftEntrySamples {

    private RaftEntrySamples() {
    }

    /** 会话登记条目。 */
    static RaftLogEntry sessionOpen(long sessionId, long wallMs, long seq) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.SESSION_OPEN).setSeq(seq)
                .setWallClockMs(wallMs)
                .setCommandPayload(SessionPayload.newBuilder().setSessionId(sessionId).build().toByteString())
                .build();
    }

    /** 会话关闭条目。 */
    static RaftLogEntry sessionClose(long sessionId, long wallMs, long seq) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.SESSION_CLOSE).setSeq(seq)
                .setWallClockMs(wallMs)
                .setCommandPayload(SessionPayload.newBuilder().setSessionId(sessionId).build().toByteString())
                .build();
    }

    /** 获取锁条目（queue_if_busy 经 wait_ms 表达；此处 wait_ms=-1 表示客户端愿意排队）。 */
    static RaftLogEntry acquire(long sessionId, long requestId, String key, long wallMs,
                                LockType type, long seq) {
        return acquireWithWait(sessionId, requestId, key, wallMs, type, seq, -1, 60_000, 7);
    }

    /** 获取锁条目（完整参数）。 */
    static RaftLogEntry acquireWithWait(long sessionId, long requestId, String key, long wallMs,
                                        LockType type, long seq, long waitMs, long leaseMs, long threadId) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.LOCK_ACQUIRE_ENTRY).setSeq(seq)
                .setWallClockMs(wallMs)
                .setCommandPayload(AcquirePayload.newBuilder()
                        .setSessionId(sessionId).setRequestId(requestId)
                        .setRequest(AcquireRequest.newBuilder()
                                .setKey(key).setLockType(type)
                                .setThreadId(threadId).setLeaseMs(leaseMs).setWaitMs(waitMs))
                        .build().toByteString())
                .build();
    }

    /** Semaphore 获取条目（许可请求与总量断言）。 */
    static RaftLogEntry acquireSemaphore(long sessionId, long requestId, String key, long wallMs,
                                         int permits, int permitsTotal, long seq, long leaseMs) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.LOCK_ACQUIRE_ENTRY).setSeq(seq)
                .setWallClockMs(wallMs)
                .setCommandPayload(AcquirePayload.newBuilder()
                        .setSessionId(sessionId).setRequestId(requestId)
                        .setRequest(AcquireRequest.newBuilder()
                                .setKey(key).setLockType(LockType.LOCK_TYPE_SEMAPHORE)
                                .setThreadId(7).setLeaseMs(leaseMs).setWaitMs(-1)
                                .setPermits(permits).setPermitsTotal(permitsTotal))
                        .build().toByteString())
                .build();
    }

    /** Semaphore 归还条目（许可数）。 */
    static RaftLogEntry releasePermits(long sessionId, String key, long token, int permits,
                                       long wallMs, long seq) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.LOCK_RELEASE_ENTRY).setSeq(seq)
                .setWallClockMs(wallMs)
                .setCommandPayload(ReleasePayload.newBuilder()
                        .setSessionId(sessionId)
                        .setRequest(ReleaseRequest.newBuilder()
                                .setKey(key).setLeaseToken(token).setThreadId(7).setPermits(permits))
                        .build().toByteString())
                .build();
    }

    /** 屏障倒计数条目（count=0 且 total>0 为纯初始化）。 */
    static RaftLogEntry latchCountDown(long sessionId, String key, long count, long total,
                                       long wallMs, long seq) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.LATCH_COUNT_DOWN_ENTRY).setSeq(seq)
                .setWallClockMs(wallMs)
                .setCommandPayload(LatchCountDownPayload.newBuilder()
                        .setSessionId(sessionId)
                        .setRequest(LatchCountDownRequest.newBuilder()
                                .setKey(key).setCount(count).setTotal(total))
                        .build().toByteString())
                .build();
    }

    /** 原子操作条目（v4；GET 传 opSeq=0）。 */
    static RaftLogEntry atomic(long sessionId, long requestId, String key,
                               io.github.lamspace.openlatch.protocol.AtomicOp op,
                               LockType kind, long operand, long expected, long expectedVersion,
                               long initialValue, long opSeq, long wallMs, long seq) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.ATOMIC_OP_ENTRY).setSeq(seq)
                .setWallClockMs(wallMs)
                .setCommandPayload(AtomicOpPayload.newBuilder()
                        .setSessionId(sessionId).setRequestId(requestId)
                        .setRequest(io.github.lamspace.openlatch.protocol.AtomicOpRequest.newBuilder()
                                .setKey(key).setOp(op).setLockType(kind)
                                .setOperand(operand).setExpected(expected)
                                .setExpectedVersion(expectedVersion).setInitialValue(initialValue)
                                .setOpSeq(opSeq))
                        .build().toByteString())
                .build();
    }

    /** 有值引用原子操作条目（v6，载荷数组 {@code null}=字段缺省即 null 态）。 */
    static RaftLogEntry atomicRefSample(long sessionId, long requestId, String key,
                                        io.github.lamspace.openlatch.protocol.AtomicOp op,
                                        byte[] operand, byte[] expected, byte[] initial,
                                        long expectedVersion, long opSeq, long wallMs, long seq) {
        io.github.lamspace.openlatch.protocol.AtomicOpRequest.Builder rb =
                io.github.lamspace.openlatch.protocol.AtomicOpRequest.newBuilder()
                        .setKey(key).setOp(op).setLockType(
                                LockType.LOCK_TYPE_ATOMIC_REFERENCE)
                        .setExpectedVersion(expectedVersion).setOpSeq(opSeq);
        if (operand != null) {
            rb.setOperandBytes(com.google.protobuf.ByteString.copyFrom(operand));
        }
        if (expected != null) {
            rb.setExpectedBytes(com.google.protobuf.ByteString.copyFrom(expected));
        }
        if (initial != null) {
            rb.setInitialBytes(com.google.protobuf.ByteString.copyFrom(initial));
        }
        return RaftLogEntry.newBuilder().setType(RaftEntryType.ATOMIC_OP_ENTRY).setSeq(seq)
                .setWallClockMs(wallMs)
                .setCommandPayload(AtomicOpPayload.newBuilder()
                        .setSessionId(sessionId).setRequestId(requestId).setRequest(rb)
                        .build().toByteString())
                .build();
    }

    /**
     * 队列操作条目（v7）：blocking 位恒按参数承载（apply 侧固定立即式，
     * 本夹具用于构造确定性回弹场景）。
     */
    static RaftLogEntry queueSample(long sessionId, long requestId, String key,
                                    io.github.lamspace.openlatch.protocol.LockType kind,
                                    io.github.lamspace.openlatch.protocol.QueueOp op,
                                    boolean blocking, long capacity, byte[] element,
                                    long delayMs, int maxElements, long opSeq,
                                    long wallMs, long seq) {
        io.github.lamspace.openlatch.protocol.QueueOpRequest.Builder rb =
                io.github.lamspace.openlatch.protocol.QueueOpRequest.newBuilder()
                        .setKey(key).setOp(op).setLockType(kind).setBlocking(blocking)
                        .setCapacity(capacity).setDelayMs(delayMs)
                        .setMaxElements(maxElements).setOpSeq(opSeq);
        if (element != null) {
            rb.setElementBytes(com.google.protobuf.ByteString.copyFrom(element));
        }
        return RaftLogEntry.newBuilder().setType(RaftEntryType.QUEUE_OP_ENTRY).setSeq(seq)
                .setWallClockMs(wallMs)
                .setCommandPayload(QueueOpPayload.newBuilder()
                        .setSessionId(sessionId).setRequestId(requestId).setRequest(rb)
                        .build().toByteString())
                .build();
    }

    /** 循环屏障到场条目（v5）。 */
    static RaftLogEntry barrierAwait(long sessionId, long requestId, String key, long parties,
                                     boolean carriesAction, long wallMs, long seq) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.BARRIER_AWAIT_ENTRY).setSeq(seq)
                .setWallClockMs(wallMs)
                .setCommandPayload(BarrierAwaitPayload.newBuilder()
                        .setSessionId(sessionId).setRequestId(requestId)
                        .setRequest(io.github.lamspace.openlatch.protocol.BarrierAwaitRequest
                                .newBuilder().setKey(key).setParties(parties)
                                .setCarriesAction(carriesAction))
                        .build().toByteString())
                .build();
    }

    /** 循环屏障离场条目（v5；awaitRequestId=0 为纯破障主张）。 */
    static RaftLogEntry barrierLeave(long sessionId, String key, long awaitRequestId,
                                     long wallMs, long seq) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.BARRIER_LEAVE_ENTRY).setSeq(seq)
                .setWallClockMs(wallMs)
                .setCommandPayload(BarrierLeavePayload.newBuilder()
                        .setSessionId(sessionId)
                        .setRequest(io.github.lamspace.openlatch.protocol.BarrierLeaveRequest
                                .newBuilder().setKey(key).setAwaitRequestId(awaitRequestId))
                        .build().toByteString())
                .build();
    }

    /** 循环屏障动作了结条目（v5）。 */
    static RaftLogEntry barrierActionDone(long sessionId, String key, long generation,
                                          long wallMs, long seq) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.BARRIER_ACTION_DONE_ENTRY)
                .setSeq(seq)
                .setWallClockMs(wallMs)
                .setCommandPayload(BarrierActionDonePayload.newBuilder()
                        .setSessionId(sessionId)
                        .setRequest(io.github.lamspace.openlatch.protocol.BarrierActionDoneRequest
                                .newBuilder().setKey(key).setGeneration(generation))
                        .build().toByteString())
                .build();
    }

    /** 释放锁条目。 */
    static RaftLogEntry release(long sessionId, String key, long token, long wallMs, long seq) {
        return release(sessionId, key, token, wallMs, seq, 7);
    }

    /** 释放锁条目（指定线程）。 */
    static RaftLogEntry release(long sessionId, String key, long token, long wallMs, long seq, long threadId) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.LOCK_RELEASE_ENTRY).setSeq(seq)
                .setWallClockMs(wallMs)
                .setCommandPayload(ReleasePayload.newBuilder()
                        .setSessionId(sessionId)
                        .setRequest(ReleaseRequest.newBuilder()
                                .setKey(key).setLeaseToken(token).setThreadId(threadId))
                        .build().toByteString())
                .build();
    }

    /** 续租条目。 */
    static RaftLogEntry renew(long sessionId, String key, long token, long leaseMs, long wallMs, long seq) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.LEASE_RENEW_ENTRY).setSeq(seq)
                .setWallClockMs(wallMs)
                .setCommandPayload(RenewPayload.newBuilder()
                        .setSessionId(sessionId)
                        .setRequest(LeaseRenewRequest.newBuilder()
                                .setKey(key).setLeaseToken(token).setLeaseMs(leaseMs))
                        .build().toByteString())
                .build();
    }

    /** 到期条目。 */
    static RaftLogEntry expire(String key, long token, long wallMs, long seq) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.LEASE_EXPIRE_ENTRY).setSeq(seq)
                .setWallClockMs(wallMs)
                .setCommandPayload(ExpirePayload.newBuilder().setKey(key).setLeaseToken(token).build().toByteString())
                .build();
    }

    /** NOOP 条目。 */
    static RaftLogEntry noop(long wallMs, long seq) {
        return RaftLogEntry.newBuilder().setType(RaftEntryType.NOOP).setSeq(seq)
                .setWallClockMs(wallMs).build();
    }
}
