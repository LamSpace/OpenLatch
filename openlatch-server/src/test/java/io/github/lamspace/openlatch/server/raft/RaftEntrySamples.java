package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.LeaseRenewRequest;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.raft.AcquirePayload;
import io.github.lamspace.openlatch.protocol.raft.ExpirePayload;
import io.github.lamspace.openlatch.protocol.raft.RaftEntryType;
import io.github.lamspace.openlatch.protocol.raft.RaftLogEntry;
import io.github.lamspace.openlatch.protocol.raft.ReleasePayload;
import io.github.lamspace.openlatch.protocol.raft.RenewPayload;
import io.github.lamspace.openlatch.protocol.raft.SessionPayload;

import com.google.protobuf.ByteString;

/**
 * 测试用复制条目样本工厂：集中构造 §4.2 各类型条目，供确定性回放测试、
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
