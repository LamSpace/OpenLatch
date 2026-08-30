package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.ApplyStatus;
import io.github.lamspace.openlatch.protocol.raft.RaftLogEntry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 回放确定性测试（详设 §10"状态机单元"层，P2-06 验证列；PoC
 * {@code DeterminismTest} 转正并扩展，design D2）。
 *
 * <p>判据全部落在复制状态摘要（{@link ShadowTable#digest()}）上：同序列
 * 两次应用摘要一致（含到期/续租/会话关闭对时刻的敏感路径）、到期语义
 * 不依赖物理时钟、ABA 幂等、EntryClock 线程契约（串扰反例）。
 */
class StateMachineDeterminismTest {

    /** 以序列化字节应用全序列并返回复制状态摘要。 */
    private static String replay(List<RaftLogEntry> seq) {
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        for (RaftLogEntry e : seq) {
            core.applyEntry(e.toByteArray());
        }
        assertThat(core.applyFailures()).isZero();
        return core.digest();
    }

    @Test
    void sameSequenceReplayedTwiceYieldsIdenticalDigest() {
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(11, 1_000, 1),
                RaftEntrySamples.sessionOpen(12, 1_000, 2),
                RaftEntrySamples.acquire(11, 101, "a", 2_000, LockType.LOCK_TYPE_REENTRANT, 3),
                RaftEntrySamples.acquire(11, 102, "b", 3_000, LockType.LOCK_TYPE_REENTRANT, 4),
                RaftEntrySamples.acquire(12, 103, "c", 4_000, LockType.LOCK_TYPE_READ, 5),
                RaftEntrySamples.noop(4_500, 6),
                RaftEntrySamples.renew(11, "a", 1, 60_000, 5_000, 7),
                RaftEntrySamples.sessionClose(12, 6_000, 8));
        assertThat(replay(seq)).isEqualTo(replay(seq));
    }

    @Test
    void leaseTimeComesFromEntryNotPhysicalClock() {
        // 同一逻辑条目、不同携带时刻 → 到期时刻不同（摘要不同）；
        // 各自重放稳定（与回放时的物理时钟无关）。
        List<RaftLogEntry> early = List.of(
                RaftEntrySamples.sessionOpen(21, 5_000, 1),
                RaftEntrySamples.acquire(21, 201, "k", 5_000, LockType.LOCK_TYPE_REENTRANT, 2));
        List<RaftLogEntry> late = List.of(
                RaftEntrySamples.sessionOpen(21, 5_000_000, 1),
                RaftEntrySamples.acquire(21, 201, "k", 5_000_000, LockType.LOCK_TYPE_REENTRANT, 2));
        String dEarly = replay(early);
        String dLate = replay(late);
        assertThat(dEarly).isNotEqualTo(dLate);
        assertThat(replay(early)).isEqualTo(dEarly);
        assertThat(replay(late)).isEqualTo(dLate);
    }

    @Test
    void expireEntryReleasesAtEntryTimeNotReplayTime() {
        // 租约 1s，到期条目携带时刻恰在到期之后：任何时刻回放，锁都已释放。
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(31, 1_000, 1),
                RaftEntrySamples.acquireWithWait(31, 301, "k", 1_000, LockType.LOCK_TYPE_REENTRANT, 2, -1, 1_000, 7),
                RaftEntrySamples.expire("k", 1, 2_000, 3));
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        for (RaftLogEntry e : seq) {
            core.applyEntry(e.toByteArray());
        }
        assertThat(core.shadow().isHeld("k")).isFalse();
        assertThat(core.shadow().lockCount()).isZero();
        assertThat(core.digest()).isEqualTo(replay(seq));
    }

    @Test
    void staleExpireEntryIsNoOpAfterReleaseAndReacquire() {
        // ABA：tok1 到期条目在途期间 key 已释放并被新会话以 tok2 重新授予，
        // 回放该到期条目不得误杀新持有者（token/到期时刻双陈旧校验）。
        List<RaftLogEntry> seq = List.of(
                RaftEntrySamples.sessionOpen(41, 1_000, 1),
                RaftEntrySamples.sessionOpen(42, 1_000, 2),
                RaftEntrySamples.acquireWithWait(41, 401, "k", 1_000, LockType.LOCK_TYPE_REENTRANT, 3, -1, 5_000, 7),
                // tok1 到期 6000；41 在 2000 主动释放，42 于 6100 重新授予（新 token）
                RaftEntrySamples.release(41, "k", 1, 2_000, 4),
                RaftEntrySamples.acquireWithWait(42, 402, "k", 6_100, LockType.LOCK_TYPE_REENTRANT, 5, -1, 5_000, 8),
                // 迟到的到期条目：Leader 于 6200 发现 tok1 过期并提交（携带时刻 6200）
                RaftEntrySamples.expire("k", 1, 6_200, 6));
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        for (RaftLogEntry e : seq) {
            core.applyEntry(e.toByteArray());
        }
        // 42 的持有（到期 11100 > 6200）必须存活。
        assertThat(core.shadow().isHeld("k")).isTrue();
    }

    @Test
    void unregisteredSessionAcquireRejectedWithoutStateChange() throws Exception {
        LockStateMachineCore core = new LockStateMachineCore(new CoreConfig());
        ApplyResult r = ApplyResult.parseFrom(core.applyEntry(
                RaftEntrySamples.acquire(999, 501, "k", 1_000, LockType.LOCK_TYPE_REENTRANT, 1).toByteArray()));
        assertThat(r.getStatus()).isEqualTo(ApplyStatus.REJECT_SESSION);
        assertThat(core.digest()).isEqualTo(new LockStateMachineCore(new CoreConfig()).digest());
        assertThat(core.applyFailures()).isZero();
    }

    @Test
    void sessionCloseReleasesHeldLocksOnAllMirrors() {
        String digest = replay(List.of(
                RaftEntrySamples.sessionOpen(51, 1_000, 1),
                RaftEntrySamples.acquire(51, 501, "k", 2_000, LockType.LOCK_TYPE_REENTRANT, 2),
                RaftEntrySamples.sessionClose(51, 3_000, 3)));
        assertThat(digest).isEqualTo(new LockStateMachineCore(new CoreConfig()).digest());
    }

    @ParameterizedTest(name = "随机混排序列 seed={0}")
    @ValueSource(longs = {1L, 7L, 42L, 1337L, 20260830L, 9001L, 555L, 31L})
    void randomMixedSequenceIsDeterministic(long seed) {
        List<RaftLogEntry> seq = randomSequence(new Random(seed));
        assertThat(replay(seq)).isEqualTo(replay(seq));
    }

    @Test
    void concurrentAppliesDoNotCrossLeakEntryClock() throws Exception {
        // EntryClock 为 thread-local：两线程并发应用不同时刻序列，结果与
        // 单线程一致（若标记跨线程泄漏，digest 必偏离）。
        Random r = new Random(2026);
        List<RaftLogEntry> seqA = randomSequence(r);
        List<RaftLogEntry> seqB = randomSequence(new Random(1234));
        try (ExecutorService pool = Executors.newFixedThreadPool(2)) {
            Future<String> fa = pool.submit(() -> replay(seqA));
            Future<String> fb = pool.submit(() -> replay(seqB));
            assertThat(fa.get()).isEqualTo(replay(seqA));
            assertThat(fb.get()).isEqualTo(replay(seqB));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        } catch (ExecutionException e) {
            throw new AssertionError(e.getCause());
        }
    }

    @Test
    void entryClockFallsBackToSystemClockOutsideApply() {
        long before = System.currentTimeMillis();
        EntryClock.clearApplyNow(); // 确保无残留标记
        long read = new EntryClock().nowMs();
        assertThat(read).isBetween(before, System.currentTimeMillis() + 50);
        // 应用线程内的 set 对主线程不可见（thread-local 隔离的串扰反例）。
        Thread t = new Thread(() -> EntryClock.setApplyNow(12345));
        t.start();
        try {
            t.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        assertThat(new EntryClock().nowMs()).isNotEqualTo(12345);
    }

    /**
     * 生成一组语义合法的随机条目序列：会话开闭、四类锁获取（含可排队
     * 标记）、释放、续租、随机位置的到期条目（携带时刻单调不减，与
     * Leader 扫描事实一致）。
     */
    private static List<RaftLogEntry> randomSequence(Random rnd) {
        List<RaftLogEntry> seq = new ArrayList<>();
        long[] sids = {61, 62, 63};
        String[] keys = {"k1", "k2", "k3"};
        LockType[] types = {LockType.LOCK_TYPE_REENTRANT, LockType.LOCK_TYPE_SIMPLE,
                LockType.LOCK_TYPE_READ, LockType.LOCK_TYPE_WRITE};
        long t = 1_000;
        long seqNo = 1;
        for (long sid : sids) {
            seq.add(RaftEntrySamples.sessionOpen(sid, t, seqNo++));
        }
        long lastExpireT = t;
        int ops = 30 + rnd.nextInt(50);
        for (int i = 0; i < ops; i++) {
            t += 500 + rnd.nextInt(2_000);
            long sid = sids[rnd.nextInt(sids.length)];
            String key = keys[rnd.nextInt(keys.length)];
            int pick = rnd.nextInt(10);
            if (pick < 4) {
                seq.add(RaftEntrySamples.acquireWithWait(sid, 1000 + i, key, t,
                        types[rnd.nextInt(types.length)], seqNo++,
                        rnd.nextBoolean() ? -1 : 0, 1_000 + rnd.nextInt(5_000), 1 + rnd.nextInt(3)));
            } else if (pick < 6) {
                seq.add(RaftEntrySamples.release(sid, key, 1 + rnd.nextInt(4), t, seqNo++));
            } else if (pick < 8) {
                seq.add(RaftEntrySamples.renew(sid, key, 1 + rnd.nextInt(4),
                        1_000 + rnd.nextInt(5_000), t, seqNo++));
            } else if (pick < 9) {
                // 到期条目：携带时刻单调不减（Leader 物理时钟视角）。
                lastExpireT = t;
                seq.add(RaftEntrySamples.expire(keys[rnd.nextInt(keys.length)], 1 + rnd.nextInt(4),
                        lastExpireT, seqNo++));
            } else {
                seq.add(RaftEntrySamples.noop(t, seqNo++));
            }
        }
        // 全部 key 的"到期后清扫"：末位再放一条到期条目，确保序列对
        // expireUpTo/expireDue 的收尾路径也成立。
        seq.add(RaftEntrySamples.expire(keys[0], 0, lastExpireT + 60_000, seqNo));
        return seq;
    }
}
