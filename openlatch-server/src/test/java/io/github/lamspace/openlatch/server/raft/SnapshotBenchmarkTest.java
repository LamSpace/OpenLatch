package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.ApplyStatus;
import io.github.lamspace.openlatch.protocol.raft.RaftLogEntry;
import io.github.lamspace.openlatch.protocol.raft.SnapshotState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 10 万锁条目快照基准（详设 §2.4 门槛 + §10"快照"层，S4/P2-16 验证列
 * "恢复 &lt; 30s；全量比对一致"，design D9）。
 *
 * <p><b>构造取向</b>：apply 直灌（每条目独立序列化→解析→应用，贴近真实
 * apply 路径而不引入网络吞吐变量）。状态形态刻意含 <b>历史释放空洞</b>
 * （5 万条 acquire+release 交错）与双持有（读锁两读者/重入两读者），使
 * 基准同时是 design D10 发号水位在规模下的判据载体。
 *
 * <p><b>度量口径</b>：序列化（applyLock 内一致性副本）、落盘（真实磁盘写
 * +原子 rename+MD5 伴随）、加载（快照解析+引擎重建）、尾部回放到"恢复完成"
 * 的分段耗时；全量比对用 {@link StateComparisons#diff}（结构级、逐字段，
 * MUST NOT 抽样）。
 */
@Timeout(value = 240, unit = TimeUnit.SECONDS)
class SnapshotBenchmarkTest {

    /** 存活锁条目数（§2.4 规模）。 */
    private static final int LIVE_LOCKS = 100_000;
    /** 释放空洞条目对数（acquire+release 各一）。 */
    private static final int CHURN_PAIRS = 50_000;
    /** 逻辑会话数。 */
    private static final int SESSIONS = 500;
    /** 恢复完成判定阈值（毫秒，§2.4 门槛 30s）。 */
    private static final long RECOVERY_BUDGET_MS = 30_000;

    @Test
    void hundredThousandEntriesSnapshotRecoveryWithinBudget() throws Exception {
        LockStateMachineCore origin = new LockStateMachineCore(new CoreConfig());
        long wall = 1_000;
        long seq = 1;

        // ---- 前缀构造：会话登记 + 10 万存活锁 + 5 万释放空洞（交错进发号序列） ----
        for (long s = 1; s <= SESSIONS; s++) {
            apply(origin, RaftEntrySamples.sessionOpen(1L << 32 | s, wall, seq++));
        }
        long half = LIVE_LOCKS / 2;
        for (int i = 0; i < LIVE_LOCKS; i++) {
            // 交错注入空洞：每两个存活授予之间做一次 churn acquire+release。
            if (i < CHURN_PAIRS) {
                String ck = "churn-" + i;
                long cSid = 1L << 32 | (i % SESSIONS + 1);
                ApplyResult g = apply(origin, RaftEntrySamples.acquire(cSid, 900_000 + i,
                        ck, wall + i, LockType.LOCK_TYPE_REENTRANT, seq++));
                assertThat(g.getStatus()).isEqualTo(ApplyStatus.OK);
                apply(origin, RaftEntrySamples.release(cSid, ck, g.getLeaseToken(),
                        wall + i, seq++));
            }
            long sid = 1L << 32 | (i % SESSIONS + 1);
            if (i < half) {
                // 前半：可重入写持有（含两层重入者）。
                apply(origin, RaftEntrySamples.acquire(sid, 1_000 + i, "live-" + i,
                        wall + i, LockType.LOCK_TYPE_REENTRANT, seq++));
                if (i % 3 == 0) {
                    apply(origin, RaftEntrySamples.acquire(sid, 1_000 + i + LIVE_LOCKS,
                            "live-" + i, wall + i, LockType.LOCK_TYPE_REENTRANT, seq++));
                }
            } else {
                // 后半：读锁双读者（共享凭证）。
                apply(origin, RaftEntrySamples.acquire(sid, 1_000 + i, "live-" + i,
                        wall + i, LockType.LOCK_TYPE_READ, seq++));
                long sid2 = 1L << 32 | ((i + 7) % SESSIONS + 1);
                apply(origin, RaftEntrySamples.acquire(sid2, 2_000 + i, "live-" + i,
                        wall + i, LockType.LOCK_TYPE_READ, seq++));
            }
        }
        assertThat(origin.shadow().lockCount()).isEqualTo(LIVE_LOCKS);
        assertThat(origin.applyFailures()).isZero();
        long wallMark = wall + LIVE_LOCKS + 1_000;

        // ---- 度量 1：一致性副本序列化（takeSnapshot 锁内段） ----
        long t0 = System.nanoTime();
        SnapshotState snapshot = origin.snapshotState();
        long serializeMs = (System.nanoTime() - t0) / 1_000_000;
        byte[] bytes = snapshot.toByteArray();

        // ---- 度量 2：落盘（真实写 + 原子 rename + MD5 伴随，路径同库命名） ----
        Path dir = Files.createTempDirectory("openlatch-bench-snap-");
        Path dst = dir.resolve("snapshot.7_421000");
        long t1 = System.nanoTime();
        Path tmp = dir.resolve("snapshot.tmp");
        Files.write(tmp, bytes);
        Files.move(tmp, dst, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        org.apache.ratis.util.MD5FileUtil.computeAndSaveMd5ForFile(dst.toFile());
        long writeMs = (System.nanoTime() - t1) / 1_000_000;

        // ---- 度量 3：加载（解析 + 引擎整体重建）与尾部回放，合并为"恢复" ----
        SnapshotState fromDisk = SnapshotState.parseFrom(Files.readAllBytes(dst));
        List<RaftLogEntry> tail = new ArrayList<>();
        for (int i = 0; i < 1_000; i++) {
            long sid = 1L << 32 | (i % SESSIONS + 1);
            long token = snapshot.getLocks(i).getLeaseToken();
            tail.add(RaftEntrySamples.renew(sid, "live-" + snapshot.getLocks(i).getKey(),
                    token, 45_000, wallMark + i, seq++));
        }
        for (int i = 0; i < 1_000; i++) {
            tail.add(RaftEntrySamples.acquire(1L << 32 | 1, 3_000 + i, "tail-" + i,
                    wallMark + 1_000 + i, LockType.LOCK_TYPE_REENTRANT, seq++));
        }
        // 尾部含一个到期条目（继承堆与到期驱动在规模下协同）。
        tail.add(RaftEntrySamples.expire("live-0", snapshot.getLocks(0).getLeaseToken(),
                wallMark + 60_000, seq));

        long t2 = System.nanoTime();
        LockStateMachineCore restored = new LockStateMachineCore(new CoreConfig());
        restored.installSnapshot(fromDisk);
        applyAll(restored, tail);
        long recoveryMs = (System.nanoTime() - t2) / 1_000_000;

        applyAll(origin, tail);

        // ---- 全量比对（结构级逐字段，MUST NOT 抽样） ----
        List<String> diffs = StateComparisons.diff(origin.snapshotState(), restored.snapshotState());
        assertThat(diffs).isEmpty();
        assertThat(restored.digest()).isEqualTo(origin.digest());
        assertThat(restored.applyFailures()).isZero();

        System.out.printf("[bench] liveLocks=%d entries=%d snapshotBytes=%d "
                        + "serializeMs=%d writeMs=%d recoveryMs(install+tail)=%d budgetMs=%d%n",
                LIVE_LOCKS, seq, bytes.length, serializeMs, writeMs, recoveryMs, RECOVERY_BUDGET_MS);
        assertThat(recoveryMs).as("加载+回放恢复总耗时（§2.4：10 万条目 < 30s）")
                .isLessThan(RECOVERY_BUDGET_MS);

        Files.walk(dir).sorted(java.util.Comparator.reverseOrder()).forEach(f -> {
            try {
                Files.deleteIfExists(f);
            } catch (Exception ignored) {
                // best effort
            }
        });
    }

    /** 应用条目并解析回执。 */
    private static ApplyResult apply(LockStateMachineCore core, RaftLogEntry entry)
            throws Exception {
        return ApplyResult.parseFrom(core.applyEntry(entry.toByteArray()));
    }

    private static void applyAll(LockStateMachineCore core, List<RaftLogEntry> entries)
            throws Exception {
        for (RaftLogEntry e : entries) {
            ApplyResult r = apply(core, e);
            assertThat(r.getStatus()).isNotEqualTo(ApplyStatus.INTERNAL_ERROR);
        }
    }
}
