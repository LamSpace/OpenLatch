package io.github.lamspace.openlatch.server.raft;

import io.github.lamspace.openlatch.server.raft.ReplicationStallWatchdog.Snapshot;
import io.github.lamspace.openlatch.server.raft.ReplicationStallWatchdog.StallActions;
import io.github.lamspace.openlatch.server.raft.ReplicationStallWatchdog.StallProbe;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link ReplicationStallWatchdog} 确定性单测（2B.1 verify 上半）：以脚本化
 * 探针与动作记录器注入全部时序参数（压缩用例时长至秒级），断言——
 * ①模拟停摆态在 ≤T_stall+ε 内触发让位、观察窗到期升级重启；②正常推进/选举窗
 * 零误伤；③每任期一次让位配额与重启冷却窗生效；④探针异常 fail-open。
 *
 * <p>ε 取 (M+8) 个采样周期（tick 调度抖动上界）；「不早于 T_stall」同时断言
 * （防误伤的另一半：阈值前禁止动作）。
 */
@Timeout(value = 60, unit = java.util.concurrent.TimeUnit.SECONDS)
class ReplicationStallWatchdogTest {

    /** 采样周期（毫秒，测试用小值）。 */
    private static final long SAMPLE_MS = 50;
    /** 停摆阈值 T_stall（测试用小值）。 */
    private static final long T_STALL_MS = 300;
    /** 零推进连续样本数 M。 */
    private static final int M = 3;
    /** 让位→升级观察窗（测试用小值）。 */
    private static final long GRACE_MS = 200;
    /** 触发时刻容差上界 ε（tick 抖动）。 */
    private static final long EPSILON_MS = (M + 8) * SAMPLE_MS;

    /** 记录型动作组：让位/重启调用计数与首触发时刻。 */
    private static final class RecordingActions implements StallActions {
        /** 让位调用次数。 */
        final AtomicInteger transfers = new AtomicInteger();
        /** 让位目标序列（按调用序记录）。 */
        final java.util.List<String> targets = java.util.Collections.synchronizedList(
                new java.util.ArrayList<>());
        /** 重启调用次数。 */
        final AtomicInteger restarts = new AtomicInteger();
        /** 让位首次调用时刻（0＝未发生）。 */
        final AtomicLong transferAtMs = new AtomicLong();
        /** 重启首次调用时刻（0＝未发生）。 */
        final AtomicLong restartAtMs = new AtomicLong();

        @Override
        public boolean transferLeadership(String peerId) {
            transferAtMs.compareAndSet(0, System.currentTimeMillis());
            targets.add(peerId);
            return transfers.incrementAndGet() == 1;
        }

        @Override
        public void restartProcess() {
            restartAtMs.compareAndSet(0, System.currentTimeMillis());
            restarts.incrementAndGet();
        }
    }

    /** 可编程探针：角色/任期/提交位点/对侧表均为原子可变槽。 */
    private static final class ScriptedProbe implements StallProbe {
        /** 是否 Leader。 */
        final AtomicBoolean leader = new AtomicBoolean(true);
        /** 任期。 */
        final AtomicLong term = new AtomicLong(7);
        /** 本节点 commitIndex（推进测试用自增）。 */
        final AtomicLong commit = new AtomicLong(553);
        /** 对侧 commitIndex 表。 */
        final AtomicReference<Map<String, Long>> peers =
                new AtomicReference<>(Map.of("n1", 244L, "n3", 398L));
        /** 探针抛异常开关（fail-open 测试）。 */
        final AtomicBoolean throwOnSample = new AtomicBoolean();

        @Override
        public Snapshot sample() throws Exception {
            if (throwOnSample.get()) {
                throw new IllegalStateException("division gone");
            }
            return new Snapshot(leader.get(), term.get(), commit.get(), peers.get());
        }
    }

    /** 轮询等待条件（超时返回 false，不抛出——由断言裁决）。 */
    private static boolean await(java.util.function.BooleanSupplier cond, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return true;
            }
            Thread.sleep(10);
        }
        return cond.getAsBoolean();
    }

    /** 停摆态：≤T_stall+ε 内让位至最新对侧；观察窗后仍未推进 → 升级重启一次。 */
    @Test
    void stalledLeaderTriggersTransferThenRestart() throws Exception {
        ScriptedProbe p = new ScriptedProbe();
        RecordingActions a = new RecordingActions();
        try (ReplicationStallWatchdog w = new ReplicationStallWatchdog(
                p, a, T_STALL_MS, SAMPLE_MS, M, GRACE_MS, 60_000)) {
            long t0 = System.currentTimeMillis();
            assertThat(await(() -> a.transfers.get() >= 1, T_STALL_MS + EPSILON_MS * 2))
                    .as("停摆让位在 ≤T_stall+ε 内触发").isTrue();
            long at = a.transferAtMs.get() - t0;
            assertThat(at).as("不早于 T_stall（阈值前禁止动作）").isGreaterThanOrEqualTo(T_STALL_MS);
            assertThat(a.targets).as("目标＝commitIndex 最新对侧 n3").element(0).isEqualTo("n3");

            assertThat(await(() -> a.restarts.get() >= 1, GRACE_MS + EPSILON_MS * 2))
                    .as("让位后提交仍冻结 → 观察窗到期升级重启").isTrue();
            // 观察窗基准是让位步序发起时刻（先于动作记录器时间戳 ≤1 个采样），
            // 断言取 GRACE_MS−SAMPLE_MS 容差防毫秒截边。
            long restartGap = a.restartAtMs.get() - a.transferAtMs.get();
            assertThat(restartGap).as("升级不早于观察窗")
                    .isGreaterThanOrEqualTo(GRACE_MS - SAMPLE_MS);
            // 此后保持冻结：配额/升级标记挡住重复动作。
            Thread.sleep(T_STALL_MS + GRACE_MS);
            assertThat(a.transfers.get()).as("每任期一次让位配额").isEqualTo(1);
            assertThat(a.restarts.get()).as("升级重启每 episode 一次").isEqualTo(1);
        }
    }

    /** 健康推进（NOOP 探针节奏）：远超阈值也零动作——正常 Leader 零误伤。 */
    @Test
    void advancingCommitNeverActs() throws Exception {
        ScriptedProbe p = new ScriptedProbe();
        RecordingActions a = new RecordingActions();
        AtomicBoolean run = new AtomicBoolean(true);
        // 推进节奏刻意快于采样（SAMPLE_MS/2）：两次相邻 tick 之间必然发生推进，
        // 零推进连击至多 1，结构上排除相位锁导致的误触发。
        Thread pump = Thread.ofPlatform().daemon(true).start(() -> {
            while (run.get()) {
                p.commit.incrementAndGet();
                try {
                    Thread.sleep(SAMPLE_MS / 2);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        try (ReplicationStallWatchdog w = new ReplicationStallWatchdog(
                p, a, T_STALL_MS, SAMPLE_MS, M, GRACE_MS, 60_000)) {
            Thread.sleep(6L * T_STALL_MS);
            assertThat(a.transfers.get()).as("提交在推进 → 零让位").isZero();
            assertThat(a.restarts.get()).as("提交在推进 → 零重启").isZero();
        } finally {
            run.set(false);
            pump.join(2_000);
        }
    }

    /** 选举空窗与非 Leader 角色天然豁免；任期变化重置让位配额。 */
    @Test
    void electionWindowsExemptAndTermChangeResetsQuota() throws Exception {
        ScriptedProbe p = new ScriptedProbe();
        RecordingActions a = new RecordingActions();
        try (ReplicationStallWatchdog w = new ReplicationStallWatchdog(
                p, a, T_STALL_MS, SAMPLE_MS, M, GRACE_MS, 60_000)) {
            // 选举空窗（含间歇探针异常）：持续 4×T_stall 零动作。
            p.leader.set(false);
            long dead = System.currentTimeMillis() + 4 * T_STALL_MS;
            while (System.currentTimeMillis() < dead) {
                p.throwOnSample.set(System.nanoTime() % 3 == 0);
                Thread.sleep(SAMPLE_MS);
            }
            p.throwOnSample.set(false);
            assertThat(a.transfers.get()).as("非 Leader/探针异常全程豁免").isZero();

            // 当选冻结 → 第一次让位；换任期后冻结 → 新 episode 再次让位（配额重置）。
            p.leader.set(true);
            assertThat(await(() -> a.transfers.get() >= 1, T_STALL_MS + EPSILON_MS * 2))
                    .as("term 7 停摆触发让位").isTrue();
            p.term.set(8);
            assertThat(await(() -> a.transfers.get() >= 2, T_STALL_MS + EPSILON_MS * 2))
                    .as("term 8 新 episode 再次可让位").isTrue();
            assertThat(a.targets).containsExactly("n3", "n3");
        }
    }

    /** 重启冷却窗：窗内升级仅 WARN 挂起，不二次触发重启路径。 */
    @Test
    void restartSuppressedWithinCooldown() throws Exception {
        ScriptedProbe p = new ScriptedProbe();
        RecordingActions a = new RecordingActions();
        try (ReplicationStallWatchdog w = new ReplicationStallWatchdog(
                p, a, T_STALL_MS, SAMPLE_MS, M, GRACE_MS, 600_000)) {
            // 首个 episode 走完升级（记录 lastRestartMs），换任期后的第二个
            // episode 落入 600s 冷却窗：让位步序照常、重启路径被冷却抑制。
            assertThat(await(() -> a.restarts.get() >= 1,
                    T_STALL_MS + GRACE_MS + EPSILON_MS * 4)).as("首次升级重启").isTrue();
            p.term.set(9);
            Thread.sleep(T_STALL_MS + GRACE_MS + EPSILON_MS);
            assertThat(a.restarts.get()).as("冷却窗内不二次触发重启路径").isEqualTo(1);
            // 冷却窗内让位配额仍可用（阶梯第一步不受冷却约束）。
            assertThat(a.transfers.get()).as("新任期让位照常").isGreaterThanOrEqualTo(2);
        }
    }

    /** 无可用对侧（孤立单主）：不发让位 RPC 但消耗步序，观察窗后照常升级重启。 */
    @Test
    void noPeerEscalatesOnlyAfterStillStalled() throws Exception {
        ScriptedProbe p = new ScriptedProbe();
        p.peers.set(Map.of());
        RecordingActions a = new RecordingActions();
        try (ReplicationStallWatchdog w = new ReplicationStallWatchdog(
                p, a, T_STALL_MS, SAMPLE_MS, M, GRACE_MS, 60_000)) {
            assertThat(await(() -> a.restarts.get() >= 1,
                    T_STALL_MS + GRACE_MS + EPSILON_MS * 2))
                    .as("无对侧：让位步序视同失败，升级重启照常").isTrue();
            assertThat(a.transfers.get()).as("空对侧不发让位 RPC").isZero();
        }
    }
}
