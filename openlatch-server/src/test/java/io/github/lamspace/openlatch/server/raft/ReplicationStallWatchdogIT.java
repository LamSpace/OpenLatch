package io.github.lamspace.openlatch.server.raft;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 复制停摆看门狗端到端集成用例（2B.1 验收补强）：以<b>真实</b> Ratis division
 * 状态验证生产装配路径（{@link ReplicationStallWatchdog#attach(RaftSubsystem)}
 * 的探针与动作组），补足单测（假探针）无法覆盖的「探针对真实冻结态判得出、
 * 让位 RPC 走真通道」两段——gate2 停摆复核中 stallEvents=0 的归因判别也依赖此
 * 用例的确定性结论。
 *
 * <p><b>构造手法</b>：{@link ClusterHarness} 三节点（election-timeout 800ms →
 * 生产折算 T_stall=max(10s, 5×800ms)=10s）；确认当值 Leader 后
 * {@code setProbesEnabled(false)} 并停止一切流量——集群随即进入「Leader 角色
 * 稳定持有（心跳多数派健康，无让位/无再选诱因）∧ commitIndex 恒冻结」的
 * 形态 A 稳态：与真实停摆的差异仅在冻结成因（成因本身正是被测点之外的输入）。
 *
 * <p><b>断言</b>：①在任后短窗口（&lt;T_stall）内 Leader 不变（阈值前禁止动作，
 * 含"刚当选尚未推进"不误伤）；②≤ T_stall + 12 个采样周期（生产 800ms 参数
 * 的最坏链 + 余量）内 Leadership 因让位离开原主；③让位后集群仍健康有主
 * （让位走真通道、非瘫痪）。
 *
 * <p><b>归属</b>：非 {@code @Tag("drill")}——确定性、秒级预算（~30s），随缺省
 * failsafe 门禁运行（详 {@code SmokeIT} 同纪律）。
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ReplicationStallWatchdogIT {

    /** 生产折算的停摆阈值（election-timeout 800ms → max(10s, 4s)）。 */
    private static final long T_STALL_MS = 10_000;
    /** 采样周期（与 election-timeout 同值，生产口径）。 */
    private static final long SAMPLE_MS = 800;

    @Test
    void productionWatchdogTransfersRealFrozenLeader() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3, 800)) {
            ClusterHarness.Node leader = h.leader();
            assertThat(leader).as("初始选主").isNotNull();

            // 冻结构造：探针关，流量不进——Leader 稳定在任但 commit 永不推进。
            h.setProbesEnabled(false);

            long t0 = System.currentTimeMillis();
            // ① 阈值内不误伤：至少 4×T_stall 之前…取半窗防调度边缘：T_stall/2 内仍是原主。
            Thread.sleep(T_STALL_MS / 2);
            assertThat(h.leader()).as("T_stall/2 内不得提前让位").isSameAs(leader);

            // ② 最迟 T_stall + 12 采样内让位发生（elapsed 门 + M 连击 + 调度余量）。
            long deadline = t0 + T_STALL_MS + 12 * SAMPLE_MS;
            boolean moved = false;
            while (System.currentTimeMillis() < deadline) {
                if (!leader.isLeader()) {
                    moved = true;
                    break;
                }
                Thread.sleep(200);
            }
            assertThat(moved)
                    .as("真实冻结态在 ≤T_stall+ε 内触发生产让位（探针/动作组真通道）")
                    .isTrue();

            // ③ 让位后集群健康：仍有 Leader（且不是原主立刻自反接）。
            h.awaitTrue(() -> {
                ClusterHarness.Node l = h.leader();
                return l != null && l != leader && l.isLeader();
            }, 20_000, "让位后新主就绪");
        }
    }
}
