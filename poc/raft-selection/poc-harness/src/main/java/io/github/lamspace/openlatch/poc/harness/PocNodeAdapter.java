package io.github.lamspace.openlatch.poc.harness;

import io.github.lamspace.openlatch.poc.proto.RaftPoc.ApplyResult;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.RaftLogEntry;

import java.util.concurrent.CompletableFuture;

/**
 * 候选库接入 SPI（P2-01/P2-02/P2-03 的唯一差异点）。
 *
 * <p>适配器只负责"库生命周期 + 条目搬运 + 角色视图 + 快照触发"；
 * 条目 → {@code CoreEngine} 的应用语义全部在共享的 {@link LockStateMachineCore} 中，
 * 保证两候选的功能面与测量点严格同一。
 */
public interface PocNodeAdapter {

    /** 启动 Raft 库节点并完成组网。 */
    void start(AdapterContext ctx, LockStateMachineCore core) throws Exception;

    /** 关停库节点（仅优雅路径；杀主演练由 driver 直接 destroyForcibly）。 */
    void stop();

    /**
     * Leader 侧提案：条目经多数派复制并应用后完成。
     * 非 Leader 调用 MUST 以 {@link NotLeaderException} 异常完成。
     */
    CompletableFuture<ApplyResult> propose(RaftLogEntry entry);

    /** 本节点当前是否为 Leader。 */
    boolean isLeader();

    /** 当前 Leader 的 nodeId；未知返回 -1。 */
    int leaderNodeId();

    /** 当前 term；库不暴露时返回 -1（诊断用）。 */
    long term();

    /** 手动触发一次快照（详设 §2.3 第 4 项）。 */
    void triggerSnapshot() throws Exception;

    /** 已持久化快照文件字节数；无快照返回 -1。 */
    long lastSnapshotBytes();

    /** 行协议名（ratis / jraft / noraft），用于报告标注。 */
    String name();

    /** 非 Leader 提案拒绝。 */
    final class NotLeaderException extends RuntimeException {
        /** 当前已知 Leader 的 nodeId（-1 未知）。 */
        public final int leaderNodeId;

        public NotLeaderException(int leaderNodeId) {
            super("not leader; leader=" + leaderNodeId);
            this.leaderNodeId = leaderNodeId;
        }
    }
}
