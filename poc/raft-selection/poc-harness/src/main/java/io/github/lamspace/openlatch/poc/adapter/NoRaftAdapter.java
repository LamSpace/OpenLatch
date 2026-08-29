package io.github.lamspace.openlatch.poc.adapter;

import io.github.lamspace.openlatch.poc.harness.AdapterContext;
import io.github.lamspace.openlatch.poc.harness.LockStateMachineCore;
import io.github.lamspace.openlatch.poc.harness.PocNodeAdapter;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.ApplyResult;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.RaftLogEntry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

/**
 * no-raft 伪节点（design D4）：SPI 直通 {@link LockStateMachineCore#applyEntry}，
 * 不经任何 Raft 库。与候选完全同构的测量路径（行协议 + apply + 回执），
 * 差值即"复制开销"的归因基准；同时承担 P2-01 骨架自测。
 */
public final class NoRaftAdapter implements PocNodeAdapter {

    private AdapterContext ctx;
    private LockStateMachineCore core;
    private Path snapFile;

    @Override
    public void start(AdapterContext ctx, LockStateMachineCore core) throws IOException {
        this.ctx = ctx;
        this.core = core;
        this.snapFile = Path.of(ctx.dataDir(), "poc-snapshot.dat");
        if (Files.exists(snapFile)) {
            core.installSnapshot(Files.readAllBytes(snapFile));
        }
    }

    @Override
    public void stop() {
    }

    @Override
    public CompletableFuture<ApplyResult> propose(RaftLogEntry entry) {
        try {
            byte[] res = core.applyEntry(entry.toByteArray());
            return CompletableFuture.completedFuture(ApplyResult.parseFrom(res));
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public boolean isLeader() {
        return true;
    }

    @Override
    public int leaderNodeId() {
        return ctx.nodeId();
    }

    @Override
    public long term() {
        return 1;
    }

    @Override
    public void triggerSnapshot() throws IOException {
        Files.createDirectories(snapFile.getParent());
        Files.write(snapFile, core.snapshotBundle(1, core.shadow().lockCount()).shadowBytes());
    }

    @Override
    public long lastSnapshotBytes() {
        try {
            return Files.exists(snapFile) ? Files.size(snapFile) : -1;
        } catch (IOException e) {
            return -1;
        }
    }

    @Override
    public String name() {
        return "noraft";
    }
}
