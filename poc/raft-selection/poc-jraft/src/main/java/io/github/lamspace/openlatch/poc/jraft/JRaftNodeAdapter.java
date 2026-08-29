package io.github.lamspace.openlatch.poc.jraft;

import com.alipay.sofa.jraft.Closure;
import com.alipay.sofa.jraft.Iterator;
import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.RaftGroupService;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.core.StateMachineAdapter;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.entity.Task;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotReader;
import com.alipay.sofa.jraft.storage.snapshot.SnapshotWriter;
import io.github.lamspace.openlatch.poc.harness.AdapterContext;
import io.github.lamspace.openlatch.poc.harness.LockStateMachineCore;
import io.github.lamspace.openlatch.poc.harness.PocNodeAdapter;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.ApplyResult;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.RaftLogEntry;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.ResultStatus;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.SnapshotFile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * SOFAJRaft 候选适配器（P2-03）：bolt/jrpc 传输（D5）、静态 initial conf 组网、
 * onSnapshotSave/Load 挂影子表快照；条目语义全部委托共享 {@link LockStateMachineCore}。
 */
public final class JRaftNodeAdapter implements PocNodeAdapter {

    private static final String GROUP_ID = "openlatch-poc-group";
    private static final String SNAP_FILE = "lock.snapshot";

    private AdapterContext ctx;
    private LockStateMachineCore core;
    private RaftGroupService service;
    private Node node;
    private volatile long leaderTerm = -1;

    @Override
    public void start(AdapterContext ctx, LockStateMachineCore core) throws Exception {
        this.ctx = ctx;
        this.core = core;

        String raftAddr = ctx.peers().get(ctx.nodeId());
        int colon = raftAddr.lastIndexOf(':');
        PeerId serverId = new PeerId(raftAddr.substring(0, colon),
                Integer.parseInt(raftAddr.substring(colon + 1)));

        List<PeerId> peerIds = new ArrayList<>();
        for (var addr : ctx.peers().values()) {
            int c = addr.lastIndexOf(':');
            peerIds.add(new PeerId(addr.substring(0, c), Integer.parseInt(addr.substring(c + 1))));
        }
        Configuration conf = new Configuration(peerIds);

        Path data = Path.of(ctx.dataDir());
        NodeOptions opts = new NodeOptions();
        opts.setFsm(new PocFSM());
        opts.setInitialConf(conf);
        if (ctx.electionTimeoutMs() > 0) {
            opts.setElectionTimeoutMs((int) ctx.electionTimeoutMs());
        }
        // 摩擦日志：1.4.1 的 RocksDBLogStorage 把 file:// 前缀当字面路径 mkdir，裸绝对路径才通
        opts.setRaftMetaUri(data.resolve("meta").toAbsolutePath().toString());
        opts.setLogUri(data.resolve("log").toAbsolutePath().toString());
        opts.setSnapshotUri(data.resolve("snapshot").toAbsolutePath().toString());
        opts.setSnapshotTempUri(data.resolve("snapshot_tmp").toAbsolutePath().toString());

        service = new RaftGroupService(GROUP_ID, serverId, opts);
        node = service.start();
    }

    @Override
    public void stop() {
        try {
            if (node != null) {
                node.shutdown(null);
                node.join();
            }
        } catch (Exception ignored) {
            // best effort
        }
    }

    @Override
    public CompletableFuture<ApplyResult> propose(RaftLogEntry entry) {
        if (!isLeader()) {
            return CompletableFuture.failedFuture(new NotLeaderException(leaderNodeId()));
        }
        CompletableFuture<ApplyResult> f = new CompletableFuture<>();
        Task task = new Task();
        task.setData(ByteBuffer.wrap(entry.toByteArray()));
        task.setDone(new ProposeClosure(f));
        node.apply(task);
        return f;
    }

    @Override
    public boolean isLeader() {
        Node n = node;
        return n != null && n.isLeader();
    }

    @Override
    public int leaderNodeId() {
        Node n = node;
        if (n == null) {
            return -1;
        }
        PeerId lid = n.getLeaderId();
        if (lid == null || lid.isEmpty()) {
            return -1;
        }
        String s = lid.toString();
        for (var e : ctx.peers().entrySet()) {
            if (e.getValue().equals(s)) {
                return e.getKey();
            }
        }
        return -1;
    }

    @Override
    public long term() {
        return leaderTerm;
    }

    @Override
    public void triggerSnapshot() throws Exception {
        // 摩擦日志：Node#snapshotSync 仅允许在 StateMachine 回调线程内调用；
        // 外部手动触发必须走 CliService（异步 RPC，等待落盘文件出现）。
        String raftAddr = ctx.peers().get(ctx.nodeId());
        int c = raftAddr.lastIndexOf(':');
        PeerId self = new PeerId(raftAddr.substring(0, c), Integer.parseInt(raftAddr.substring(c + 1)));
        if (cli == null) {
            cli = new com.alipay.sofa.jraft.core.CliServiceImpl();
            if (!((com.alipay.sofa.jraft.CliService) cli).init(new com.alipay.sofa.jraft.option.CliOptions())) {
                throw new IOException("CliService init failed");
            }
        }
        long before = lastSnapshotBytes();
        Status st = cli.snapshot(GROUP_ID, self);
        if (st == null || !st.isOk()) {
            throw new IOException("snapshot request failed: " + st);
        }
        long deadline = System.currentTimeMillis() + 120_000;
        while (System.currentTimeMillis() < deadline) {
            if (lastSnapshotBytes() > before || lastSnapshotBytes() > 0) {
                return;
            }
            Thread.sleep(50);
        }
        throw new IOException("snapshot file not found within 120s");
    }

    private com.alipay.sofa.jraft.CliService cli;

    @Override
    public long lastSnapshotBytes() {
        Path dir = Path.of(ctx.dataDir(), "snapshot");
        long best = -1;
        try (var walk = Files.walk(dir, 4)) {
            for (Path p : walk.filter(f -> f.getFileName().toString().equals(SNAP_FILE)).toList()) {
                best = Math.max(best, Files.size(p));
            }
        } catch (IOException ignored) {
            // no snapshot yet
        }
        return best;
    }

    @Override
    public String name() {
        return "jraft";
    }

    /** 提案回执闭包：apply 线程写入结果后完成 future。 */
    static final class ProposeClosure implements Closure {
        private final CompletableFuture<ApplyResult> future;
        private byte[] result;

        ProposeClosure(CompletableFuture<ApplyResult> future) {
            this.future = future;
        }

        void setResult(byte[] res) {
            this.result = res;
        }

        @Override
        public void run(Status status) {
            if (status != null && !status.isOk()) {
                future.completeExceptionally(new IOException("apply status: " + status));
            } else if (result != null) {
                try {
                    future.complete(ApplyResult.parseFrom(result));
                } catch (Exception e) {
                    future.completeExceptionally(e);
                }
            } else {
                future.completeExceptionally(new IOException("apply produced no result"));
            }
        }
    }

    /** JRaft 状态机：onApply 搬运条目，onSnapshotSave/Load 读写影子表。 */
    final class PocFSM extends StateMachineAdapter {

        private final AtomicLong lastAppliedIndex = new AtomicLong(-1);

        @Override
        public void onLeaderStart(long term) {
            leaderTerm = term;
        }

        @Override
        public void onLeaderStop(Status status) {
            leaderTerm = -1;
        }

        @Override
        public void onApply(Iterator it) {
            while (it.hasNext()) {
                try {
                    ByteBuffer buf = it.getData();
                    byte[] res;
                    if (buf == null || buf.remaining() == 0) {
                        res = ApplyResult.newBuilder().setStatus(ResultStatus.R_OK)
                                .build().toByteArray();
                    } else {
                        byte[] bytes = new byte[buf.remaining()];
                        buf.duplicate().get(bytes);
                        res = core.applyEntry(bytes);
                    }
                    lastAppliedIndex.set(it.getIndex());
                    Closure done = it.done();
                    if (done instanceof ProposeClosure pc) {
                        pc.setResult(res);
                    }
                    if (done != null) {
                        done.run(new Status());
                    }
                } catch (Exception e) {
                    Closure done = it.done();
                    if (done != null) {
                        done.run(new Status(-1, e.getClass().getSimpleName() + ":" + e.getMessage()));
                    }
                } finally {
                    it.next();
                }
            }
            it.commit();
        }

        @Override
        public void onSnapshotSave(SnapshotWriter writer, Closure done) {
            try {
                long term = Math.max(1, leaderTerm);
                long index = lastAppliedIndex.get();
                SnapshotFile sf = SnapshotFile.newBuilder()
                        .setTerm(term).setIndex(index)
                        .setShadow(com.google.protobuf.ByteString
                                .copyFrom(core.snapshotBundle(term, index).shadowBytes()))
                        .build();
                File f = Paths.get(writer.getPath(), SNAP_FILE).toFile();
                Files.write(f.toPath(), sf.toByteArray());
                if (writer.addFile(SNAP_FILE)) {
                    done.run(new Status());
                } else {
                    done.run(new Status(-1, "jraft addFile rejected"));
                }
            } catch (Exception e) {
                done.run(new Status(-1, "snapshot save failed: " + e));
            }
        }

        @Override
        public boolean onSnapshotLoad(SnapshotReader reader) {
            try {
                File f = Paths.get(reader.getPath(), SNAP_FILE).toFile();
                if (!f.exists()) {
                    return false;
                }
                SnapshotFile sf = SnapshotFile.parseFrom(Files.readAllBytes(f.toPath()));
                core.installSnapshot(sf.getShadow().toByteArray());
                lastAppliedIndex.set(sf.getIndex());
                return true;
            } catch (Exception e) {
                return false;
            }
        }
    }
}
