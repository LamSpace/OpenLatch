package io.github.lamspace.openlatch.poc.ratis;

import io.github.lamspace.openlatch.poc.harness.AdapterContext;
import io.github.lamspace.openlatch.poc.harness.LockStateMachineCore;
import io.github.lamspace.openlatch.poc.harness.PocNodeAdapter;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.ApplyResult;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.RaftLogEntry;
import io.github.lamspace.openlatch.poc.proto.RaftPoc.SnapshotFile;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Apache Ratis 候选适配器（P2-02）：gRPC 传输（D5）、单组、
 * SimpleStateMachineStorage 挂影子表快照；条目语义全部委托共享
 * {@link LockStateMachineCore}。
 */
public final class RatisNodeAdapter implements PocNodeAdapter {

    private static final UUID GROUP_UUID =
            UUID.nameUUIDFromBytes("openlatch-poc-group".getBytes(StandardCharsets.UTF_8));

    private AdapterContext ctx;
    private RaftServer server;
    private RaftClient client;
    private RaftGroupId groupId;
    private volatile PocStateMachine sm;

    @Override
    public void start(AdapterContext ctx, LockStateMachineCore core) throws IOException {
        this.ctx = ctx;
        this.groupId = RaftGroupId.valueOf(GROUP_UUID);

        RaftProperties props = new RaftProperties();
        GrpcConfigKeys.Server.setPort(props, ctx.raftPort());
        RaftServerConfigKeys.setStorageDir(props, List.of(new File(ctx.dataDir())));
        if (ctx.electionTimeoutMs() > 0) {
            RaftServerConfigKeys.Rpc.setTimeoutMin(props,
                    TimeDuration.valueOf(ctx.electionTimeoutMs() / 2, TimeUnit.MILLISECONDS));
            RaftServerConfigKeys.Rpc.setTimeoutMax(props,
                    TimeDuration.valueOf(ctx.electionTimeoutMs(), TimeUnit.MILLISECONDS));
        }
        if (ctx.snapshotThreshold() > 0) {
            RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(props, true);
            RaftServerConfigKeys.Snapshot.setAutoTriggerThreshold(props, ctx.snapshotThreshold());
        }

        List<RaftPeer> peers = new ArrayList<>();
        for (var e : ctx.peers().entrySet()) {
            peers.add(RaftPeer.newBuilder()
                    .setId(RaftPeerId.valueOf("n" + e.getKey()))
                    .setAddress(e.getValue())
                    .build());
        }
        RaftGroup group = RaftGroup.valueOf(groupId, peers);
        RaftPeerId self = RaftPeerId.valueOf("n" + ctx.nodeId());

        // 摩擦日志：单组 setGroup 默认 StartupOption=FORMAT，重启目录非空即抛
        // "Failed to FORMAT"；需按存储是否已存在选 RECOVER/CREATE。
        // RaftGroupId.toString() 带 "#" 前缀，落盘目录是裸 UUID
        boolean storageExists = Files.exists(Path.of(ctx.dataDir(), groupId.getUuid().toString()));
        server = RaftServer.newBuilder()
                .setServerId(self)
                .setGroup(group)
                .setProperties(props)
                .setOption(storageExists
                        ? org.apache.ratis.server.storage.RaftStorage.StartupOption.RECOVER
                        : org.apache.ratis.server.storage.RaftStorage.StartupOption.FORMAT)
                .setStateMachineRegistry(pid -> {
                    sm = new PocStateMachine(core);
                    return sm;
                })
                .build();
        server.start();

        client = RaftClient.newBuilder()
                .setProperties(new RaftProperties())
                .setRaftGroup(group)
                .build();
        // 摩擦日志：leader 对同一 ClientId 的在途请求串行排队（单 client 吞吐 ~500/s），
        // 批量追赶需多客户端池才能测出库真实复制带宽。
        clients = new RaftClient[8];
        for (int i = 0; i < clients.length; i++) {
            clients[i] = RaftClient.newBuilder()
                    .setProperties(new RaftProperties())
                    .setRaftGroup(group)
                    .build();
        }
    }

    private final java.util.concurrent.atomic.AtomicInteger clientIdx =
            new java.util.concurrent.atomic.AtomicInteger();
    private RaftClient[] clients;

    @Override
    public void stop() {
        try {
            if (client != null) {
                client.close();
            }
            if (clients != null) {
                for (RaftClient c : clients) {
                    c.close();
                }
            }
        } catch (Exception ignored) {
            // best effort
        }
        try {
            if (server != null) {
                server.close();
            }
        } catch (Exception ignored) {
            // best effort
        }
    }

    private DivisionInfo info() throws IOException {
        return server.getDivision(groupId).getInfo();
    }

    @Override
    public CompletableFuture<ApplyResult> propose(RaftLogEntry entry) {
        if (!isLeader()) {
            return CompletableFuture.failedFuture(new NotLeaderException(leaderNodeId()));
        }
        // 摩擦日志：io() 阻塞式回执与 JRaft 异步链口径不对等，改走 async()
        RaftClient c = clients[Math.floorMod(clientIdx.getAndIncrement(), clients.length)];
        return c.async().send(Message.valueOf(ByteString.copyFrom(entry.toByteArray())))
                .thenApply(reply -> {
                    try {
                        if (!reply.isSuccess()) {
                            String msg = String.valueOf(reply.getException());
                            if (msg.contains("NOT_LEADER")) {
                                throw new java.util.concurrent.CompletionException(
                                        new NotLeaderException(leaderNodeId()));
                            }
                            throw new java.util.concurrent.CompletionException(
                                    new IOException("ratis reply failed: " + msg));
                        }
                        return ApplyResult.parseFrom(reply.getMessage().getContent().toByteArray());
                    } catch (IOException ioe) {
                        throw new java.util.concurrent.CompletionException(ioe);
                    }
                });
    }

    @Override
    public boolean isLeader() {
        try {
            return info().isLeader();
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public int leaderNodeId() {
        try {
            RaftPeerId lid = info().getLeaderId();
            return lid == null ? -1 : Integer.parseInt(lid.toString().substring(1));
        } catch (Exception e) {
            return -1;
        }
    }

    @Override
    public long term() {
        try {
            var smx = sm;
            return smx == null ? -1 : smx.currentTerm();
        } catch (Exception e) {
            return -1;
        }
    }

    @Override
    public void triggerSnapshot() throws IOException {
        if (isLeader()) {
            client.getSnapshotManagementApi().create(30_000);
        } else {
            // Follower 本地触发（driver 的"各节点快照"路径）：直调状态机 takeSnapshot
            ((PocStateMachine) server.getDivision(groupId).getStateMachine()).takeSnapshot();
        }
    }

    @Override
    public long lastSnapshotBytes() {
        PocStateMachine s = sm;
        return s == null ? -1 : s.lastSnapshotBytes();
    }

    @Override
    public String name() {
        return "ratis";
    }

    /**
     * Ratis 状态机：搬运条目与快照文件，语义委托共享内核。
     * 回放截断经 {@link #getLatestSnapshot()} 交给 Ratis 的 ServerState 处理。
     */
    static final class PocStateMachine extends BaseStateMachine {

        private final LockStateMachineCore core;
        private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

        PocStateMachine(LockStateMachineCore core) {
            this.core = core;
        }

        @Override
        public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage)
                throws IOException {
            super.initialize(server, groupId, raftStorage);
            storage.init(raftStorage);
            SingleFileSnapshotInfo snap = storage.loadLatestSnapshot();
            if (snap != null) {
                byte[] bytes = Files.readAllBytes(snap.getFile().getPath());
                SnapshotFile sf = SnapshotFile.parseFrom(bytes);
                core.installSnapshot(sf.getShadow().toByteArray());
                notifyTermIndexUpdated(sf.getTerm(), sf.getIndex());
            }
        }

        @Override
        public SingleFileSnapshotInfo getLatestSnapshot() {
            return storage.getLatestSnapshot();
        }

        @Override
        public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
            var entry = trx.getLogEntry();
            CompletableFuture<Message> f = new CompletableFuture<>();
            try {
                notifyTermIndexUpdated(entry.getTerm(), entry.getIndex());
                if (entry.hasStateMachineLogEntry()) {
                    byte[] res = core.applyEntry(
                            entry.getStateMachineLogEntry().getLogData().toByteArray());
                    f.complete(Message.valueOf(ByteString.copyFrom(res)));
                } else {
                    f.complete(Message.valueOf(ByteString.EMPTY));
                }
            } catch (Exception e) {
                f.completeExceptionally(e);
            }
            return f;
        }

        @Override
        public long takeSnapshot() throws IOException {
            var ti = getLastAppliedTermIndex();
            long term = ti == null ? 1 : ti.getTerm();
            long index = ti == null ? 0 : ti.getIndex();
            SnapshotFile sf = SnapshotFile.newBuilder()
                    .setTerm(term).setIndex(index)
                    .setShadow(com.google.protobuf.ByteString
                            .copyFrom(core.snapshotBundle(term, index).shadowBytes()))
                    .build();
            File dst = storage.getSnapshotFile(term, index);
            File tmp = new File(dst.getParentFile(), dst.getName() + ".tmp");
            if (tmp.getParentFile() != null) {
                Files.createDirectories(tmp.getParentFile().toPath());
            }
            Files.write(tmp.toPath(), sf.toByteArray());
            Files.move(tmp.toPath(), dst.toPath(), StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            storage.updateLatestSnapshot(new SingleFileSnapshotInfo(
                    new FileInfo(dst.toPath(), MD5Hash.newInstance(new byte[MD5Hash.MD5_LENGTH])), term, index));
            return index;
        }

        long currentTerm() {
            var ti = getLastAppliedTermIndex();
            return ti == null ? -1 : ti.getTerm();
        }

        long lastSnapshotBytes() {
            SingleFileSnapshotInfo info = storage.getLatestSnapshot();
            if (info == null) {
                return -1;
            }
            Path p = info.getFile().getPath();
            try {
                return Files.exists(p) ? Files.size(p) : -1;
            } catch (IOException e) {
                return -1;
            }
        }
    }
}
