/*
 * Copyright 2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.github.lamspace.openlatch.server.raft;

import com.google.protobuf.InvalidProtocolBufferException;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.ApplyStatus;
import io.github.lamspace.openlatch.protocol.raft.RaftLogEntry;
import io.github.lamspace.openlatch.protocol.raft.SnapshotState;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.MD5FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntConsumer;

/**
 * Ratis 状态机适配器（详设 §3.2 {@code LockStateMachine}）：把 Ratis 的
 * 应用与 Leadership 事件翻译到 {@link LockStateMachineCore} 与
 * {@link ApplyObserver}，自身不含任何锁语义。
 *
 * <p><b>契约要点</b>（design D10）：Ratis 仅应用<b>多数派已提交</b>的条目
 * （{@code StateMachineUpdater} 以 {@code applied < committedIndex} 推进，
 * Leader 与 Follower 同线程同规则），因此：①客户端在应用回执后应答即天然
 * "提交后应答"；②被截断（未提交）的条目从未进入过本状态机，降级/回滚
 * 无需任何补偿；③应用线程单线程串行，{@link EntryClock} 的 thread-local
 * 条目时刻契约成立。
 *
 * <p><b>快照通道（S4，§7/P2-15~16，design D3/D4/D5）</b>：{@link #takeSnapshot()}
 * 在应用线程（Ratis {@code StateMachineUpdater}，与 apply 同线程）产出
 * applyLock 内的一致性副本（{@link LockStateMachineCore#snapshotState()}），
 * <b>锁外</b>写盘（tmp→原子 rename→MD5 伴随文件）后交还位点——库侧据此
 * 截断日志并按保留数清理旧快照（保留 2 份由装配层配置）。「异步落盘」的
 * 口径即"applyLock 外落盘"：完全异步（先返位点后落盘）会造成截断先于
 * 持久化的崩溃窗口，被 design D4 显式否决；写盘期间 updater 线程的短暂
 * 停为既定代价，耗时由 §10 快照基准度量。安装侧走 Ratis 3.3 的
 * pause→库发布快照文件→{@code StateMachineUpdater.reload}：{@link #pause()}
 * 仅做生命周期迁移（PAUSING→PAUSED），{@link #reinitialize()} 重扫目录取
 * 最新快照文件、经 {@link LockStateMachineCore#installSnapshot} 整体替换
 * 状态并把位点钉到快照位点；启动加载走同一 {@code loadSnapshot} 通道
 * （{@link #initialize}）。
 *
 * <p><b>线程模型</b>：{@link #applyTransaction} 与 {@link #notifyLeaderChanged}
 * 由 Ratis 内部线程调用；对观察者的转发保持同步、无阻塞（观察者契约要求，
 * 见 {@link ApplyObserver}）。{@code observer} 为构造注入后的 volatile 悬挂点，
 * 装配期允许先建状态机后接观察者（{@code ReplicationGateway} 晚于子系统创建）。
 */
public final class LockStateMachine extends BaseStateMachine {

    /** 日志器：条目解析失败、应用异常回执与快照生成/加载的生命周期事件。 */
    private static final Logger log = LoggerFactory.getLogger(LockStateMachine.class);

    /** 语义内核（条目 → 引擎迁移）。 */
    private final LockStateMachineCore core;
    /**
     * Ratis 原生单文件快照存储（snapshot.T_I 命名、latest 引用与保留清理
     * 由库负责）：安装流中库侧把 Leader 下发的快照块直接落到本存储目录，
     * 本状态机经同一实例读写——不自管磁盘（design D3）。
     */
    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    /** 应用/Leadership 观察者，装配期可替换（volatile：应用线程读、装配线程写）。 */
    private volatile ApplyObserver observer;
    /** 本节点 Raft 成员 id，用于把 Leader 变更事件折算成本节点角色。 */
    private volatile RaftPeerId selfId;
    /**
     * Leader 身份监听器（{@link LeaderTracker} 的挂点）：每次 Leadership
     * 变更回调携带<b>新 Leader 的 nodeId</b>（选举中无 Leader 时为 -1），
     * 不再仅折算本节点布尔角色（volatile：事件线程读、装配线程写）。
     */
    private volatile IntConsumer leaderIdentityListener;

    /**
     * 构造状态机。
     *
     * @param core 语义内核；null 抛 {@link NullPointerException}
     */
    public LockStateMachine(LockStateMachineCore core) {
        this.core = java.util.Objects.requireNonNull(core);
    }

    /**
     * 装配观察者（{@code ReplicationGateway} 构造后回挂）。
     *
     * @param observer 观察者，null 表示摘除
     */
    public void setObserver(ApplyObserver observer) {
        this.observer = observer;
    }

    /**
     * Ratis 初始化：登记 server/group 与快照存储，随后加载本地最新快照
     * （§7.3-1：重建锁状态后由库从快照位点起重放日志）。生命周期经
     * {@code startAndTransition} 推进（NEW→STARTING→RUNNING）——S4 起
     * {@link #pause()} 依赖 RUNNING 态的合法性（reload 断言 PAUSED）。
     *
     * @param server      承载本状态机的 Raft 服务
     * @param groupId     组 id
     * @param raftStorage 存储
     * @throws IOException 基类初始化失败或快照文件损坏/不可解析（拒绝启动）
     */
    @Override
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
        getLifeCycle().startAndTransition(() -> {
            super.initialize(server, groupId, raftStorage);
            this.selfId = server.getId();
            storage.init(raftStorage);
            loadSnapshot(storage.loadLatestSnapshot());
        });
    }

    /**
     * 应用一条已提交条目（由 Ratis 应用线程串行调用）。
     *
     * <p>流程：位点推进 → 解析条目（解析失败按 {@link ApplyStatus#INTERNAL_ERROR}
     * 回执，MUST NOT 抛出——抛出会令 Ratis 关停副本）→ 内核应用 → 观察者通知
     * → 回执序列化进 {@link Message}。非状态机日志（元数据/配置条目）按空回执处理。
     *
     * @param trx 事务上下文（已提交条目）
     * @return 完成于应用回执的 future
     */
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        var entryProto = trx.getLogEntry();
        updateLastAppliedTermIndex(entryProto.getTerm(), entryProto.getIndex());
        ApplyResult result;
        RaftLogEntry entry = null;
        try {
            if (!entryProto.hasStateMachineLogEntry()) {
                result = ApplyResult.newBuilder().setStatus(ApplyStatus.OK).build();
            } else {
                byte[] bytes = entryProto.getStateMachineLogEntry().getLogData().toByteArray();
                entry = RaftLogEntry.parseFrom(bytes);
                result = core.apply(entry);
            }
        } catch (InvalidProtocolBufferException e) {
            log.error("undecodable committed entry at term={} index={}",
                    entryProto.getTerm(), entryProto.getIndex(), e);
            result = ApplyResult.newBuilder().setStatus(ApplyStatus.INTERNAL_ERROR).build();
        } catch (RuntimeException e) {
            log.error("apply threw at term={} index={}", entryProto.getTerm(), entryProto.getIndex(), e);
            result = ApplyResult.newBuilder().setStatus(ApplyStatus.INTERNAL_ERROR).build();
        }
        ApplyObserver obs = observer;
        if (obs != null && entry != null) {
            try {
                obs.onApplied(entry, result);
            } catch (RuntimeException e) {
                log.error("apply observer failure (seq={})", entry.getSeq(), e);
            }
        }
        return CompletableFuture.completedFuture(
                Message.valueOf(ByteString.copyFrom(result.toByteArray())));
    }

    /**
     * Leader 变更事件：折算为本节点角色后转发观察者（在途回执收尾与
     * 任期队列清理的触发源，§4.4/§8），并把新 Leader 身份投递
     * {@link #setLeaderIdentityListener 领导身份监听器}（s3 design D3 的
     * {@link LeaderTracker} 数据源）：成员 id "n&lt;nodeId&gt;" 折算为数值
     * nodeId，选举中无 Leader（或无法解析）以 -1 表达。两个转发都同步、
     * 不阻塞（观察者契约）。
     *
     * @param groupMemberId 组成员标识
     * @param newLeaderId   新 Leader 的 peer id，{@code null} 表示暂无 Leader
     */
    @Override
    public void notifyLeaderChanged(org.apache.ratis.protocol.RaftGroupMemberId groupMemberId,
                                    RaftPeerId newLeaderId) {
        super.notifyLeaderChanged(groupMemberId, newLeaderId);
        ApplyObserver obs = observer;
        RaftPeerId self = selfId;
        if (obs != null && self != null) {
            obs.onLeaderChanged(newLeaderId != null && newLeaderId.equals(self));
        }
        IntConsumer listener = leaderIdentityListener;
        if (listener != null) {
            listener.accept(parseNodeId(newLeaderId));
        }
    }

    /**
     * 装配领导身份监听器（{@link LeaderTracker} 挂点，s3 design D3）。
     * 须在 {@code RaftServer} 启动前注册；重复调用以最后一次为准。
     *
     * @param listener 监听器，{@code null} 表示摘除
     */
    public void setLeaderIdentityListener(IntConsumer listener) {
        this.leaderIdentityListener = listener;
    }

    /**
     * 把 Ratis 成员 id（约定 "n&lt;nodeId&gt;"）折算为数值 nodeId。
     *
     * @param peerId Leader 成员 id，可为 {@code null}
     * @return 数值 nodeId；无 Leader 或形态不识别时为 -1（提示降级为未知）
     */
    private static int parseNodeId(RaftPeerId peerId) {
        if (peerId == null) {
            return -1;
        }
        String s = peerId.toString();
        try {
            return Integer.parseInt(s.substring(1));
        } catch (IndexOutOfBoundsException | NumberFormatException e) {
            return -1;
        }
    }

    /**
     * 生成并落盘一份快照（详设 §7.2，S4/P2-15）。由 Ratis 应用线程在
     * 未快照位点差越过阈值时调用（亦经 {@code RaftSubsystem.triggerSnapshot()}
     * 的运维/测试通道，见其 Javadoc 的线程注记）。
     *
     * <p><b>流程与语义</b>（design D4）：取已应用位点 → applyLock 内
     * {@link LockStateMachineCore#snapshotState()} 一致性副本（含发号水位）→
     * 放锁 → 写临时文件、原子 rename 到库命名 {@code snapshot.T_I}、
     * 计算并存储 MD5 伴随文件（安装端校验依赖）→ 更新库侧 latest 引用。
     * 落盘失败以异常上抛（库侧记 WARN 并留待下轮阈值触发，位点不推进、
     * 日志不截断，无半快照风险）。位点无效（term/index 非正，如从未应用）
     * 返回 {@code -1} 声明"本轮无快照"。
     *
     * <p><b>并发</b>：{@code synchronized} 于本状态机实例，与 {@link #pause()}
     * /{@link #reinitialize()} 互斥；一致性副本与条目应用经内核
     * {@code applyLock} 串行（本方法在应用线程调用时内核无争用）。
     *
     * @return 快照位点（已应用索引）；无可快照位点时 {@code -1}
     * @throws IOException 快照文件写盘/MD5 失败
     */
    @Override
    public synchronized long takeSnapshot() throws IOException {
        final TermIndex ti = getLastAppliedTermIndex();
        if (ti == null || ti.getTerm() <= 0 || ti.getIndex() <= 0) {
            return -1L;
        }
        // 锁内一致性副本（applyLock 内序列化，design D4）——此后副本不可变，
        // 状态照常演化，演化由快照位点之后的日志承载。
        final SnapshotState state = core.snapshotState();
        final File dst = storage.getSnapshotFile(ti.getTerm(), ti.getIndex());
        final File tmp = new File(dst.getParentFile(), dst.getName() + ".tmp");
        try {
            Files.write(tmp.toPath(), state.toByteArray());
            Files.move(tmp.toPath(), dst.toPath(),
                    StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            MD5Hash digest = MD5FileUtil.computeAndSaveMd5ForFile(dst);
            storage.updateLatestSnapshot(
                    new SingleFileSnapshotInfo(new FileInfo(dst.toPath(), digest), ti));
        } catch (IOException e) {
            if (!tmp.delete()) {
                log.debug("tmp snapshot cleanup failed: {}", tmp);
            }
            throw e;
        }
        log.info("took snapshot at {} ({} lock entries, {} bytes)", ti,
                state.getLocksCount(), state.getSerializedSize());
        return ti.getIndex();
    }

    /**
     * 快照安装暂停（Ratis 3.3 安装流第一步，design D5）：库侧写完快照块后、
     * 原子发布前调用本方法，仅做生命周期迁移（RUNNING→PAUSING→PAUSED）；
     * 应用位点的重置在 {@link #reinitialize()}。
     */
    @Override
    public synchronized void pause() {
        getLifeCycle().transition(LifeCycle.State.PAUSING);
        getLifeCycle().transition(LifeCycle.State.PAUSED);
    }

    /**
     * 快照安装重启（安装流末步，由 {@code StateMachineUpdater.reload} 在
     * PAUSED 态调用）：重扫存储目录取库刚发布的最新快照（安装路径的发布
     * 不经本实例引用更新，必须盘上重扫），经
     * {@link LockStateMachineCore#installSnapshot} 整体替换状态、位点钉到
     * 快照位点，生命周期回到 RUNNING——此后增量回放从快照位点续起。
     *
     * @throws IOException 快照文件损坏/不可解析（上抛由库侧重试安装）
     */
    @Override
    public synchronized void reinitialize() throws IOException {
        loadSnapshot(storage.loadLatestSnapshot());
        if (getLifeCycleState() == LifeCycle.State.PAUSED) {
            getLifeCycle().transition(LifeCycle.State.STARTING);
            getLifeCycle().transition(LifeCycle.State.RUNNING);
        }
    }

    /**
     * 暴露本状态机的单文件快照存储：安装流的落盘/发布与读侧 latest 查询
     * 共用此实例（Ratis 3.3 中库经该接口与状态机交换快照文件）。
     *
     * @return 本状态机的 {@link SimpleStateMachineStorage}
     */
    @Override
    public StateMachineStorage getStateMachineStorage() {
        return storage;
    }

    /**
     * 加载一份快照文件到内核（启动加载与安装重启共用通道）：读尽文件、
     * 反序列化 {@link SnapshotState}、{@link LockStateMachineCore#installSnapshot}
     * 整体重建、位点钉到快照位点并同步库侧 latest 引用。
     *
     * @param info 快照文件信息；{@code null} 表示无快照可加载（无操作）
     * @throws IOException 文件不可读或内容不可解析——损坏快照必须显式
     *                     失败（启动期拒绝启动；安装期由库侧重试），MUST NOT
     *                     静默以空状态顶替
     */
    private void loadSnapshot(SingleFileSnapshotInfo info) throws IOException {
        if (info == null) {
            return;
        }
        try {
            byte[] bytes = Files.readAllBytes(info.getFile().getPath());
            core.installSnapshot(SnapshotState.parseFrom(bytes));
        } catch (InvalidProtocolBufferException e) {
            throw new IOException("corrupt snapshot file at " + info.getFile().getPath(), e);
        }
        setLastAppliedTermIndex(info.getTermIndex());
        storage.updateLatestSnapshot(info);
        log.info("loaded snapshot at {}", info.getTermIndex());
    }

    /**
     * 本状态机最近应用的位点（测试与诊断）。
     *
     * @return 位点，未应用过为 {@code null}
     */
    public TermIndex lastApplied() {
        return getLastAppliedTermIndex();
    }
}
