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
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

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
 * <p><b>快照边界（S2）</b>：不实现 {@code takeSnapshot}/{@code initialize}
 * 的快照加载（§7 归 S4/P2-15～16）；装配层必须关闭 Ratis 自动快照触发，
 * 否则基类默认实现返回 {@code -1} 会在长日志场景停滞快照流程（见
 * {@code RaftSubsystem} 配置）。
 *
 * <p><b>线程模型</b>：{@link #applyTransaction} 与 {@link #notifyLeaderChanged}
 * 由 Ratis 内部线程调用；对观察者的转发保持同步、无阻塞（观察者契约要求，
 * 见 {@link ApplyObserver}）。{@code observer} 为构造注入后的 volatile 悬挂点，
 * 装配期允许先建状态机后接观察者（{@code ReplicationGateway} 晚于子系统创建）。
 */
public final class LockStateMachine extends BaseStateMachine {

    /** 日志器：未实现生命周期与异常回执路径。 */
    private static final Logger log = LoggerFactory.getLogger(LockStateMachine.class);

    /** 语义内核（条目 → 引擎迁移）。 */
    private final LockStateMachineCore core;
    /** 应用/Leadership 观察者，装配期可替换（volatile：应用线程读、装配线程写）。 */
    private volatile ApplyObserver observer;
    /** 本节点 Raft 成员 id，用于把 Leader 变更事件折算成本节点角色。 */
    private volatile RaftPeerId selfId;

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
     * Ratis 初始化：记录本成员 id，交由基类登记 server/group。
     *
     * @param server      承载本状态机的 Raft 服务
     * @param groupId     组 id
     * @param raftStorage 存储
     * @throws IOException 基类初始化失败
     */
    @Override
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
        super.initialize(server, groupId, raftStorage);
        this.selfId = server.getId();
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
     * 任期队列清理的触发源，§4.4/§8）。
     *
     * @param groupMemberId 组成员标识
     * @param newLeaderId   新 Leader 的 peer id
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
    }

    /**
     * S2 不提供快照（§7 归 S4）：返回 {@code -1} 显式声明"不支持"，
     * 并要求装配层关闭自动触发（见类注释）。
     *
     * @return 恒为 {@code -1}
     * @throws IOException 从不抛出
     */
    @Override
    public long takeSnapshot() throws IOException {
        log.debug("takeSnapshot requested but S2 has no snapshot support (S4 scope)");
        return -1L;
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
