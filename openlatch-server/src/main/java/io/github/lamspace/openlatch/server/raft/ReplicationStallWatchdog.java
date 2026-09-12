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

import io.github.lamspace.openlatch.server.ClusterConfig;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 复制停摆自愈看门狗。
 *
 * <p><b>职责</b>：检测「本节点为当值 Leader 但任期提交永久冻结」的停摆态（Ratis 3.3.0
 * 存量缺陷），
 * 并按固定阶梯动作恢复写面：①让位一次（transferLeadership 至日志最新的健康对侧）；
 * ②让位后观察窗内提交仍零推进 → 进程级自杀式重启（{@code System.exit}，交外部
 * supervisor 拉起，以纯 follower 身份归群复位）。
 *
 * <p><b>检测信号（双条件，防误伤；窗口键＝连续在任段）</b>：仅当「本节点
 * 不间断担任 Leader 的时长 &gt; {@code T_stall}」<b>且</b>「本节点 commitIndex
 * 连续 {@code M} 个采样周期零推进」同时成立才判停摆。任何一次 commitIndex
 * 推进清零计数；在任期间的任期跃迁（含同任期再任——新
 * {@code LeaderStateImpl} 的任期条目同冻）不重置计时、仅更换让位配额；失去
 * Leader 角色才弃置整段——与 {@code SessionCoordinator} 失联判定（进度
 * 保护）同构。
 * 选举空窗与非 Leader 角色天然豁免（角色采样恒先判）；正常选举/追赶窗内提交恢复
 * 推进，快于 {@code T_stall} 下限 10s（实测恢复 1.6–1.9s，十倍余量）。
 *
 * <p><b>振荡保护</b>：每任期至多让位一次；升级重启带进程生命期内的
 * 全局冷却窗（默认 5 分钟内不二次触发重启路径，冷却期内改为 WARN 挂起等待）。
 * 跨进程重启的循环抑制不在本类职责——由外部 supervisor 的退避拉起策略承载。
 *
 * <p><b>可配性</b>：全部阈值为装配层钉死常量（由 election-timeout 折算），
 * 不引入运维配置键。
 *
 * <p><b>线程模型</b>：判定与动作全部发生在自有单线程调度器的 tick 序列内——
 * 无可变状态跨线程，无需加锁；{@link #close()} 幂等，先于其余组件关停
 * （{@code ClusterRuntime.close} 首位），关停后不再触发任何动作。探针抛出的
 * 一切异常（含 {@code IOException}，语义＝状态不可得/正被关停）按非 Leader
 * 处理（fail-open：观测不到就不动作），并以限频 WARN 留痕（取证可区分
 * "未判停摆"与"探针致盲"）。
 *
 * <p><b>测试缝</b>：{@link StallProbe} / {@link StallActions} 与全部时序参数经
 * 构造器注入，确定性单测以脚本化采样断言「≤T_stall+ε 内让位/重启触发、正常
 * 选举窗零误伤」；生产装配走 {@link #attach(RaftSubsystem)}，重启动作即
 * {@code System.exit(1)}。
 */
public final class ReplicationStallWatchdog implements AutoCloseable {

    /** 日志器：检测/动作/升级全程可观测（log 面即事件面）。 */
    private static final Logger log = LoggerFactory.getLogger(ReplicationStallWatchdog.class);

    /** {@code T_stall} 下限（毫秒）：max(10s, 5×election-timeout) 的钉死底。 */
    static final long T_STALL_FLOOR_MS = 10_000;
    /** 零推进连续样本数阈值 M：与失联判定容忍数同族（3 个采样周期）。 */
    static final int M_STALL_SAMPLES = 3;
    /** 让位 RPC 等待上限（毫秒，基座先例同值）。 */
    static final long TRANSFER_TIMEOUT_MS = 5_000;
    /** 让位→升级重启之间的观察窗下限（毫秒；实际取 max(本值, T_stall)）。 */
    static final long ESCALATE_GRACE_FLOOR_MS = 5_000;
    /** 进程重启冷却窗（毫秒：5 分钟内不二次触发重启路径）。 */
    static final long RESTART_COOLDOWN_MS = 300_000;
    /** 自杀式重启退出码（supervisor 侧可辨识「停摆自愈退出」）。 */
    static final int RESTART_EXIT_CODE = 1;

    /**
     * 停摆判定的一次采样快照。
     *
     * @param leader           本节点当前是否当值 Leader（角色门，false 一律豁免并弃段）
     * @param term             当前任期（让位配额键；不重置观察窗）
     * @param commitIndex      本节点自视角提交位点（零推进判定的被测量）
     * @param peerCommitIndexes 对侧上报 commitIndex（让位目标选择：取最大者）
     */
    record Snapshot(boolean leader, long term, long commitIndex,
                    Map<String, Long> peerCommitIndexes) {
    }

    /**
     * 停摆探针：读取角色/任期/提交位点（生产实现经 Ratis division 实况）。
     * 抛出的任何异常按「非 Leader」处理（fail-open）。
     */
    interface StallProbe {
        /**
         * 取一次采样。
         *
         * @return 当前快照
         * @throws Exception 状态不可得（关停中/查询失败）——看门狗据此豁免判定
         */
        Snapshot sample() throws Exception;
    }

    /**
     * 停摆恢复动作组（生产实现走 {@code AdminApi.transferLeadership} 与
     * {@code System.exit}；单测注入 fake 记录调用）。
     */
    interface StallActions {
        /**
         * 让位至指定对侧节点。
         *
         * @param peerId 目标 Raft 成员 id（日志最新存活对侧）
         * @return 让位请求成功返回 {@code true}；失败（被拒/超时/异常）返回
         *         {@code false}——不抛出，升级观察窗照常计时
         */
        boolean transferLeadership(String peerId);

        /**
         * 进程级自杀式重启（交外部 supervisor 拉起）。生产实现以退出码
         * {@value #RESTART_EXIT_CODE} 退出，可能不返回；单测注入记录器。
         */
        void restartProcess();
    }

    /** 停摆探针（生产＝division 实况；测试＝脚本序列）。 */
    private final StallProbe probe;
    /** 恢复动作组。 */
    private final StallActions actions;
    /** 停摆判定阈值 T_stall（毫秒）。 */
    private final long tStallMs;
    /** 采样周期（毫秒）。 */
    private final long sampleMs;
    /** 零推进连续样本阈值 M。 */
    private final int mSamples;
    /** 让位→升级重启的观察窗（毫秒）。 */
    private final long escalateGraceMs;
    /** 重启冷却窗（毫秒）。 */
    private final long cooldownMs;
    /** 自有单线程调度器（全部判定与动作的执行域）。 */
    private final ScheduledExecutorService scheduler;
    /** 当前连续在任段的停摆 episode；仅在失去 Leader 角色时弃置（任期跃迁不弃窗）。 */
    private Episode episode;
    /** 上一次触发重启动作的时刻（冷却窗基准，进程生命期内有效）。 */
    private long lastRestartMs;
    /** 本 tick 内的重启冷却提示去重标记（每冷却窗一条 WARN）。 */
    private boolean cooldownLogged;
    /** 探针连续失败计数（限频 WARN 留痕用，成功一次即清零）。 */
    private int probeFailStreak;
    /** 探针失败 WARN 限频周期（每 N 次连败重复一条）。 */
    private static final int PROBE_FAIL_LOG_EVERY = 25;
    /** 关停标志（幂等 close 与 tick 短路）。 */
    private volatile boolean closed;

    /**
     * 一段连续在任（Leader 身份不间断）的停摆观察状态：在任起始时刻、提交位点
     * 基准、连续零推进计数、让位配额与升级步进。仅在调度器线程内读写。
     *
     * <p><b>窗口键＝连续在任段而非任期号</b>：在任期间任期号跃迁（含同任期再任）
     * 不重置计时——跃迁后新 {@code LeaderStateImpl} 的任期条目同样
     * 冻结在窗内；失去 Leader 角色才弃置整段（角色翻转＝换代/下台，新主另起窗，
     * 选举与恢复追赶不误伤）。让位配额按任期记账（每任期至多一次）。
     */
    private static final class Episode {
        /** 在任段起始时刻（false→true 跃迁瞬间，毫秒）。 */
        final long leaderSinceMs = System.currentTimeMillis();
        /** 最近一次观察到的 commitIndex（推进检测基准）。 */
        long lastCommit;
        /** 连续零推进样本计数（任何推进清零）。 */
        int zeroStreak;
        /** 已消耗让位配额的任期号（-1＝本段尚无）。 */
        long transferUsedTerm = -1;
        /** 最近一次让位动作时刻（0＝未让位）；升级观察窗自此起算。 */
        long transferredAtMs;
        /** 本次让位是否已升级为重启（防重复触发动作）。 */
        boolean escalated;

        /**
         * 建立在任段 episode。
         *
         * @param commitIndex 跃迁时刻本节点 commitIndex（零推进基准）
         */
        Episode(long commitIndex) {
            this.lastCommit = commitIndex;
        }
    }

    /**
     * 全参构造（时序参数显式给定——生产经 {@link #attach(RaftSubsystem)} 折算，
     * 单测注入小参数压缩用例时长）。
     *
     * @param probe           停摆探针
     * @param actions         恢复动作组
     * @param tStallMs        停摆判定阈值 T_stall（毫秒）
     * @param sampleMs        采样周期（毫秒）
     * @param mSamples        零推进连续样本阈值 M（≥1）
     * @param escalateGraceMs 让位→升级重启观察窗（毫秒）
     * @param cooldownMs      重启冷却窗（毫秒）
     */
    ReplicationStallWatchdog(StallProbe probe, StallActions actions,
                             long tStallMs, long sampleMs, int mSamples,
                             long escalateGraceMs, long cooldownMs) {
        this.probe = probe;
        this.actions = actions;
        this.tStallMs = tStallMs;
        this.sampleMs = sampleMs;
        this.mSamples = mSamples;
        this.escalateGraceMs = escalateGraceMs;
        this.cooldownMs = cooldownMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "openlatch-replication-stall-watchdog");
            t.setDaemon(true);
            return t;
        });
        this.scheduler.scheduleAtFixedRate(this::tickSafely, sampleMs, sampleMs,
                TimeUnit.MILLISECONDS);
        log.info("replication stall watchdog armed: T_stall={}ms, sample={}ms, M={}, "
                + "escalateGrace={}ms, restartCooldown={}ms",
                tStallMs, sampleMs, mSamples, escalateGraceMs, cooldownMs);
    }

    /**
     * 生产装配：以子系统实况构建探针与动作组，阈值由 election-timeout
     * 折算钉死。
     *
     * @param subsystem 已启动的 Raft 子系统
     * @return 已在运行的看门狗
     */
    public static ReplicationStallWatchdog attach(RaftSubsystem subsystem) {
        ClusterConfig cc = subsystem.clusterConfig();
        long tStall = Math.max(T_STALL_FLOOR_MS, 5L * cc.electionTimeoutMs());
        long sample = cc.electionTimeoutMs();
        long grace = Math.max(ESCALATE_GRACE_FLOOR_MS, tStall);
        StallProbe probe = () -> {
            var div = subsystem.division();
            var info = div.getInfo();
            Map<String, Long> peers = new HashMap<>();
            long commit = -1;
            String self = subsystem.clusterConfig().selfPeerId();
            for (CommitInfoProto ci : div.getCommitInfos()) {
                String id = ci.getServer().getId().toStringUtf8();
                if (id.equals(self)) {
                    commit = ci.getCommitIndex();
                } else {
                    peers.put(id, ci.getCommitIndex());
                }
            }
            return new Snapshot(info.isLeader(), info.getCurrentTerm(), commit, peers);
        };
        StallActions actions = new StallActions() {
            @Override
            public boolean transferLeadership(String peerId) {
                var client = subsystem.acquireClient();
                if (client == null) {
                    return false;
                }
                try {
                    var reply = client.admin()
                            .transferLeadership(RaftPeerId.valueOf(peerId), TRANSFER_TIMEOUT_MS);
                    return reply != null && reply.isSuccess();
                } catch (Exception e) {
                    log.warn("stall watchdog transferLeadership({}) failed: {}", peerId, e.toString());
                    return false;
                }
            }

            @Override
            public void restartProcess() {
                log.error("replication stall unrecoverable by transfer: exiting (code {}) "
                        + "for supervisor restart — node {}", RESTART_EXIT_CODE, cc.nodeId());
                System.exit(RESTART_EXIT_CODE);
            }
        };
        return new ReplicationStallWatchdog(probe, actions, tStall, sample, M_STALL_SAMPLES,
                grace, RESTART_COOLDOWN_MS);
    }

    /**
     * tick 入口：吞掉一切异常（判定仪器不得杀死自身调度线程；探针异常路径已在
     * {@link #tick()} 内处理，此处为兜底）。
     */
    private void tickSafely() {
        try {
            tick();
        } catch (Throwable t) {
            if (!closed) {
                log.warn("stall watchdog tick failed (ignored)", t);
            }
        }
    }

    /**
     * 单 tick 判定（调度线程）：采样 → 角色门 → 双条件停摆判定 → 动作阶梯。
     *
     * <p>步序：①非 Leader（含探针异常）→ 弃置在任段 episode（重新在任另起窗）；
     * ②首次/重新在任 → 建 episode（零推进基准取当下）；③commitIndex 推进 →
     * 清零计数；④「连续在任时长 &gt; T_stall ∧ 连续零推进 ≥M」成立且当前任期
     * 让位配额未用 → 让位（目标＝对侧 commitIndex 最大者；无目标视同让位失败，
     * 照常进入升级观察窗）——任期跃迁不弃窗、仅按任期换配额；⑤本任期让位后
     * 观察窗到期提交仍零推进 → 升级重启（冷却窗内降级为 WARN 挂起）。
     */
    private void tick() {
        if (closed) {
            return;
        }
        Snapshot s;
        try {
            s = probe.sample();
        } catch (Exception e) {
            // 状态不可得：fail-open，不动作。探针异常限频 WARN 留痕——停摆
            // 取证必须能区分"未判停摆"与"探针持续失败致盲"（静默豁免不可观测）。
            probeFailStreak++;
            if (probeFailStreak == 1 || probeFailStreak % PROBE_FAIL_LOG_EVERY == 0) {
                log.warn("stall watchdog probe failed {} time(s), episode dropped: {}",
                        probeFailStreak, e.toString());
            }
            episode = null;
            return;
        }
        probeFailStreak = 0;
        if (s == null || !s.leader()) {
            episode = null;
            return;
        }
        Episode ep = episode;
        if (ep == null) {
            episode = new Episode(s.commitIndex());
            cooldownLogged = false;
            return;
        }
        if (s.commitIndex() != ep.lastCommit) {
            ep.lastCommit = s.commitIndex();
            ep.zeroStreak = 0;
            return;
        }
        ep.zeroStreak++;
        long elapsed = System.currentTimeMillis() - ep.leaderSinceMs;
        if (elapsed <= tStallMs || ep.zeroStreak < mSamples) {
            return;
        }
        if (ep.transferUsedTerm != s.term()) {
            ep.transferUsedTerm = s.term();
            ep.transferredAtMs = System.currentTimeMillis();
            ep.escalated = false;
            String target = freshestPeer(s.peerCommitIndexes());
            log.warn("replication stall detected: continuous leadership {}ms (term {}), "
                            + "commit {} frozen for {} samples; transferring to freshest peer {}",
                    elapsed, s.term(), s.commitIndex(), ep.zeroStreak, target);
            if (target != null) {
                boolean ok = actions.transferLeadership(target);
                log.info("stall watchdog transferLeadership({}) result={}", target, ok);
            } else {
                log.warn("stall watchdog: no freshest peer available, escalate path armed");
            }
            return;
        }
        // 本任期让位已发起：观察窗到期提交仍冻结 → 升级重启（每次让位一次 + 冷却窗）。
        if (ep.escalated || System.currentTimeMillis() - ep.transferredAtMs < escalateGraceMs) {
            return;
        }
        ep.escalated = true;
        long now = System.currentTimeMillis();
        if (now - lastRestartMs < cooldownMs) {
            if (!cooldownLogged) {
                cooldownLogged = true;
                log.warn("replication stall persists after transfer; restart suppressed by "
                        + "cooldown ({}ms left) — manual intervention advised",
                        cooldownMs - (now - lastRestartMs));
            }
            return;
        }
        lastRestartMs = now;
        log.error("replication stall persists after leadership transfer (term {}, commit {}): "
                + "escalating to process restart for supervisor-driven rejoin",
                s.term(), s.commitIndex());
        actions.restartProcess();
    }

    /**
     * 选择让位目标：对侧中 commitIndex 最大（日志最新）者；无可用对侧返回
     * {@code null}（视同让位失败进入升级窗）。
     *
     * @param peers 对侧 commitIndex 表
     * @return 目标 peer id，或 {@code null}
     */
    private static String freshestPeer(Map<String, Long> peers) {
        String best = null;
        long bestCommit = Long.MIN_VALUE;
        for (Map.Entry<String, Long> e : peers.entrySet()) {
            long c = e.getValue() == null ? Long.MIN_VALUE : e.getValue();
            if (best == null || c > bestCommit) {
                best = e.getKey();
                bestCommit = c;
            }
        }
        return best;
    }

    /**
     * 关停：停止调度，幂等。在途 tick 不再触发任何动作。
     */
    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        scheduler.shutdownNow();
    }
}
