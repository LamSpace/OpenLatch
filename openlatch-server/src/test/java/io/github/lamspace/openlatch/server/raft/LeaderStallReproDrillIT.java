package io.github.lamspace.openlatch.server.raft;

import com.google.protobuf.ByteString;
import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.RaftEntryType;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.server.DivisionInfo;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * leader 复制停摆最小复现采样器（phase2-leader-stall-followup 任务 1.1，design D1）：
 * in-JVM 三节点在「旧 leader 带脏条目重启归群」语境下按 K 轮采样停摆命中率，
 * 为根因定位（1.2）与修复路径验收（2A 复跑归零 / 2B 复跑全自愈）提供复现基座。
 *
 * <p><b>单轮构造</b>（前件与收口缺陷档案
 * {@code archive/2026-09-06-phase2-release-closure/defects/leader-replication-stall-ratis-3.3.0.md}
 * 对齐，进程内等价复写「先主后从」滚动重启的首拍）：
 * <ol>
 *   <li>{@link ClusterHarness#start(int, long) ClusterHarness.start}(3, 800ms)——真实
 *       {@code RaftSubsystem}+gRPC 三节点，选举超时与进程级演练同拍；探针恒开
 *       （生产路径，制造 800ms 周期的任期条目）；</li>
 *   <li>稳态写流建立日志水位（协议车道 acquire+release）；</li>
 *   <li>停当值 Leader 前提交在途 NOOP（脏尾：仅落本机日志、未达多数派的
 *       本任期条目），随后 {@code stopNode}——SIGKILL 语义：Ratis 3.3.0
 *       {@code RaftServerImpl.close()} 仅停角色，<b>不做</b>让位/清理日志，
 *       数据目录保留（源码对账见 1.2 档案）；</li>
 *   <li>后台写载持续打流（等价滚动演练的驱动线程，产面错误率与档案
 *       5%–28% 同量纲可比）；停主后以 400–1600ms 随机延迟 {@code restartNode}
 *       ——归群<b>与选举窗口重叠</b>（档案互拒链语境：存活侧同刻超时互斥
 *       拉票、归来旧主以更高任期/最长日志当选随即陷入停滞）。实测教训：
 *       "先等新 leader ready 再归群"会结构性排除停摆形态（v1 12/12
 *       RECOVERED——选举先收敛则 NOOP 已由双节点多数派提交，归群无法冻结
 *       不需要它的多数派），故本步不设就绪门；</li>
 *   <li>宽限期后开采样窗：250ms 周期采样当值 Leader 的
 *       {@code (term, commitIndex, isLeaderReady)} 并向其投 NOOP 探针
 *       （生产探车道 {@code gateway().submit(NOOP)}）。</li>
 * </ol>
 *
 * <p><b>停摆判定</b>（design D1，与档案症状同形）：窗口内当值 Leader 身份
 * （节点 id + 任期）恒定、探针<b>零</b>成功（NOT_READY＝库内
 * {@code LeaderNotReadyException} 回执即 {@code startupLogEntry} 永不应用；
 * IN_FLIGHT＝ready 但提交冻结的无回执形态）、窗口首尾 commitIndex 零推进
 * ——三者同时成立判 STALL；探针在窗口内首次成功判 RECOVERED（记恢复耗时）。
 * Leader 身份切换/持续缺位判 CHURN（另一形态，单独计数不入命中率分子）。
 *
 * <p><b>本用例是采样仪器而非验收门禁</b>：不断言命中率&gt;0——停摆是概率
 * 双稳态（档案：热机 4/4、冷机 1/5），命中判定由任务验收在档案侧完成
 * （1.1 verify「命中率 &gt;0 且显著」；2A/2B 的「复跑归零/全自愈」同理）。
 * 仅当全部轮次 ABORTED（构造不可得：端口/环境漂移）时用例失败。
 *
 * <p><b>门控</b>：{@code @Tag("drill")}，默认构建排除（failsafe
 * {@code excludedGroups}）；执行 {@code -Pdrill}（见 openlatch-server pom
 * drill profile），K 轮高频采样耗时长（默认 8 轮 ≈ 5–8 分钟），不入缺省门禁。
 *
 * <p><b>产物</b>：每轮前置事实 + 采样序列 CSV + 判词追加写入仓库根
 * {@code docs/leader-stall-repro-<日期>.md}（failsafe 工作目录为模块域，
 * 落盘路径取 {@code ../docs/}，与其余演练报告同纪律）；轮末 stdout 摘要
 * {@code [STALL-DRILL]}。
 */
@Tag("drill")
@Timeout(value = 30, unit = TimeUnit.MINUTES)
class LeaderStallReproDrillIT {

    /** 默认采样轮数 K（design D1"循环采样 K 轮"；{@code -Ddrill.stall.rounds} 覆盖）。 */
    private static final int DEFAULT_ROUNDS = 8;
    /** 选举超时（毫秒）：与进程级演练（LeaderKill/RollingRestart）同拍。 */
    private static final long ELECTION_MS = 800;
    /** 脏尾在途 NOOP 条数：kill 前落本机日志、未及多数派确认的任期条目。 */
    private static final int DIRTY_TAIL_NOOPS = 6;
    /** 稳态/健康窗协议车道写流轮数（每轮 acquire+release 各 1 条目）。 */
    private static final int TRAFFIC_ROUNDS = 8;
    /** 停主→归群随机延迟下界（毫秒）：与选举超时（min 400ms）同拍起步。 */
    private static final long RACE_MIN_MS = 400;
    /** 停主→归群随机延迟上界（毫秒）：覆盖两存活节点约两轮互拒拉票窗口。 */
    private static final long RACE_MAX_MS = 1_600;
    /** 后台写载投条周期（毫秒）：等价滚动演练驱动线程节奏。 */
    private static final long LOAD_INTERVAL_MS = 100;
    /** 归群后宽限期（毫秒）：旧主重启装配 + 可能触发的重选窗口豁免（D2 防误伤同构）。 */
    private static final long GRACE_MS = 3_000;
    /** 采样窗长（毫秒）：档案症状为 200+ 秒不自愈，窗内持续冻结即判 STALL。 */
    private static final long WINDOW_MS = 20_000;
    /** 采样周期（毫秒）。 */
    private static final long SAMPLE_MS = 250;
    /** 单次探针"无回执"判定上限（毫秒）：正常 NOOP 提交回执 ≪ 此值。 */
    private static final long PROBE_TIMEOUT_MS = 1_500;
    /** 窗口中段连续无主样本数上限：超过判 CHURN（选举风暴形态，另案记录）。 */
    private static final int MAX_EMPTY_STREAK = 30;

    /** 流量键唯一性计数（跨轮不重名，避免残留锁干扰）。 */
    private static final AtomicInteger KEY_SEQ = new AtomicInteger();

    /** 单轮判词（CHURN/ABORTED 不计入命中率分子，也不计入分母）。 */
    private enum Verdict {
        /** 停摆命中：身份稳定 + 探针零成功 + commitIndex 零推进贯穿全窗。 */
        STALL,
        /** 收敛：窗口内探针首次成功（记恢复耗时）。 */
        RECOVERED,
        /** 形态偏移：窗口内 Leader 身份切换/持续缺位（非本档案签名，另计）。 */
        CHURN,
        /** 构造不可得：前置编排失败（环境/装配问题，非故障证据）。 */
        ABORTED
    }

    /** 探针三态分类：OK＝回执完成（提交+应用推进）；NOT_READY＝LeaderNotReady 回执；IN_FLIGHT＝无回执；OTHER＝其余失败。 */
    private enum Probe { OK, NOT_READY, IN_FLIGHT, OTHER }

    /** 单次采样记录（归档 CSV 行：窗口相对毫秒, 当值 leader 节点 id, 任期, isLeaderReady, commitIndex, 探针分类）。 */
    private record Sample(long tMs, int leaderId, long term, boolean ready, long commit, Probe probe) {

        /** @return CSV 一行 */
        String csv() {
            return tMs + "," + leaderId + "," + term + "," + ready + "," + commit + "," + probe;
        }
    }

    /** 单轮结果载体（前置事实 + 采样序列 + 判词与恢复计时）。 */
    private static final class RoundOutcome {
        /** 轮序号（1 起）。 */
        final int round;
        /** 采样序列。 */
        final List<Sample> samples = new ArrayList<>();
        /** 判词。 */
        Verdict verdict = Verdict.ABORTED;
        /** 备注/失败原因。 */
        String note = "";
        /** 被杀原 leader 节点 id（0＝未推进到该步）。 */
        int killedId;
        /** 被杀时的原 leader 任期。 */
        long killedTerm = -1;
        /** 被杀时刻原 leader commitIndex。 */
        long commitAtKill = -1;
        /** 归群时刻的当值 leader 节点 id（0＝归群瞬间尚无主）。 */
        int newLeaderId;
        /** 归群时刻当值 leader 任期。 */
        long newLeaderTerm = -1;
        /** 归群时刻当值 leader commitIndex（无主 -1）。 */
        long commitPreRejoin = -1;
        /** 停主→归群的竞速延迟实取值（毫秒）。 */
        long raceDelayMs;
        /** 后台写载成功条目数（应用回执完成）。 */
        final AtomicLong loadOk = new AtomicLong();
        /** 后台写载失败条目数（拒绝/终结异常——产面错误率同量纲证据）。 */
        final AtomicLong loadErr = new AtomicLong();
        /** 探针成功次数（含 RECOVERED 终结探针）。 */
        int probeOk;
        /** 探针 NOT_READY 次数。 */
        int probeNotReady;
        /** 探针 IN_FLIGHT 次数。 */
        int probeInFlight;
        /** 探针 OTHER 次数。 */
        int probeOther;
        /** 收敛轮：归群→首次探针成功耗时（毫秒，0＝未收敛）。 */
        long recoveredMs;

        RoundOutcome(int round) {
            this.round = round;
        }
    }

    /** 报告文件路径（首轮写入时按当日日期定名）。 */
    private Path reportPath;

    @Test
    void sampleLeaderStallRateAcrossRounds() throws Exception {
        int rounds = Integer.getInteger("drill.stall.rounds", DEFAULT_ROUNDS);
        assertThat(rounds).as("drill.stall.rounds").isPositive();
        openReport(rounds);

        List<RoundOutcome> results = new ArrayList<>();
        for (int r = 1; r <= rounds; r++) {
            RoundOutcome ro = runRound(r);
            results.add(ro);
            appendRound(ro);
            System.out.println("[STALL-DRILL] round " + r + " verdict=" + ro.verdict
                    + " recoveredMs=" + ro.recoveredMs + " race=" + ro.raceDelayMs
                    + "ms samples=" + ro.samples.size()
                    + " probe(ok/notReady/inFlight/other)=" + ro.probeOk + "/" + ro.probeNotReady
                    + "/" + ro.probeInFlight + "/" + ro.probeOther
                    + " load=" + ro.loadOk.get() + "/" + ro.loadErr.get() + " " + ro.note);
        }

        long stall = results.stream().filter(x -> x.verdict == Verdict.STALL).count();
        long recovered = results.stream().filter(x -> x.verdict == Verdict.RECOVERED).count();
        long churn = results.stream().filter(x -> x.verdict == Verdict.CHURN).count();
        long aborted = results.stream().filter(x -> x.verdict == Verdict.ABORTED).count();
        long valid = stall + recovered + churn;
        appendSummary(rounds, stall, recovered, churn, aborted);
        System.out.println("[STALL-DRILL] SUMMARY rounds=" + rounds + " STALL=" + stall
                + " RECOVERED=" + recovered + " CHURN=" + churn + " ABORTED=" + aborted
                + " hitRate(valid)=" + String.format("%.2f", valid == 0 ? 0 : 100.0 * stall / valid)
                + "% report=" + reportPath.toAbsolutePath());
        // 仪器健康度：至少一轮走完编排（全 ABORTED＝构造不可得，属装配/环境缺陷，
        // 须显式失败并复盘，不得静默读作"未复现"）。命中率本身不设断言（见类 Javadoc）。
        assertThat(valid).as("有效轮数（非全 ABORTED）").isPositive();
    }

    /** 单轮编排（构造→采样→判词），任何前置失败折返 ABORTED（带原因），不上抛。 */
    private RoundOutcome runRound(int r) {
        RoundOutcome out = new RoundOutcome(r);
        try (ClusterHarness h = ClusterHarness.start(3, ELECTION_MS)) {
            ClusterHarness.Node killed = h.leader();
            out.killedId = killed.id;
            out.killedTerm = term(killed);

            // ① 稳态写流：建立日志水位（协议车道全量路径）
            traffic(h, killed, TRAFFIC_ROUNDS, out);

            // ② 脏尾：kill 前投在途 NOOP——仅落本机日志、未及多数派确认，
            //    即档案"携带旧任期脏条目"的本机残留。
            for (int i = 0; i < DIRTY_TAIL_NOOPS; i++) {
                killed.runtime.gateway().submit(RaftEntryType.NOOP, ByteString.EMPTY);
            }
            Thread.sleep(150); // 等 append 落本机日志（不等其提交——提交=无脏尾）
            out.commitAtKill = commitIndex(killed);
            h.stopNode(killed.id); // SIGKILL 语义：close 不让位不清日志，目录保留

            // ③ 持续写载（等价滚动演练驱动线程）+ 停主后随机延迟归群——
            //    归群与选举窗口竞速重叠（档案互拒链语境），不设就绪门：
            //    等 ready 再归群会结构性排除"任期 NOOP 永不提交"的停摆形态
            AtomicBoolean loadStop = new AtomicBoolean();
            Thread load = startLoadDriver(h, loadStop, out);
            out.raceDelayMs = ThreadLocalRandom.current().nextLong(RACE_MIN_MS, RACE_MAX_MS + 1);
            sleep(out.raceDelayMs);
            ClusterHarness.Node pre = h.leader();
            out.newLeaderId = pre == null ? 0 : pre.id;
            out.newLeaderTerm = pre == null ? -1 : term(pre);
            out.commitPreRejoin = pre == null ? -1 : commitIndex(pre);

            // ④ 旧主带脏条目归群 → ⑤ 宽限后采样窗（探针与写载并行，
            //    探针仅承担判定，写载维持产面压力与档案形态对齐）
            h.restartNode(killed.id);
            long tRejoin = System.currentTimeMillis();
            Thread.sleep(GRACE_MS);
            try {
                sampleWindow(h, out, tRejoin);
            } finally {
                loadStop.set(true);
                load.join(5_000);
            }
        } catch (Throwable t) {
            out.verdict = Verdict.ABORTED;
            out.note = "构造失败: " + t;
        }
        return out;
    }

    /**
     * 采样窗：每 {@value #SAMPLE_MS}ms 取当值 Leader 的
     * {@code (term, commitIndex, isLeaderReady)} 并投一发 NOOP 探针；
     * 判词规则见类 Javadoc「停摆判定」。探针成功即收敛提前收窗。
     */
    private void sampleWindow(ClusterHarness h, RoundOutcome out, long tRejoin) throws Exception {
        long winStart = System.currentTimeMillis();
        int leaderId = -1;
        long term0 = -1;
        long winCommit0 = -1;
        int emptyStreak = 0;
        int samplesNeeded = (int) (WINDOW_MS / SAMPLE_MS);
        while (System.currentTimeMillis() - winStart < WINDOW_MS) {
            ClusterHarness.Node cur = h.leader();
            DivisionInfo di = info(cur);
            if (cur == null || di == null || !di.isLeader()) {
                // 窗口头部的无主段不计（重选中）；中段持续无主 → CHURN
                if (++emptyStreak > MAX_EMPTY_STREAK && !out.samples.isEmpty()) {
                    out.verdict = Verdict.CHURN;
                    out.note = "窗口持续无主 " + emptyStreak + " 样本";
                    return;
                }
                sleep(SAMPLE_MS);
                continue;
            }
            emptyStreak = 0;
            if (leaderId == -1) {
                leaderId = cur.id;
                term0 = di.getCurrentTerm();
                winCommit0 = commitIndex(cur);
            } else if (cur.id != leaderId || di.getCurrentTerm() != term0) {
                out.verdict = Verdict.CHURN;
                out.note = "窗口内 Leader 身份切换 " + leaderId + "@" + term0
                        + " → " + cur.id + "@" + di.getCurrentTerm();
                return;
            }
            Probe p = probe(cur);
            out.samples.add(new Sample(System.currentTimeMillis() - winStart, cur.id,
                    di.getCurrentTerm(), di.isLeaderReady(), commitIndex(cur), p));
            switch (p) {
                case OK -> {
                    out.probeOk++;
                    out.verdict = Verdict.RECOVERED;
                    out.recoveredMs = System.currentTimeMillis() - tRejoin;
                    return;
                }
                case NOT_READY -> out.probeNotReady++;
                case IN_FLIGHT -> out.probeInFlight++;
                case OTHER -> out.probeOther++;
            }
            sleep(SAMPLE_MS);
        }
        // 全窗零探针成功：commitIndex 首尾零推进 → STALL；有推进（探针误伤
        // 边缘形态）→ CHURN 另计，防把"提交在动但探针被拒"的异象记成命中。
        ClusterHarness.Node cur = h.leader();
        long endCommit = commitIndex(cur);
        if (out.samples.size() < samplesNeeded / 2) {
            out.verdict = Verdict.CHURN;
            out.note = "有效样本不足（" + out.samples.size() + "/" + samplesNeeded + "）";
        } else if (endCommit == winCommit0) {
            out.verdict = Verdict.STALL;
            out.note = "全窗 " + out.samples.size() + " 样本零提交推进"
                    + "（ready=" + (info(cur) != null && info(cur).isLeaderReady()) + "）";
        } else {
            out.verdict = Verdict.CHURN;
            out.note = "commit " + winCommit0 + "→" + endCommit + " 推进但探针零成功，形态另记";
        }
    }

    /**
     * 一发 NOOP 探针（生产探车道）：经当值 Leader 的
     * {@code gateway().submit} 提交并限时等待应用回执。
     *
     * @param leader 当值 Leader 节点
     * @return 探针分类（OK/NOT_READY/IN_FLIGHT/OTHER）
     */
    private static Probe probe(ClusterHarness.Node leader) {
        CompletableFuture<ApplyResult> f =
                leader.runtime.gateway().submit(RaftEntryType.NOOP, ByteString.EMPTY);
        try {
            f.get(PROBE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            return Probe.OK;
        } catch (ExecutionException e) {
            String chain = chainText(e);
            return chain.contains("LeaderNotReady") ? Probe.NOT_READY : Probe.OTHER;
        } catch (TimeoutException e) {
            return Probe.IN_FLIGHT;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Probe.OTHER;
        }
    }

    /**
     * 后台写载线程：每 {@value #LOAD_INTERVAL_MS}ms 向随机存活节点的内部网关投一发
     * NOOP（Ratis 客户端自动路由当值 Leader，与生产提交同车道），成功/失败计数
     * 入档——档案"客户端错误率 5%–28%"的产面同形观测；失败非致命，本就是证据。
     *
     * @param h    集群基座
     * @param stop 停止标志（采样窗结束后置位）
     * @param out  本轮结果载体（计数落点）
     * @return 已启动的守护线程
     */
    private static Thread startLoadDriver(ClusterHarness h, AtomicBoolean stop, RoundOutcome out) {
        return Thread.ofPlatform().name("stall-load-" + out.round).daemon(true).start(() -> {
            while (!stop.get()) {
                List<ClusterHarness.Node> alive = h.nodes().stream()
                        .filter(ClusterHarness.Node::alive).toList();
                if (!alive.isEmpty()) {
                    ClusterHarness.Node x =
                            alive.get(ThreadLocalRandom.current().nextInt(alive.size()));
                    var rt = x.runtime;
                    if (rt != null) {
                        rt.gateway().submit(RaftEntryType.NOOP, ByteString.EMPTY)
                                .whenComplete((r, e) -> {
                                    if (e == null) {
                                        out.loadOk.incrementAndGet();
                                    } else {
                                        out.loadErr.incrementAndGet();
                                    }
                                });
                    }
                }
                sleep(LOAD_INTERVAL_MS);
            }
        });
    }

    /** 协议车道写流：rounds 轮 acquire+release，任一失败上抛（调用方折返 ABORTED）。 */
    private static void traffic(ClusterHarness h, ClusterHarness.Node n, int rounds,
                                RoundOutcome out) throws Exception {
        ClusterHarness.TestConn c = h.connect(n);
        c.hello(1);
        for (int i = 0; i < rounds; i++) {
            String key = "stall-" + out.round + "-" + KEY_SEQ.incrementAndGet();
            Envelope g = c.request(acquire(2L + 2 * i, key));
            assertThat(g.getAcquireResponse().getStatus())
                    .as("轮 %d 节点 %d acquire %s", out.round, n.id, key)
                    .isEqualTo(StatusCode.OK);
            Envelope rel = c.request(release(3L + 2 * i, key, g.getAcquireResponse().getLeaseToken()));
            assertThat(rel.getReleaseResponse().getStatus())
                    .as("轮 %d 节点 %d release %s", out.round, n.id, key)
                    .isEqualTo(StatusCode.OK);
        }
    }

    /** ACQUIRE 信封（不排队、租约 30s——健康窗写流语义）。 */
    private static Envelope acquire(long rid, String key) {
        return Envelope.newBuilder().setProtocolVersion(2).setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid)
                .setAcquireRequest(AcquireRequest.newBuilder().setKey(key)
                        .setLockType(LockType.LOCK_TYPE_REENTRANT)
                        .setThreadId(1L).setLeaseMs(30_000).setWaitMs(-1))
                .build();
    }

    /** RELEASE 信封。 */
    private static Envelope release(long rid, String key, long token) {
        return Envelope.newBuilder().setProtocolVersion(2).setType(MessageType.LOCK_RELEASE)
                .setRequestId(rid)
                .setReleaseRequest(io.github.lamspace.openlatch.protocol.ReleaseRequest
                        .newBuilder().setKey(key).setLeaseToken(token).setThreadId(1L))
                .build();
    }

    /** 节点 DivisionInfo 快照（停机/未启动/关闭中返回 {@code null}）。 */
    private static DivisionInfo info(ClusterHarness.Node n) {
        if (n == null || n.runtime == null) {
            return null;
        }
        try {
            return n.runtime.subsystem().division().getInfo();
        } catch (IOException | RuntimeException e) {
            return null;
        }
    }

    /** 当值角色任期（不可得 -1）。 */
    private static long term(ClusterHarness.Node n) {
        DivisionInfo di = info(n);
        return di == null ? -1 : di.getCurrentTerm();
    }

    /**
     * 本节点自视角 commitIndex：取 {@code Division.getCommitInfos()} 中本节点
     * 条目（Ratis 3.3.0 {@code RaftServerImpl.getCommitInfos()} 恒含 self）；
     * 缺项/异常以 {@code lastAppliedIndex} 兜底（应用位点⊆提交位点，
     * "冻结"判据方向一致——leader 仅应用已提交条目）。
     */
    private static long commitIndex(ClusterHarness.Node n) {
        try {
            if (n != null && n.runtime != null) {
                String self = "n" + n.id;
                for (CommitInfoProto ci : n.runtime.subsystem().division().getCommitInfos()) {
                    if (ci.getServer().getId().toStringUtf8().equals(self)) {
                        return ci.getCommitIndex();
                    }
                }
            }
        } catch (IOException | RuntimeException e) {
            // 关停竞态：落入兜底路径
        }
        DivisionInfo di = info(n);
        return di == null ? -1 : di.getLastAppliedIndex();
    }

    /** 异常链全文（分类探针失败形态用：含各 cause 的类名与消息）。 */
    private static String chainText(Throwable t) {
        StringBuilder sb = new StringBuilder();
        for (Throwable x = t; x != null && x != x.getCause(); x = x.getCause()) {
            sb.append(x.getClass().getName()).append(": ").append(x.getMessage()).append('\n');
        }
        return sb.toString();
    }

    /** 定步长休眠（不吞中断）。 */
    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---------- 归档（docs/leader-stall-repro-<日期>.md，纪律同其余演练报告） ----------

    /**
     * 定名/续建报告文件：文件头首次写；每次运行另起一节 run 头（构造、K、
     * 判据、命令、环境）——同日多构造版本（如 v1 先等新主就绪 vs v2 竞速
     * 归群）的证据可分辨。
     *
     * @param rounds 本轮跑批的 K 值
     * @throws IOException 落盘失败
     */
    private void openReport(int rounds) throws IOException {
        reportPath = Path.of("..", "docs", "leader-stall-repro-"
                + LocalDate.now() + ".md");
        Files.createDirectories(reportPath.getParent());
        if (!Files.exists(reportPath)) {
            Files.writeString(reportPath, "# leader 复制停摆复现采样档案（phase2-leader-stall-followup 任务 1.1）\n\n");
        }
        Files.writeString(reportPath, "## Run "
                + LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss"))
                + "（K=" + rounds + "，构造：稳态写流→脏尾停主→写载+竞速归群）\n\n"
                + "- 构造：design D1 in-JVM 3 节点（真实 RaftSubsystem+gRPC，election-timeout "
                + ELECTION_MS + "ms，探针恒开）——稳态写流建立水位→在途 NOOP 脏尾→"
                + "SIGKILL 语义停当值 Leader→后台写载打流 + " + RACE_MIN_MS + "–" + RACE_MAX_MS
                + "ms 随机延迟归群（与选举窗重叠、不设就绪门）→宽限 " + GRACE_MS
                + "ms 后采样窗。\n"
                + "- 判据：窗口内 Leader 身份（节点+任期）恒定 ∧ NOOP 探针零成功 ∧ commitIndex 首尾零推进 → STALL；"
                + "身份切换/持续无主 → CHURN（不计命中）。\n"
                + "- 命令：`mvn -s <settings> -pl openlatch-server verify -Pdrill -Dit.test=LeaderStallReproDrillIT "
                + "-Dtest=NoSuchUnitTest -Dsurefire.failIfNoSpecifiedTests=false "
                + "-Dfailsafe.failIfNoSpecifiedTests=false -Ddrill.stall.rounds=" + rounds + "`。\n"
                + "- 环境：" + System.getProperty("java.version") + " / "
                + System.getProperty("os.name") + " " + System.getProperty("os.arch")
                + " / " + Runtime.getRuntime().availableProcessors() + " cpus\n\n",
                java.nio.file.StandardOpenOption.APPEND);
    }

    /** 单轮一节：前置事实表 + 判词 + 采样序列 CSV。 */
    private void appendRound(RoundOutcome o) throws IOException {
        StringBuilder sb = new StringBuilder();
        sb.append("### 轮 ").append(o.round).append("（")
                .append(LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss")))
                .append("）— **").append(o.verdict).append("**")
                .append(o.verdict == Verdict.RECOVERED
                        ? "（归群后 " + o.recoveredMs + "ms 收敛）" : "").append("\n\n");
        sb.append("| 前置 | 值 |\n|---|---|\n");
        sb.append("| 杀主（SIGKILL 语义停） | node").append(o.killedId)
                .append(" term=").append(o.killedTerm)
                .append(" commit@kill=").append(o.commitAtKill).append(" |\n");
        sb.append("| 竞速归群 | 停主后 ").append(o.raceDelayMs)
                .append("ms 归群；归群瞬刻 leader=node").append(o.newLeaderId)
                .append(" term=").append(o.newLeaderTerm)
                .append(" commit=").append(o.commitPreRejoin).append(" |\n");
        sb.append("| 写载 | 成功 ").append(o.loadOk.get()).append(" / 失败 ")
                .append(o.loadErr.get()).append(" |\n");
        sb.append("| 采样 | 样本 ").append(o.samples.size())
                .append("，探针 ok/notReady/inFlight/other = ")
                .append(o.probeOk).append('/').append(o.probeNotReady).append('/')
                .append(o.probeInFlight).append('/').append(o.probeOther).append(" |\n");
        if (!o.note.isEmpty()) {
            sb.append("| 备注 | ").append(o.note).append(" |\n");
        }
        sb.append("\n");
        if (!o.samples.isEmpty()) {
            sb.append("```\ncsv: tMs,leaderId,term,leaderReady,commitIndex,probe\n");
            o.samples.forEach(s -> sb.append(s.csv()).append('\n'));
            sb.append("```\n\n");
        }
        Files.writeString(reportPath, sb.toString(), java.nio.file.StandardOpenOption.APPEND);
    }

    /** 收尾汇总节：命中率与轮次分布（1.1 verify「采样数据入档」的结论面）。 */
    private void appendSummary(int rounds, long stall, long recovered, long churn, long aborted)
            throws IOException {
        long valid = stall + recovered + churn;
        Files.writeString(reportPath, "### 汇总\n\n"
                + "- 轮数 K=" + rounds + "：STALL=" + stall + " / RECOVERED=" + recovered
                + " / CHURN=" + churn + " / ABORTED=" + aborted + "\n"
                + "- 命中率（STALL/有效轮，CHURN 计入分母不计入分子）："
                + String.format("%.2f", valid == 0 ? 0 : 100.0 * stall / valid) + " %\n"
                + "- 判读：STALL>0 即复现基座建立（任务 1.1 verify）；修复路径落地后复跑，"
                + "2A 期望归零、2B 期望全轮 RECOVERED 且恢复耗时 ≲ T_stall+ε。\n\n",
                java.nio.file.StandardOpenOption.APPEND);
    }
}
