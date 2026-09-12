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

package io.github.lamspace.openlatch.server.admin;

import io.github.lamspace.openlatch.core.CoreEngine;
import io.github.lamspace.openlatch.core.CoreInspection;
import io.github.lamspace.openlatch.core.CoreStats;
import io.github.lamspace.openlatch.core.KeyFamily;
import io.github.lamspace.openlatch.protocol.AdminKeyDetailRequest;
import io.github.lamspace.openlatch.protocol.AdminKeyDetailResponse;
import io.github.lamspace.openlatch.protocol.AdminKeyHolderInfo;
import io.github.lamspace.openlatch.protocol.AdminKeyInfo;
import io.github.lamspace.openlatch.protocol.AdminKeyWaiterInfo;
import io.github.lamspace.openlatch.protocol.AdminListKeysRequest;
import io.github.lamspace.openlatch.protocol.AdminListKeysResponse;
import io.github.lamspace.openlatch.protocol.AdminListSessionsResponse;
import io.github.lamspace.openlatch.protocol.AdminSessionInfo;
import io.github.lamspace.openlatch.protocol.AdminSummaryResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.AdminConfig;
import io.github.lamspace.openlatch.server.OpenLatchServer;
import io.github.lamspace.openlatch.server.dispatch.RequestDispatcher;
import io.github.lamspace.openlatch.server.raft.ClusterRuntime;
import io.github.lamspace.openlatch.server.raft.LeaderTracker;
import io.github.lamspace.openlatch.server.raft.ShadowTable;
import io.github.lamspace.openlatch.server.security.ConstantTime;
import io.github.lamspace.openlatch.server.session.ServerSession;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/**
 * 管理观察请求处理器。四消息全部本地作答、只读、不产生复制
 * 日志条目——与 {@code ServerMetrics} 同构的"单组件双数据源"形态，
 * 从装配第一天就覆盖单机与集群两种形态。
 *
 * <p><b>受理门序</b>（首个不满足者即为结果，全部在管理 handler 入口、
 * 任何状态读取之前）：
 * <ol>
 *   <li>会话协商版本 ≥3，否则 {@code INVALID_REQUEST}（消息级拒绝，
 *       不断连——与 v3 新类型门控同纪律）；</li>
 *   <li>请求载荷形状合法（类型与 payload 匹配），否则
 *       {@code INVALID_REQUEST}；</li>
 *   <li>管理令牌常量时间比对（{@link ConstantTime}，防时序
 *       侧信道）：失败或未配置令牌 → {@code INVALID_REQUEST} + 断连
 *       （不泄露原因）。</li>
 * </ol>
 *
 * <p><b>数据源</b>：单机读 {@link CoreEngine} 的明细只读观察面
 * （{@code inspect}/{@code inspectKey}/{@code stats}，引擎时钟口径）；
 * 集群读本节点 {@link ShadowTable#adminEntries()} 管理投影（应用点整体
 * 重发布的弱一致镜像、逻辑会话 id 口径）+ {@code WaitQueue} 明细
 * （等待队列非复制状态、Leader 权威——follower 应答等待区恒空并置
 * {@code wait_queue_leader_only}）。{@code ADMIN_LIST_SESSIONS} 两形态
 * 均只覆盖受理节点自身接入的会话（{@link ServerSessionRegistry}），
 * 跨节点全景由控制台多节点聚合。
 *
 * <p><b>隔离保证</b>：本处理器由 {@code ServerSessionHandler} 在业务
 * 分发与 {@code ServerMetrics} 埋点之前独立早退调用——管理流量零指标
 * 污染；仍处单连接在途限额记账内。
 *
 * <p><b>线程模型</b>：{@link #handle} 在受理连接的 EventLoop 上同步执行
 * （只读观察面均为条目锁/实例锁内的短临界区拷贝，与租约扫描线程、状态机
 * 应用线程经同一互斥粒度并发安全）；MUST NOT 在观察读上做任何授予判定。
 * 应答经写完成 listener 终结在途记账；认证失败路径写完即断连。
 */
public final class AdminRequestHandler {

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(AdminRequestHandler.class);

    /** 分页页大小上限（防观察面自身成为放大攻击源）。 */
    public static final int MAX_PAGE_SIZE = 200;
    /** ADMIN 消息的最低连接协商协议版本（v3 专属语义）。 */
    public static final int MIN_ADMIN_VERSION = 3;

    /** 管理令牌配置（未配置即拒绝一切，安全默认）。 */
    private final AdminConfig config;
    /** 单机核心引擎（单机形态非空，集群形态为 {@code null}）。 */
    private final CoreEngine standaloneCore;
    /** 集群运行时（集群形态非空，单机形态为 {@code null}）。 */
    private final ClusterRuntime cluster;
    /** 本节点连接注册表（会话列表与活跃会话数来源）。 */
    private final ServerSessionRegistry sessions;
    /** 运行时长供给（毫秒），装配自 {@link OpenLatchServer#uptimeMs()}。 */
    private final LongSupplier uptimeMs;

    /**
     * 构造管理处理器。
     *
     * @param config        管理配置（令牌）
     * @param standaloneCore 单机核心引擎；集群形态传 {@code null}
     * @param cluster       集群运行时；单机形态传 {@code null}
     * @param sessions      本节点连接注册表
     * @param uptimeMs      服务器运行时长供给（毫秒）
     */
    public AdminRequestHandler(AdminConfig config, CoreEngine standaloneCore,
                               ClusterRuntime cluster, ServerSessionRegistry sessions,
                               LongSupplier uptimeMs) {
        this.config = config;
        this.standaloneCore = standaloneCore;
        this.cluster = cluster;
        this.sessions = sessions;
        this.uptimeMs = uptimeMs;
    }

    /**
     * 是否 ADMIN 消息类型（接入层路由判据）。
     *
     * @param type 信封消息类型
     * @return ADMIN 四类型之一返回 true
     */
    public static boolean isAdminType(MessageType type) {
        return switch (type) {
            case ADMIN_SUMMARY, ADMIN_LIST_KEYS, ADMIN_KEY_DETAIL, ADMIN_LIST_SESSIONS -> true;
            default -> false;
        };
    }

    /**
     * 受理一条 ADMIN 请求：按类注释门序裁决后装配应答并写回；认证失败
     * 路径写完 {@code INVALID_REQUEST} 即断连。在受理连接 EventLoop 上
     * 同步执行，写完成处终结在途记账。
     *
     * @param ctx     连接上下文
     * @param session 连接簿记（已握手）
     * @param msg     入站 ADMIN 信封
     */
    public void handle(ChannelHandlerContext ctx, ServerSession session, Envelope msg) {
        Envelope resp;
        boolean closeAfterWrite = false;
        try {
            if (session.protocolVersion() < MIN_ADMIN_VERSION) {
                // v3 专属语义的低版本连接：消息级拒绝、不断连（wire-protocol 门控）。
                resp = RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST);
            } else if (!tokenOk(msg)) {
                // 令牌缺失/为空/不符/未配置统一形态：不泄露原因，应答后即断连。
                resp = RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST);
                closeAfterWrite = true;
            } else {
                resp = switch (msg.getType()) {
                    case ADMIN_SUMMARY -> summary(msg);
                    case ADMIN_LIST_KEYS -> listKeys(msg);
                    case ADMIN_KEY_DETAIL -> keyDetail(msg);
                    case ADMIN_LIST_SESSIONS -> listSessions(msg);
                    default -> RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST);
                };
            }
        } catch (RuntimeException e) {
            // 分发兜底与业务路径同纪律：绝不静默悬挂。
            log.warn("admin dispatch failure on request {} (type {})",
                    msg.getRequestId(), msg.getType(), e);
            resp = RequestDispatcher.errorResponse(msg, StatusCode.INTERNAL_ERROR);
        }
        final boolean close = closeAfterWrite;
        ctx.writeAndFlush(resp).addListener(f -> {
            session.endRequest();
            if (close) {
                ctx.close();
            }
        });
    }

    /**
     * 令牌校验：未配置一律拒；载荷形状不符拒；比对用共享的
     * {@link ConstantTime}（等长补齐常量时间，防时序/长度侧信道）。
     *
     * @param msg ADMIN 请求信封
     * @return 校验通过返回 true
     */
    private boolean tokenOk(Envelope msg) {
        String presented = switch (msg.getType()) {
            case ADMIN_SUMMARY -> msg.hasAdminSummaryRequest()
                    ? msg.getAdminSummaryRequest().getToken() : null;
            case ADMIN_LIST_KEYS -> msg.hasAdminListKeysRequest()
                    ? msg.getAdminListKeysRequest().getToken() : null;
            case ADMIN_KEY_DETAIL -> msg.hasAdminKeyDetailRequest()
                    ? msg.getAdminKeyDetailRequest().getToken() : null;
            case ADMIN_LIST_SESSIONS -> msg.hasAdminListSessionsRequest()
                    ? msg.getAdminListSessionsRequest().getToken() : null;
            default -> null;
        };
        if (!config.isConfigured() || presented == null) {
            return false;
        }
        // 常量时间原语：与业务令牌共用单一
        // 实现（等长补齐，长度不等不早退）；评审面收敛单处。
        return ConstantTime.matches(presented, config.token());
    }

    // ===================== ADMIN_SUMMARY =====================

    /**
     * 装配摘要应答：聚合读数单机取 {@code stats()} + 屏障条目计数、
     * 集群取影子表投影计数；角色/会话数按节点视角如实呈现。
     *
     * @param msg 请求信封
     * @return 应答信封
     */
    private Envelope summary(Envelope msg) {
        AdminSummaryResponse.Builder b = AdminSummaryResponse.newBuilder()
                .setStatus(StatusCode.OK)
                .setUptimeMs(uptimeMs.getAsLong())
                .setVersion(OpenLatchServer.serverVersion())
                .setSessionCount(sessions.size());
        if (cluster == null) {
            CoreStats st = standaloneCore.stats();
            int latchEntries = 0;
            for (CoreInspection.KeySnapshot k : standaloneCore.inspect().keys()) {
                if (k.family() == KeyFamily.LATCH) {
                    latchEntries++;
                }
            }
            b.setHeldLocks(st.heldLocks()).setHeldSemaphores(st.heldSemaphores())
                    .setLatchEntries(latchEntries).setTotalWaiters(st.totalWaiters())
                    .setNodeRole("SINGLE");
        } else {
            ShadowTable shadow = cluster.core().shadow();
            int[] held = shadow.heldFamilyCounts();
            int latchEntries = 0;
            for (ShadowTable.AdminEntryView v : shadow.adminEntries().values()) {
                if (v.lockType() == LockType.LOCK_TYPE_LATCH_VALUE) {
                    latchEntries++;
                }
            }
            b.setHeldLocks(held[0]).setHeldSemaphores(held[1])
                    .setLatchEntries(latchEntries)
                    .setTotalWaiters(leaderNow() ? cluster.waitQueue().totalWaiters() : 0)
                    .setNodeRole(currentRole());
        }
        return envelope(msg, MessageType.ADMIN_SUMMARY, x -> x.setAdminSummaryResponse(b));
    }

    /**
     * 集群角色读数：本节点 nodeId 与 {@link LeaderTracker} 快照比对——
     * 选举空窗如实 {@code UNKNOWN}，MUST NOT 虚报。
     *
     * @return {@code LEADER}/{@code FOLLOWER}/{@code UNKNOWN}
     */
    private String currentRole() {
        int self = cluster.subsystem().clusterConfig().nodeId();
        LeaderTracker.Snapshot snap = cluster.leaderTracker().snapshot();
        if (snap.unknown()) {
            return "UNKNOWN";
        }
        return snap.leaderNodeId() == self ? "LEADER" : "FOLLOWER";
    }

    /**
     * 本节点是否当值 Leader（等待队列可见性门控用）：取复制网关的权威
     * 角色判定（应用事件折算），比 {@code LeaderTracker} 提示更贴近
     * "队列是否为本任期真队列"。降级残留队列随下次当选一并清除，
     * 非 Leader 一律不呈现等待数据。
     *
     * @return 集群形态且当值 Leader 返回 true
     */
    private boolean leaderNow() {
        return cluster != null && cluster.gateway().isLeaderAuthoritative();
    }

    // ===================== ADMIN_LIST_KEYS =====================

    /**
     * 装配 key 列表应答：全量视图 → 前缀过滤 → 字典序 → 切片。弱一致
     * 快照语义：并发增删 MAY 使相邻页漂移，同应答内自洽。参数越界
     * （page&lt;0、page_size∉[1,{@value #MAX_PAGE_SIZE}]）消息级拒绝。
     *
     * @param msg 请求信封
     * @return 应答信封
     */
    private Envelope listKeys(Envelope msg) {
        AdminListKeysRequest req = msg.getAdminListKeysRequest();
        if (req.getPage() < 0 || req.getPageSize() < 1 || req.getPageSize() > MAX_PAGE_SIZE) {
            return RequestDispatcher.errorResponse(msg, StatusCode.INVALID_REQUEST);
        }
        List<AdminKeyInfo> rows = new ArrayList<>();
        String prefix = req.getPrefix();
        if (cluster == null) {
            for (CoreInspection.KeySnapshot k : standaloneCore.inspect().keys()) {
                if (matches(k.key(), prefix)) {
                    rows.add(AdminKeyInfo.newBuilder()
                            .setKey(k.key()).setFamily(familyName(k.family()))
                            .setHolders(k.holders().size())
                            .setRemainingLeaseMs(k.remainingLeaseMs())
                            .setWaiterCount(k.waiters().size())
                            .build());
                }
            }
        } else {
            long now = System.currentTimeMillis();
            boolean leader = leaderNow();
            for (Map.Entry<String, ShadowTable.AdminEntryView> en
                    : cluster.core().shadow().adminEntries().entrySet()) {
                if (!matches(en.getKey(), prefix)) {
                    continue;
                }
                ShadowTable.AdminEntryView v = en.getValue();
                rows.add(AdminKeyInfo.newBuilder()
                        .setKey(en.getKey()).setFamily(familyNameOfLockType(v.lockType()))
                        .setHolders(v.holders().size())
                        .setRemainingLeaseMs(v.leaseToken() != 0
                                ? Math.max(0, v.expiresAtMs() - now) : 0)
                        .setWaiterCount(leader ? cluster.waitQueue().waitCount(en.getKey()) : 0)
                        .build());
            }
        }
        rows.sort(Comparator.comparing(AdminKeyInfo::getKey));
        // 页号乘法经 long 折算防 int 溢出（越界页号按空页语义呈现，不抛异常）。
        long fromL = (long) req.getPage() * req.getPageSize();
        int from = (int) Math.min(Math.max(fromL, 0L), rows.size());
        int to = (int) Math.min((long) from + req.getPageSize(), rows.size());
        List<AdminKeyInfo> page = rows.subList(from, to);
        AdminListKeysResponse resp = AdminListKeysResponse.newBuilder()
                .setStatus(StatusCode.OK)
                .setTotalMatched(rows.size())
                .setPage(req.getPage())
                .setPageSize(req.getPageSize())
                .addAllItems(page)
                .build();
        return envelope(msg, MessageType.ADMIN_LIST_KEYS, x -> x.setAdminListKeysResponse(resp));
    }

    /**
     * 前缀过滤判定：{@code prefix} 空串不过滤（proto3 缺省语义）。
     *
     * @param key    锁键
     * @param prefix 前缀
     * @return 命中返回 true
     */
    private static boolean matches(String key, String prefix) {
        return prefix.isEmpty() || key.startsWith(prefix);
    }

    // ===================== ADMIN_KEY_DETAIL =====================

    /**
     * 装配单 key 明细应答：未命中回 {@code NOT_HELD} 形态的完整明细响应
     * （MUST NOT 空壳成功）；等待队列 Leader 权威、follower 置
     * {@code wait_queue_leader_only}。
     *
     * @param msg 请求信封
     * @return 应答信封
     */
    private Envelope keyDetail(Envelope msg) {
        AdminKeyDetailRequest req = msg.getAdminKeyDetailRequest();
        AdminKeyDetailResponse.Builder b = AdminKeyDetailResponse.newBuilder();
        if (cluster == null) {
            CoreInspection.KeySnapshot snap = standaloneCore.inspectKey(req.getKey());
            if (snap == null) {
                return envelope(msg, MessageType.ADMIN_KEY_DETAIL, x -> x.setAdminKeyDetailResponse(
                        b.setStatus(StatusCode.NOT_HELD)));
            }
            b.setStatus(StatusCode.OK).setFamily(familyName(snap.family()))
                    .setLeaseExpiresAtMs(snap.leaseExpiresAtMs())
                    .setRemainingLeaseMs(snap.remainingLeaseMs())
                    .setPermitsTotal(snap.permitsTotal())
                    .setPermitsAvailable(snap.permitsAvailable())
                    .setLatchTotal(snap.latchTotal())
                    .setLatchRemaining(snap.latchRemaining());
            for (CoreInspection.HolderSnapshot h : snap.holders()) {
                b.addHolders(AdminKeyHolderInfo.newBuilder()
                        .setSessionId(h.sessionId()).setThreadId(h.threadId())
                        .setCount(h.count()).setRole(roleName(h.role())).build());
            }
            int position = 0;
            for (CoreInspection.WaiterSnapshot w : snap.waiters()) {
                b.addWaiters(AdminKeyWaiterInfo.newBuilder()
                        .setPosition(++position).setSessionId(w.sessionId())
                        .setRequestId(w.requestId()).setPermits(w.permits())
                        .setWaitedMs(w.waitedMs()).setNotified(w.notified()).build());
            }
        } else {
            ShadowTable.AdminEntryView v = cluster.core().shadow().adminEntry(req.getKey());
            if (v == null) {
                return envelope(msg, MessageType.ADMIN_KEY_DETAIL, x -> x.setAdminKeyDetailResponse(
                        b.setStatus(StatusCode.NOT_HELD)));
            }
            long now = System.currentTimeMillis();
            b.setStatus(StatusCode.OK).setFamily(familyNameOfLockType(v.lockType()))
                    .setLeaseExpiresAtMs(v.expiresAtMs())
                    .setRemainingLeaseMs(v.leaseToken() != 0
                            ? Math.max(0, v.expiresAtMs() - now) : 0)
                    .setPermitsTotal(v.permitsTotal()).setPermitsAvailable(v.permitsAvailable())
                    .setLatchTotal(v.latchTotal()).setLatchRemaining(v.latchCount());
            for (Map.Entry<ShadowTable.Holder, Integer> h : v.holders().entrySet()) {
                b.addHolders(AdminKeyHolderInfo.newBuilder()
                        .setSessionId(h.getKey().sessionId()).setThreadId(h.getKey().threadId())
                        .setCount(h.getValue()).setRole(clusterRoleName(v.lockType())).build());
            }
            boolean leader = leaderNow();
            b.setWaitQueueLeaderOnly(!leader);
            if (leader) {
                for (var w : cluster.waitQueue().keyWaiters(req.getKey(), now)) {
                    b.addWaiters(AdminKeyWaiterInfo.newBuilder()
                            .setPosition(w.position()).setSessionId(w.sessionId())
                            .setRequestId(w.requestId()).setPermits(w.permits())
                            .setWaitedMs(w.waitedMs()).setNotified(w.notified()).build());
                }
            }
        }
        return envelope(msg, MessageType.ADMIN_KEY_DETAIL, x -> x.setAdminKeyDetailResponse(b));
    }

    // ===================== ADMIN_LIST_SESSIONS =====================

    /**
     * 装配本节点接入会话列表：仅受理节点自身接入的会话（跨节点全景由
     * 控制台聚合）；持锁数/等待数按受理节点数据源折算，字典序按会话 id
     * 稳定呈现。
     *
     * @param msg 请求信封
     * @return 应答信封
     */
    private Envelope listSessions(Envelope msg) {
        List<ServerSession> connected = new ArrayList<>(sessions.snapshot());
        connected.sort(Comparator.comparingLong(ServerSession::sessionId));
        // 观察一次即可：两形态都以"会话 → key 集"倒排聚合，避免逐会话全表扫描。
        Map<Long, Integer> heldBySession = new HashMap<>();
        Map<Long, Integer> waitingBySession = new HashMap<>();
        if (cluster == null) {
            for (CoreInspection.KeySnapshot k : standaloneCore.inspect().keys()) {
                for (CoreInspection.HolderSnapshot h : k.holders()) {
                    heldBySession.merge(h.sessionId(), 1, Integer::sum);
                }
                // 同 key 上同会话的多个等待请求只计一个"在等 key"。
                Set<Long> seen = new HashSet<>();
                for (CoreInspection.WaiterSnapshot w : k.waiters()) {
                    if (seen.add(w.sessionId())) {
                        waitingBySession.merge(w.sessionId(), 1, Integer::sum);
                    }
                }
            }
        } else {
            for (Map.Entry<String, ShadowTable.AdminEntryView> en
                    : cluster.core().shadow().adminEntries().entrySet()) {
                for (ShadowTable.Holder h : en.getValue().holders().keySet()) {
                    heldBySession.merge(h.sessionId(), 1, Integer::sum);
                }
            }
            if (leaderNow()) {
                waitingBySession.putAll(cluster.waitQueue().waitCountsBySession());
            }
        }
        AdminListSessionsResponse.Builder b = AdminListSessionsResponse.newBuilder()
                .setStatus(StatusCode.OK);
        for (ServerSession s : connected) {
            long sid = s.sessionId();
            // 接入节点：逻辑会话 id 高位（nodeId<<32|localSeq 编码约定）；
            // 单机形态无集群身份恒 0。
            long nodeId = cluster == null ? 0 : sid >>> 32;
            b.addSessions(AdminSessionInfo.newBuilder()
                    .setSessionId(sid)
                    .setNodeId(nodeId)
                    .setConnectedAtMs(s.connectedAtMs())
                    .setHeldKeys(heldBySession.getOrDefault(sid, 0))
                    .setWaitingKeys(waitingBySession.getOrDefault(sid, 0))
                    .build());
        }
        return envelope(msg, MessageType.ADMIN_LIST_SESSIONS, x -> x.setAdminListSessionsResponse(b));
    }

    // ===================== 词汇与信封 =====================

    /**
     * 家族词表（与指标 {@code locks.held} 的 type 标签同一命名点口径）。
     *
     * @param family 条目家族
     * @return {@code lock}/{@code semaphore}/{@code latch}
     */
    private static String familyName(KeyFamily family) {
        return switch (family) {
            case LOCK -> "lock";
            case SEMAPHORE -> "semaphore";
            case LATCH -> "latch";
        };
    }

    /**
     * 协议锁类型数值 → 家族词表（集群镜像侧）。
     *
     * @param lockTypeValue {@code LockType} 数值
     * @return 家族名
     */
    private static String familyNameOfLockType(int lockTypeValue) {
        if (lockTypeValue == LockType.LOCK_TYPE_SEMAPHORE_VALUE) {
            return "semaphore";
        }
        if (lockTypeValue == LockType.LOCK_TYPE_LATCH_VALUE) {
            return "latch";
        }
        return "lock";
    }

    /**
     * 单机侧持有角色词表映射。
     *
     * @param role core 角色判别
     * @return {@code writer}/{@code reader}/{@code holder}
     */
    private static String roleName(CoreInspection.HolderRole role) {
        return switch (role) {
            case WRITER -> "writer";
            case READER -> "reader";
            case HOLDER -> "holder";
        };
    }

    /**
     * 集群侧持有角色词表映射：影子表不区分写侧/读侧归属（条目定型类型
     * 是唯一依据）——READ 定型条目的持有者报 {@code reader}，
     * Semaphore 报 {@code holder}，其余锁类型报 {@code writer}
     * （观察近似口径，明细真相在 Leader 引擎，弱一致镜像呈现）。
     *
     * @param lockTypeValue 条目定型锁类型数值
     * @return 角色词
     */
    private static String clusterRoleName(int lockTypeValue) {
        if (lockTypeValue == LockType.LOCK_TYPE_SEMAPHORE_VALUE) {
            return "holder";
        }
        if (lockTypeValue == LockType.LOCK_TYPE_READ_VALUE) {
            return "reader";
        }
        return "writer";
    }

    /**
     * 应答信封构造：回显请求的 requestId 与协商协议版本（与业务应答
     * 同一回显纪律）。
     *
     * @param request 原请求信封
     * @param type    应答类型（恒等于请求类型）
     * @param fill    载荷填充
     * @return 应答信封
     */
    private static Envelope envelope(Envelope request, MessageType type,
                                     Consumer<Envelope.Builder> fill) {
        Envelope.Builder b = Envelope.newBuilder()
                .setProtocolVersion(request.getProtocolVersion())
                .setType(type)
                .setRequestId(request.getRequestId());
        fill.accept(b);
        return b.build();
    }
}
