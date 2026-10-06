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
 * {@code wait_queue_leader_only}）。v9 条件等待读数两形态分源：单机读
 * core 条目内条件等待集（{@code conditionWaiterCount}/{@code conditionWaiters}，
 * 集内等待者使条目存活故行可见、持有读数如实无持有者）；集群读 Leader 本地
 * {@code ConditionRegistry}（计数与明细仅 Leader 呈现，follower 恒零/空并随
 * {@code wait_queue_leader_only} 同源标注——等待集不入复制态，如实零读），
 * 且条件字段只挂 {@code family=lock} 行/详情（非 LOCK 键恒缺省零值/空列表，
 * 条件骑 LOCK 家族）。{@code ADMIN_LIST_SESSIONS} 两形态
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
    /** v8 单机形态 topic 登记表（集群形态为 {@code null}，经运行时取）。 */
    private final io.github.lamspace.openlatch.server.topic.TopicRegistry standaloneTopics;

    /**
     * 构造管理处理器（既有五参形态：无 topic 面，订阅维恒零）。
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
        this(config, standaloneCore, cluster, sessions, uptimeMs, null);
    }

    /**
     * 构造管理处理器（v8 全参形态）。
     *
     * @param config         管理配置（令牌）
     * @param standaloneCore 单机核心引擎；集群形态传 {@code null}
     * @param cluster        集群运行时；单机形态传 {@code null}
     * @param sessions       本节点连接注册表
     * @param uptimeMs       服务器运行时长供给（毫秒）
     * @param standaloneTopics 单机形态 topic 登记表；集群形态传 {@code null}
     *                        （数据源经 {@code ClusterRuntime.topicRegistry()}）
     */
    public AdminRequestHandler(AdminConfig config, CoreEngine standaloneCore,
                               ClusterRuntime cluster, ServerSessionRegistry sessions,
                               LongSupplier uptimeMs,
                               io.github.lamspace.openlatch.server.topic.TopicRegistry standaloneTopics) {
        this.config = config;
        this.standaloneCore = standaloneCore;
        this.cluster = cluster;
        this.sessions = sessions;
        this.uptimeMs = uptimeMs;
        this.standaloneTopics = standaloneTopics;
    }

    /**
     * topic 登记表数据源（单机=自持；集群=运行时持有）与可见性判定：
     * 订阅登记为 Leader 本地态，集群形态非 Leader MUST NOT 呈现（读数
     * 口径判例 {@code wait_queue_leader_only}）。
     *
     * @return 可呈现时的登记表；不可见为 {@code null}
     */
    private io.github.lamspace.openlatch.server.topic.TopicRegistry topicsVisible() {
        if (cluster == null) {
            return standaloneTopics;
        }
        return leaderNow() ? cluster.topicRegistry() : null;
    }

    /**
     * 订阅者明细 → 管理应答列表（会话 id、路由键、建立时刻；已交付
     * 消息内容与字节零外发——观察面防放大纪律的 topic 延伸）。
     *
     * @param topics 登记表（非空）
     * @param key    topic 键
     * @return 应答项列表（未登记键为空列表）
     */
    private static java.util.List<io.github.lamspace.openlatch.protocol.AdminTopicSubscriberInfo>
            topicSubscriberInfos(io.github.lamspace.openlatch.server.topic.TopicRegistry topics,
                                 String key) {
        java.util.List<io.github.lamspace.openlatch.protocol.AdminTopicSubscriberInfo> out =
                new ArrayList<>();
        for (var sv : topics.subscribers(key)) {
            out.add(io.github.lamspace.openlatch.protocol.AdminTopicSubscriberInfo.newBuilder()
                    .setSessionId(sv.sessionId()).setSubscriptionId(sv.subscriptionId())
                    .setSubscribedAtMs(sv.subscribedAtMs()).build());
        }
        return out;
    }

    /**
     * v9 条件等待明细五字段装配（{@code {condition, session_id, request_id,
     * thread_id, registered_at_ms}}）：单机条目视图与集群 Leader 登记表视图
     * 两数据源共用同一构装口径——条件维无内容面可外发，条件名即寻址文本，
     * 明细仅结构五字段（观察面防放大纪律的条件延伸）。
     *
     * @param condition       条件名（命名寻址）
     * @param sessionId       逻辑会话 id
     * @param requestId       折叠 ACQUIRE 的请求 id
     * @param threadId        归属线程 id
     * @param registeredAtMs  登记时刻（epoch 毫秒，受理节点应用时钟）
     * @return 明细应答项
     */
    private static io.github.lamspace.openlatch.protocol.AdminConditionWaiterInfo
            conditionWaiterInfo(String condition, long sessionId, long requestId,
                                long threadId, long registeredAtMs) {
        return io.github.lamspace.openlatch.protocol.AdminConditionWaiterInfo.newBuilder()
                .setCondition(condition).setSessionId(sessionId).setRequestId(requestId)
                .setThreadId(threadId).setRegisteredAtMs(registeredAtMs).build();
    }

    /**
     * v8：topic 键的明细应答（{@code family=topic}）——仅订阅数与订阅者
     * 列表两维；无持有/等待/租约区段（"无此语义"以零值形呈现，消费者侧
     * 按 family 分派渲染）。已交付消息内容 MUST NOT 出现在应答中。
     *
     * @param msg    请求信封
     * @param topics 登记表（调用方保证可见性）
     * @param key    topic 键
     * @return 应答信封
     */
    private Envelope topicDetail(Envelope msg,
            io.github.lamspace.openlatch.server.topic.TopicRegistry topics, String key) {
        io.github.lamspace.openlatch.protocol.AdminKeyDetailResponse resp =
                io.github.lamspace.openlatch.protocol.AdminKeyDetailResponse.newBuilder()
                        .setStatus(StatusCode.OK).setFamily("topic")
                        .setTopicSubscribers(topics.subscriberCount(key))
                        .addAllTopicSubscribersInfo(topicSubscriberInfos(topics, key))
                        .build();
        return envelope(msg, MessageType.ADMIN_KEY_DETAIL, x -> x.setAdminKeyDetailResponse(resp));
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
     * 装配摘要应答：聚合读数单机取 {@code stats()} + 屏障/原子条目计数、
     * 集群取影子表投影计数；角色/会话数按节点视角如实呈现。等待者总数
     * v9 口径含条件等待者——单机 {@code stats().totalWaiters()} 天然计入
     * 条目条件集；集群 Leader 取等待队列合计 + {@code ConditionRegistry}
     * 在册人数加数（Follower 无队列与集合来源，恒 0 如实，判例 topic 订阅
     * 者不计入的相反口径：条件等待者是等待者）。
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
            int atomicEntries = 0;
            int barrierEntries = 0;
            int queueEntries = 0;
            for (CoreInspection.KeySnapshot k : standaloneCore.inspect().keys()) {
                if (k.family() == KeyFamily.LATCH) {
                    latchEntries++;
                } else if (k.family() == KeyFamily.ATOMIC) {
                    atomicEntries++;
                } else if (k.family() == KeyFamily.BARRIER) {
                    barrierEntries++;
                } else if (k.family() == KeyFamily.QUEUE) {
                    queueEntries++;
                }
            }
            b.setHeldLocks(st.heldLocks()).setHeldSemaphores(st.heldSemaphores())
                    .setLatchEntries(latchEntries).setAtomicEntries(atomicEntries)
                    .setBarrierEntries(barrierEntries).setQueueEntries(queueEntries)
                    // v8：订阅登记键数（Leader/单机视角；无持有语义单列）。
                    .setTopicEntries(standaloneTopics == null ? 0
                            : standaloneTopics.topicKeyCount())
                    .setTotalWaiters(st.totalWaiters())
                    .setNodeRole("SINGLE");
        } else {
            ShadowTable shadow = cluster.core().shadow();
            int[] held = shadow.heldFamilyCounts();
            int latchEntries = 0;
            int atomicEntries = 0;
            int barrierEntries = 0;
            int queueEntries = 0;
            for (ShadowTable.AdminEntryView v : shadow.adminEntries().values()) {
                if (v.lockType() == LockType.LOCK_TYPE_LATCH_VALUE) {
                    latchEntries++;
                } else if (ShadowTable.isAtomicFamily(v.lockType())) {
                    // v6：有值引用条目并入 ATOMIC 家族单列计数（per-kind 区分
                    // 由指标 kind 标签承担，不扩计数线）。
                    atomicEntries++;
                } else if (v.lockType() == LockType.LOCK_TYPE_BARRIER_VALUE) {
                    barrierEntries++;
                } else if (ShadowTable.isQueueType(v.lockType())) {
                    // v7：队列两形态合并单列（判例 latch/atomic/barrier）。
                    queueEntries++;
                }
            }
            b.setHeldLocks(held[0]).setHeldSemaphores(held[1])
                    .setLatchEntries(latchEntries).setAtomicEntries(atomicEntries)
                    .setBarrierEntries(barrierEntries).setQueueEntries(queueEntries)
                    // v8：订阅登记键数仅 Leader 视角呈现（降级残留随下次当选
                    // 一并清零，非 Leader 恒 0——判例等待队列 Leader 门控）。
                    .setTopicEntries(leaderNow() ? cluster.topicRegistry().topicKeyCount() : 0)
                    // v9 等待者总数口径：等待队列合计 + 条件等待集在册人数
                    //（Leader 本地登记表读数；Follower 两源皆无、恒 0 如实；
                    // 单机侧 stats().totalWaiters() 已含条件集，两形态同口径）。
                    .setTotalWaiters(leaderNow()
                            ? cluster.waitQueue().totalWaiters()
                                    + cluster.conditionRegistry().totalCount() : 0)
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
     * LOCK 行另列 {@code condition_waiters}（v9：单机读条目条件集、集群
     * Leader 读本地登记表、Follower 恒 0 如实；已搬运入队项归
     * {@code waiterCount} 口径不重复计数；非 LOCK 行恒缺省零值）。
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
                    AdminKeyInfo.Builder row = AdminKeyInfo.newBuilder()
                            .setKey(k.key()).setFamily(familyName(k.family()))
                            .setHolders(k.holders().size())
                            .setRemainingLeaseMs(k.remainingLeaseMs())
                            .setWaiterCount(k.waiters().size());
                    if (k.family() == KeyFamily.LOCK) {
                        // v9：LOCK 行条件等待数读条目条件集（未搬运项口径；
                        // 已搬运入队项归 waiterCount 等待队列口径，不重复计）。
                        row.setConditionWaiters(standaloneCore.conditionWaiterCount(k.key()));
                    }
                    if (k.family() == KeyFamily.ATOMIC) {
                        // v4：原子行呈现形态与当前值（holders/租约/等待恒零）。
                        row.setAtomicKind(atomicKindNameOfCore(k.atomicKind()))
                                .setAtomicValue(k.atomicValue());
                        if (k.atomicKind()
                                == io.github.lamspace.openlatch.core.LockType.ATOMIC_REFERENCE) {
                            // v6：引用行以大小+截断预览承载（全量载荷零外发）。
                            row.setAtomicPayloadSize(
                                            k.atomicRefValue() == null ? 0
                                                    : k.atomicRefValue().length)
                                    .setAtomicPayloadPreview(payloadPreview(k.atomicRefValue()));
                        }
                    } else if (k.family() == KeyFamily.BARRIER) {
                        // v5：屏障行呈现 parties/世代/到场数（holders/租约恒零）。
                        row.setBarrierParties(k.barrierParties())
                                .setBarrierGeneration(k.barrierGeneration())
                                .setBarrierArrived(k.barrierArrived());
                    } else if (k.family() == KeyFamily.QUEUE) {
                        // v7：队列行呈现容量/深度/首元素大小+截断预览
                        // （全量元素零外发）。
                        row.setQueueCapacity(k.queueCapacity())
                                .setQueueDepth(k.queueDepth())
                                .setQueueHeadPayloadSize(
                                        k.queueHeadPayload() == null
                                                ? 0 : k.queueHeadPayload().length)
                                .setQueueHeadPayloadPreview(payloadPreview(k.queueHeadPayload()));
                    }
                    rows.add(row.build());
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
                String family = familyNameOfLockType(v.lockType());
                AdminKeyInfo.Builder row = AdminKeyInfo.newBuilder()
                        .setKey(en.getKey()).setFamily(family)
                        .setHolders(v.holders().size())
                        .setRemainingLeaseMs(v.leaseToken() != 0
                                ? Math.max(0, v.expiresAtMs() - now) : 0)
                        .setWaiterCount(leader ? cluster.waitQueue().waitCount(en.getKey()) : 0);
                if ("lock".equals(family)) {
                    // v9：LOCK 行另列条件等待数——Leader 读本地登记表合计，
                    // Follower 无集合来源恒 0 如实（等待集不入复制态；已搬运
                    // 入队项归 waiterCount 口径、不在本列重复计数）。
                    row.setConditionWaiters(
                            leader ? cluster.conditionRegistry().count(en.getKey()) : 0);
                }
                if (ShadowTable.isAtomicType(v.lockType())) {
                    row.setAtomicKind(atomicKindNameOf(v.lockType()))
                            .setAtomicValue(v.atomicValue());
                } else if (ShadowTable.isReferenceType(v.lockType())) {
                    // v6：引用行以大小+截断预览承载（全量载荷零外发）。
                    row.setAtomicKind(atomicKindNameOf(v.lockType()))
                            .setAtomicPayloadSize(
                                    v.refValue() == null ? 0 : v.refValue().length)
                            .setAtomicPayloadPreview(payloadPreview(v.refValue()));
                } else if (v.lockType() == LockType.LOCK_TYPE_BARRIER_VALUE) {
                    row.setBarrierParties(v.barrierParties())
                            .setBarrierGeneration(v.barrierGeneration())
                            .setBarrierArrived(v.barrierArrived());
                } else if (ShadowTable.isQueueType(v.lockType())) {
                    // v7：队列行（容量/深度/首元素大小+截断预览，全量元素零外发）。
                    row.setQueueCapacity(v.queueCapacity())
                            .setQueueDepth(v.queueDepth())
                            .setQueueHeadPayloadSize(
                                    v.queueHeadPayload() == null
                                            ? 0 : v.queueHeadPayload().length)
                            .setQueueHeadPayloadPreview(payloadPreview(v.queueHeadPayload()));
                }
                rows.add(row.build());
            }
        }
        // v8：Leader/单机视角合并订阅登记键（family=topic；holders/租约/等待
        // 恒零值形）。非 Leader 无登记来源，列表如实不含 topic 行——不呈现
        // 伪零行（与"未知 key 明确未命中"同纪律）。
        io.github.lamspace.openlatch.server.topic.TopicRegistry topics = topicsVisible();
        if (topics != null) {
            for (String tkey : topics.topicKeys()) {
                if (matches(tkey, prefix)) {
                    rows.add(AdminKeyInfo.newBuilder()
                            .setKey(tkey).setFamily("topic")
                            .setTopicSubscribers(topics.subscriberCount(tkey))
                            .build());
                }
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
     * {@code wait_queue_leader_only}。v9 条件等待明细与等待队列区段并列：
     * 单机读条目条件集（集内等待者使条目存活，故 LOCK 行在"持有已随 await
     * 释放"形态下仍可见、持有读数如实无持有者）；集群读 Leader 本地登记表、
     * follower 计数与明细恒零/空并随同一 {@code wait_queue_leader_only}
     * 标注（等待集不入复制态——如实零读）；非 LOCK 家族键两字段恒缺省
     * 零值/空列表；搬运入队项只在等待队列区段计位次、不在条件区段重复。
     * 观察 MUST NOT 推进登记、搬运、通知或清扫时序（只读快照）。
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
                // v8：引擎无条目时回查 topic 登记（一 key 一形态的互斥呈现——
                // topic 键不在 LockTable，明细来自 Leader/单机本地登记表）。
                var topics = topicsVisible();
                if (topics != null && topics.subscriberCount(req.getKey()) > 0) {
                    return topicDetail(msg, topics, req.getKey());
                }
                return envelope(msg, MessageType.ADMIN_KEY_DETAIL, x -> x.setAdminKeyDetailResponse(
                        b.setStatus(StatusCode.NOT_HELD)));
            }
            b.setStatus(StatusCode.OK).setFamily(familyName(snap.family()))
                    .setLeaseExpiresAtMs(snap.leaseExpiresAtMs())
                    .setRemainingLeaseMs(snap.remainingLeaseMs())
                    .setPermitsTotal(snap.permitsTotal())
                    .setPermitsAvailable(snap.permitsAvailable())
                    .setLatchTotal(snap.latchTotal())
                    .setLatchRemaining(snap.latchRemaining())
                    .setAtomicKind(atomicKindNameOfCore(snap.atomicKind()))
                    .setAtomicInitial(snap.atomicInitial())
                    .setAtomicValue(snap.atomicValue())
                    .setAtomicVersion(snap.atomicVersion());
            if (snap.family() == KeyFamily.BARRIER) {
                b.setBarrierParties(snap.barrierParties())
                        .setBarrierGeneration(snap.barrierGeneration())
                        .setBarrierArrived(snap.barrierArrived())
                        .setBarrierActionPending(snap.barrierActionPending())
                        .setBarrierLastFinal(snap.barrierLastFinal() == null
                                ? "none" : barrierFinalWord(snap.barrierLastFinal().ordinal()));
            }
            if (snap.family() == KeyFamily.ATOMIC && snap.atomicKind()
                    == io.github.lamspace.openlatch.core.LockType.ATOMIC_REFERENCE) {
                // v6：引用明细——标量初值/值位恒 0（上方已装配），载荷读数走
                // 大小+截断预览对；初值观察不入载荷字段（定型语义由值态承载）。
                b.setAtomicPayloadSize(snap.atomicRefValue() == null
                                ? 0 : snap.atomicRefValue().length)
                        .setAtomicPayloadPreview(payloadPreview(snap.atomicRefValue()));
            }
            if (snap.family() == KeyFamily.QUEUE) {
                // v7：队列明细——容量/深度/驻留字节/队首到期与首元素截断预览；
                // 全量元素列表零外发（观察面防放大纪律同 v6 载荷对）。
                b.setQueueCapacity(snap.queueCapacity())
                        .setQueueDepth(snap.queueDepth())
                        .setQueueHeadExpiryMs(snap.queueHeadExpiryMs())
                        .setQueueTotalPayloadBytes(snap.queueTotalPayloadBytes())
                        .setQueueHeadPayloadSize(
                                snap.queueHeadPayload() == null
                                        ? 0 : snap.queueHeadPayload().length)
                        .setQueueHeadPayloadPreview(payloadPreview(snap.queueHeadPayload()));
            }
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
                        .setWaitedMs(w.waitedMs()).setNotified(w.notified())
                            // v7：队列轨道判别（1=等容量/2=等元素；非队列 0）。
                            .setQueueTrack(w.track()).build());
            }
            if (snap.family() == KeyFamily.LOCK) {
                // v9：LOCK 键条件等待明细（条目锁内只读快照，集建立序→
                // 集内到达序；与等待队列区段并列——已搬运入队项只在上方
                // waiters 区段呈现，两区互斥计数不重复；非 LOCK 键恒零/空）。
                for (io.github.lamspace.openlatch.core.lock.LockEntry.ConditionWaiterView cw
                        : standaloneCore.conditionWaiters(req.getKey())) {
                    b.addConditionWaitersInfo(conditionWaiterInfo(cw.condition(),
                            cw.sessionId(), cw.requestId(), cw.threadId(),
                            cw.registeredAtMs()));
                }
                b.setConditionWaiters(standaloneCore.conditionWaiterCount(req.getKey()));
            }
        } else {
            ShadowTable.AdminEntryView v = cluster.core().shadow().adminEntry(req.getKey());
            if (v == null) {
                // v8：复制态无条目时回查 Leader 本地登记（非 Leader 不可见，
                // topic 键在其视角如实未命中——MUST NOT 空壳成功）。
                var topics = topicsVisible();
                if (topics != null && topics.subscriberCount(req.getKey()) > 0) {
                    return topicDetail(msg, topics, req.getKey());
                }
                return envelope(msg, MessageType.ADMIN_KEY_DETAIL, x -> x.setAdminKeyDetailResponse(
                        b.setStatus(StatusCode.NOT_HELD)));
            }
            long now = System.currentTimeMillis();
            b.setStatus(StatusCode.OK).setFamily(familyNameOfLockType(v.lockType()))
                    .setLeaseExpiresAtMs(v.expiresAtMs())
                    .setRemainingLeaseMs(v.leaseToken() != 0
                            ? Math.max(0, v.expiresAtMs() - now) : 0)
                    .setPermitsTotal(v.permitsTotal()).setPermitsAvailable(v.permitsAvailable())
                    .setLatchTotal(v.latchTotal()).setLatchRemaining(v.latchCount())
                    .setAtomicKind(atomicKindNameOf(v.lockType()))
                    .setAtomicInitial(v.atomicInitial())
                    .setAtomicValue(v.atomicValue())
                    .setAtomicVersion(v.atomicVersion());
            if (ShadowTable.isReferenceType(v.lockType())) {
                // v6：引用明细——大小+截断预览对（全量载荷零外发）。
                b.setAtomicPayloadSize(v.refValue() == null ? 0 : v.refValue().length)
                        .setAtomicPayloadPreview(payloadPreview(v.refValue()));
            }
            if (v.lockType() == LockType.LOCK_TYPE_BARRIER_VALUE) {
                b.setBarrierParties(v.barrierParties())
                        .setBarrierGeneration(v.barrierGeneration())
                        .setBarrierArrived(v.barrierArrived())
                        .setBarrierActionPending(v.barrierActionPending())
                        .setBarrierLastFinal(v.barrierCompletedResult() == 0
                                ? "none" : barrierFinalWord(v.barrierCompletedResult() - 1));
            }
            if (ShadowTable.isQueueType(v.lockType())) {
                // v7：队列明细（复制态镜像读数；首元素到期为条目时刻口径，
                // 全量元素零外发）。
                b.setQueueCapacity(v.queueCapacity())
                        .setQueueDepth(v.queueDepth())
                        .setQueueHeadExpiryMs(v.queueHeadExpiryMs())
                        .setQueueTotalPayloadBytes(v.queueTotalPayloadBytes())
                        .setQueueHeadPayloadSize(
                                v.queueHeadPayload() == null
                                        ? 0 : v.queueHeadPayload().length)
                        .setQueueHeadPayloadPreview(payloadPreview(v.queueHeadPayload()));
            }
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
                            .setWaitedMs(w.waitedMs()).setNotified(w.notified())
                            // v7：队列轨道判别（1=等容量/2=等元素；非队列 0）。
                            .setQueueTrack(w.track()).build());
                }
                if ("lock".equals(familyNameOfLockType(v.lockType()))) {
                    // v9：LOCK 键条件等待明细——Leader 本地登记表视图
                    //（key 建立序→集内到达序）；与等待队列区段并列、
                    // 搬运入队项不重复计数。非 Leader 分支不填两字段
                    //（proto 缺省零值/空列表即如实零读，随上方
                    // wait_queue_leader_only 同源标注）。
                    io.github.lamspace.openlatch.server.condition.ConditionRegistry
                            registry = cluster.conditionRegistry();
                    b.setConditionWaiters(registry.count(req.getKey()));
                    for (var cv : registry.views(req.getKey())) {
                        b.addConditionWaitersInfo(conditionWaiterInfo(cv.condition(),
                                cv.sessionId(), cv.requestId(), cv.threadId(),
                                cv.registeredAtMs()));
                    }
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
     * @return {@code lock}/{@code semaphore}/{@code latch}/{@code atomic}
     */
    private static String familyName(KeyFamily family) {
        return switch (family) {
            case LOCK -> "lock";
            case SEMAPHORE -> "semaphore";
            case LATCH -> "latch";
            case ATOMIC -> "atomic";
            case BARRIER -> "barrier";
            case QUEUE -> "queue";
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
        if (ShadowTable.isAtomicFamily(lockTypeValue)) {
            return "atomic";
        }
        if (lockTypeValue == LockType.LOCK_TYPE_BARRIER_VALUE) {
            return "barrier";
        }
        if (ShadowTable.isQueueType(lockTypeValue)) {
            return "queue";
        }
        return "lock";
    }

    /**
     * 协议形态数值 → 原子形态词表（管理观察面：long/integer/boolean/
     * reference；非原子数值回空串——proto3 缺省即"不适用"）。
     *
     * @param lockTypeValue {@code LockType} 数值
     * @return 形态词或空串
     */
    private static String atomicKindNameOf(int lockTypeValue) {
        if (lockTypeValue == LockType.LOCK_TYPE_ATOMIC_LONG_VALUE) {
            return "long";
        }
        if (lockTypeValue == LockType.LOCK_TYPE_ATOMIC_INTEGER_VALUE) {
            return "integer";
        }
        if (lockTypeValue == LockType.LOCK_TYPE_ATOMIC_BOOLEAN_VALUE) {
            return "boolean";
        }
        if (lockTypeValue == LockType.LOCK_TYPE_ATOMIC_REFERENCE_VALUE) {
            return "reference";
        }
        return "";
    }

    /**
     * core 形态枚举 → 原子形态词表（单机观察侧；null=非原子条目回空串）。
     *
     * @param kind core 形态判别
     * @return 形态词或空串
     */
    private static String atomicKindNameOfCore(io.github.lamspace.openlatch.core.LockType kind) {
        if (kind == null) {
            return "";
        }
        return switch (kind) {
            case ATOMIC_LONG -> "long";
            case ATOMIC_INTEGER -> "integer";
            case ATOMIC_BOOLEAN -> "boolean";
            case ATOMIC_REFERENCE -> "reference";
            default -> "";
        };
    }

    /**
     * 有值引用载荷的截断转义预览（v6，管理观察面）：至多 64 字节前缀，
     * 可打印 ASCII（0x20–0x7e）原样、其余 {@code \xHH} 转义；预览长度恒定
     * （不随 {@code maxValueBytes} 或实际载荷膨胀），超长以 {@code …} 结尾。
     * {@code null}（null 态）回空串，零长度（空字节串）回 {@code ""}——
     * 两态在预览上亦可区分。全量载荷 MUST NOT 出现在应答中。
     *
     * @param value 载荷字节（可 {@code null}）
     * @return 预览字符串
     */
    private static String payloadPreview(byte[] value) {
        if (value == null) {
            return "";
        }
        if (value.length == 0) {
            return "\"\"";
        }
        int limit = Math.min(value.length, 64);
        StringBuilder sb = new StringBuilder(limit + 8);
        for (int i = 0; i < limit; i++) {
            int b = value[i] & 0xff;
            if (b >= 0x20 && b <= 0x7e && b != '\\') {
                sb.append((char) b);
            } else {
                sb.append(String.format("\\x%02x", b));
            }
        }
        if (value.length > limit) {
            sb.append('…');
        }
        return sb.toString();
    }

    /**
     * 屏障了结形态词表（管理观察面：tripped/broken；0=无完结记录回
     * {@code none}，越界数值回空串——proto3 缺省即"不适用"）。
     * 编码口径：{@code BarrierFinal.ordinal()}（0=TRIPPED、1=BROKEN）与
     * 影子表镜像存储值（1=TRIPPED、2=BROKEN）经调用方折算对齐。
     *
     * @param code 了结形态编码（0=TRIPPED、1=BROKEN）
     * @return 形态词或空串
     */
    private static String barrierFinalWord(int code) {
        return switch (code) {
            case 0 -> "tripped";
            case 1 -> "broken";
            default -> "";
        };
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
