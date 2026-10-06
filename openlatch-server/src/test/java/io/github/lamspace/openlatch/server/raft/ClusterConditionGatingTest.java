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

import io.github.lamspace.openlatch.protocol.AcquireRequest;
import io.github.lamspace.openlatch.protocol.AwaitNotify;
import io.github.lamspace.openlatch.protocol.ConditionOp;
import io.github.lamspace.openlatch.protocol.ConditionOpRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.ServerConfig;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 条件 v9 集群面门控与接线测试（判例 {@code ClusterRejectShapeTest} 的
 * {@link ClusterHarness} 基座；三方法三 harness、方法内判据合并，守共享
 * fork 端口纪律）：
 * Leader 受理折叠 await（登记半程在预检点先于提交、回执改写 {@code QUEUED}
 * 且位次=本 key 等待项合计读数）→ 持有者 SIGNAL 搬运（条件集清零、等待队列
 * 接管）→ RELEASE 经 {@code freed_keys} 队首通知桥投递 {@code AWAIT_NOTIFY}
 * （ref=折叠 request_id、request_id=0，零新推送类型）；signal 权限服务端权威
 * （非持有 {@code NOT_HELD}、搬运零发生）；v8 会话两形同型
 * {@code INVALID_REQUEST} 不断连且既有请求照常；折叠形状与 op×字段矩阵违例
 * 同型拒绝且登记表零扰动；合并深度护栏 {@code OVERLOADED}（拒绝项零登记）；
 * {@code LEAVE} 幂等恒 {@code OK}。Follower 对 CONDITION_OP 同型
 * {@code NOT_LEADER} 零副作用（判例 QUEUE/TOPIC 直发车道），对折叠 ACQUIRE
 * 走 ACQUIRE 车道回 {@code NOT_LEADER} 并随附 v2 leader 提示字段。
 * 管理观察与换主分层用例：Leader/Follower 双视角 {@code condition_waiters}
 * 计数与五字段明细（Follower 如实零读随 {@code wait_queue_leader_only}
 * 同源标注、SUMMARY 合计含条件、观察零扰动）；杀主后登记集清零、窗内
 * {@code SIGNAL} 不补偿（OK 零搬运）、新会话重挂幂等接纳后唤醒链在
 * 新 term 完整续行（搬运→释放接力通知→队首重发授予）。
 */
@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ClusterConditionGatingTest {

    /** 构造 v9 获取信封（{@code condition=null} 为普通形态）。 */
    private static Envelope acquire(long rid, String key, LockType type, long threadId,
            long waitMs, String condition) {
        AcquireRequest.Builder b = AcquireRequest.newBuilder()
                .setKey(key).setLockType(type).setThreadId(threadId).setWaitMs(waitMs);
        if (condition != null) {
            b.setCondition(condition);
        }
        return Envelope.newBuilder().setProtocolVersion(9).setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid).setAcquireRequest(b).build();
    }

    /** 指定协议版本的 v9 折叠/普通获取信封（门控用例用）。 */
    private static Envelope acquireV(long rid, int version, String key, long threadId,
            long waitMs, String condition) {
        AcquireRequest.Builder b = AcquireRequest.newBuilder()
                .setKey(key).setLockType(LockType.LOCK_TYPE_REENTRANT)
                .setThreadId(threadId).setWaitMs(waitMs);
        if (condition != null) {
            b.setCondition(condition);
        }
        return Envelope.newBuilder().setProtocolVersion(version)
                .setType(MessageType.LOCK_ACQUIRE).setRequestId(rid)
                .setAcquireRequest(b).build();
    }

    /** 构造 CONDITION_OP 信封。 */
    private static Envelope condOp(long rid, int version, String key, ConditionOp op,
            String condition, long threadId, long awaitRequestId) {
        return Envelope.newBuilder().setProtocolVersion(version)
                .setType(MessageType.CONDITION_OP).setRequestId(rid)
                .setConditionOpRequest(ConditionOpRequest.newBuilder()
                        .setKey(key).setOp(op).setCondition(condition)
                        .setThreadId(threadId).setAwaitRequestId(awaitRequestId))
                .build();
    }

    @Test
    void leaderFoldSignalReleaseLoopWithGatingAndGuardrail() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn holder = h.connect(leader);
            assertThat(holder.hello(1, 9).getHelloResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            ClusterHarness.TestConn waiter = h.connect(leader);
            assertThat(waiter.hello(1, 9).getHelloResponse().getStatus())
                    .isEqualTo(StatusCode.OK);

            Envelope hold = holder.request(acquire(2, "fk",
                    LockType.LOCK_TYPE_REENTRANT, 1L, 0L, null));
            assertThat(hold.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            long token = hold.getAcquireResponse().getLeaseToken();

            // 折叠 await：Leader 预检点登记 → LOCK_ACQUIRE_ENTRY 提交 →
            // 应用点仅执行释放半程（非持有零操作）→ 回执改写 QUEUED、
            // 位次=本 key 等待项合计读数。
            Envelope await = waiter.request(acquire(7, "fk",
                    LockType.LOCK_TYPE_REENTRANT, 11L, -1L, "x"));
            assertThat(await.hasAcquireResponse()).isTrue();
            assertThat(await.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            assertThat(await.getAcquireResponse().getQueuePosition()).isEqualTo(1);
            assertThat(leader.runtime.conditionRegistry().count("fk")).isEqualTo(1);
            assertThat(leader.runtime.waitQueue().waitCount("fk")).isZero();
            // 折叠 rid 的授予侧收口不误摘本次登记（hasCondition 守卫）。
            assertThat(leader.runtime.conditionRegistry()
                    .isRegistered(waiter.session.sessionId(), 7, "fk")).isTrue();

            // v8 会话两形同型拒绝、不断连、既有请求照常（判例 v3-v8 门）。
            ClusterHarness.TestConn v8 = h.connect(leader);
            assertThat(v8.hello(1, 8).getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);
            Envelope gated = v8.request(condOp(2, 8, "fk", ConditionOp.CONDITION_OP_SIGNAL,
                    "x", 1L, 0L));
            assertThat(gated.hasConditionOpResponse()).isTrue();
            assertThat(gated.getConditionOpResponse().getStatus())
                    .isEqualTo(StatusCode.INVALID_REQUEST);
            assertThat(gated.getConditionOpResponse().getOp())
                    .isEqualTo(ConditionOp.CONDITION_OP_SIGNAL);
            Envelope gatedFold = v8.request(acquireV(3, 8, "fk2", 1L, -1L, "x"));
            assertThat(gatedFold.getAcquireResponse().getStatus())
                    .isEqualTo(StatusCode.INVALID_REQUEST);
            Envelope reg = v8.request(acquireV(4, 8, "v8k", 1L, 0L, null));
            assertThat(reg.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);

            // 形状违例矩阵（Leader v9 会话）：同型 INVALID、登记表零扰动。
            ClusterHarness.TestConn shaper = h.connect(leader);
            assertThat(shaper.hello(1, 9).getHelloResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            assertThat(shaper.request(condOp(2, 9, "fk", ConditionOp.CONDITION_OP_SIGNAL,
                    "x", 1L, 5L)).getConditionOpResponse().getStatus())
                    .isEqualTo(StatusCode.INVALID_REQUEST); // SIGNAL 携 await_request_id
            assertThat(shaper.request(condOp(3, 9, "fk", ConditionOp.CONDITION_OP_LEAVE,
                    "x", 1L, 5L)).getConditionOpResponse().getStatus())
                    .isEqualTo(StatusCode.INVALID_REQUEST); // LEAVE 携 thread_id
            assertThat(shaper.request(condOp(4, 9, "fk", ConditionOp.CONDITION_OP_SIGNAL,
                    "", 1L, 0L)).getConditionOpResponse().getStatus())
                    .isEqualTo(StatusCode.INVALID_REQUEST); // 空条件名
            assertThat(shaper.request(condOp(5, 9, "fk", ConditionOp.CONDITION_OP_SIGNAL,
                    "z".repeat(ServerConfig.DEFAULT_MAX_KEY_LENGTH + 1), 1L, 0L))
                    .getConditionOpResponse().getStatus())
                    .isEqualTo(StatusCode.INVALID_REQUEST); // 超长条件名
            assertThat(shaper.request(acquire(6, "fk3",
                    LockType.LOCK_TYPE_READ, 1L, -1L, "x")).getAcquireResponse().getStatus())
                    .isEqualTo(StatusCode.INVALID_REQUEST); // READ + condition
            assertThat(shaper.request(acquire(7, "fk3",
                    LockType.LOCK_TYPE_REENTRANT, 1L, 0L, "x")).getAcquireResponse().getStatus())
                    .isEqualTo(StatusCode.INVALID_REQUEST); // 立即式 + condition
            assertThat(leader.runtime.conditionRegistry().count("fk")).isEqualTo(1);
            assertThat(leader.runtime.conditionRegistry().count("fk3")).isZero();

            // 权限服务端权威：等待者（非持有）signal → NOT_HELD、搬运零发生。
            assertThat(waiter.request(condOp(8, 9, "fk", ConditionOp.CONDITION_OP_SIGNAL,
                    "x", 11L, 0L)).getConditionOpResponse().getStatus())
                    .isEqualTo(StatusCode.NOT_HELD);
            assertThat(leader.runtime.conditionRegistry().count("fk")).isEqualTo(1);
            assertThat(leader.runtime.waitQueue().waitCount("fk")).isZero();

            // 持有者 SIGNAL：搬运入等待队列（条件集清零、队列接管）。
            assertThat(holder.request(condOp(3, 9, "fk", ConditionOp.CONDITION_OP_SIGNAL,
                    "x", 1L, 0L)).getConditionOpResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            assertThat(leader.runtime.conditionRegistry().count("fk")).isZero();
            assertThat(leader.runtime.waitQueue().waitCount("fk")).isEqualTo(1);

            // 释放接力：freed_keys → 队首通知桥 → AWAIT_NOTIFY（ref=折叠 rid）。
            Envelope rel = holder.request(Envelope.newBuilder()
                    .setProtocolVersion(9).setType(MessageType.LOCK_RELEASE)
                    .setRequestId(4)
                    .setReleaseRequest(ReleaseRequest.newBuilder().setKey("fk")
                            .setThreadId(1L).setLeaseToken(token))
                    .build());
            assertThat(rel.getReleaseResponse().getStatus()).isEqualTo(StatusCode.OK);
            Envelope notify = waiter.awaitOutbound(10_000);
            assertThat(notify.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
            assertThat(notify.getRequestId()).isZero();
            AwaitNotify an = notify.getAwaitNotify();
            assertThat(an.getKey()).isEqualTo("fk");
            assertThat(an.getRequestIdRef()).isEqualTo(7L);

            // LEAVE 幂等（未存在的登记恒 OK 无错误态）。
            assertThat(waiter.request(condOp(9, 9, "fk", ConditionOp.CONDITION_OP_LEAVE,
                    "x", 0L, 999L)).getConditionOpResponse().getStatus())
                    .isEqualTo(StatusCode.OK);

            // 合并深度护栏：预填满登记集后新折叠 await 同型 OVERLOADED、
            // queue_position 零值、拒绝项零登记、既有等待项零扰动。
            int limit = ServerConfig.DEFAULT_MAX_QUEUE_DEPTH_PER_KEY;
            for (int i = 0; i < limit; i++) {
                leader.runtime.conditionRegistry().register(9_999L, 500_000L + i,
                        8_000L + i, "gk", "x", System.currentTimeMillis());
            }
            Envelope over = holder.request(acquire(20, "gk",
                    LockType.LOCK_TYPE_REENTRANT, 1L, -1L, "x"));
            assertThat(over.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OVERLOADED);
            assertThat(over.getAcquireResponse().getQueuePosition()).isZero();
            assertThat(leader.runtime.conditionRegistry().count("gk")).isEqualTo(limit);
            assertThat(leader.runtime.conditionRegistry()
                    .isRegistered(holder.session.sessionId(), 20, "gk")).isFalse();
        }
    }

    /**
     * v9 管理观察夹具 token（{@link io.github.lamspace.openlatch.server.admin
     * .AdminRequestHandler} 直构装配用，判例 {@code TopicAdminTest}）。
     */
    private static final String ADMIN_TOKEN = "cond-admin-token";

    /** 管理 ADMIN_SUMMARY 请求信封。 */
    private static Envelope adminSummaryMsg(long rid) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.ADMIN_SUMMARY)
                .setRequestId(rid)
                .setAdminSummaryRequest(io.github.lamspace.openlatch.protocol
                        .AdminSummaryRequest.newBuilder().setToken(ADMIN_TOKEN))
                .build();
    }

    /** 管理 ADMIN_LIST_KEYS 请求信封。 */
    private static Envelope adminListKeysMsg(long rid, String prefix) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.ADMIN_LIST_KEYS)
                .setRequestId(rid)
                .setAdminListKeysRequest(io.github.lamspace.openlatch.protocol
                        .AdminListKeysRequest.newBuilder().setToken(ADMIN_TOKEN)
                        .setPage(0).setPageSize(100).setPrefix(prefix))
                .build();
    }

    /** 管理 ADMIN_KEY_DETAIL 请求信封。 */
    private static Envelope adminKeyDetailMsg(long rid, String key) {
        return Envelope.newBuilder().setProtocolVersion(3).setType(MessageType.ADMIN_KEY_DETAIL)
                .setRequestId(rid)
                .setAdminKeyDetailRequest(io.github.lamspace.openlatch.protocol
                        .AdminKeyDetailRequest.newBuilder().setToken(ADMIN_TOKEN).setKey(key))
                .build();
    }

    /** 为节点装配管理处理器（集群数据源形态；判例 TopicAdminTest）。 */
    private static io.github.lamspace.openlatch.server.admin.AdminRequestHandler adminHandler(
            ClusterHarness.Node node) {
        return new io.github.lamspace.openlatch.server.admin.AdminRequestHandler(
                new io.github.lamspace.openlatch.server.AdminConfig(ADMIN_TOKEN),
                null, node.runtime, node.registry, () -> 77L);
    }

    /** 同步发一条 ADMIN 请求并取回应答。 */
    private static Envelope admin(ClusterHarness.Node node,
            io.github.lamspace.openlatch.server.admin.AdminRequestHandler handler,
            ClusterHarness.TestConn conn, Envelope msg) {
        handler.handle(conn.ctx, conn.session, msg);
        return conn.awaitOutbound(10_000);
    }

    @Test
    void leaderKillReRegisterSignalNotCompensatedAndAdminDualView() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.Node follower = h.nodes().stream()
                    .filter(ClusterHarness.Node::alive).filter(x -> !x.isLeader())
                    .findFirst().orElseThrow();
            ClusterHarness.TestConn awaiter = h.connect(leader);
            assertThat(awaiter.hello(1, 9).getHelloResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            ClusterHarness.TestConn holder = h.connect(leader);
            assertThat(holder.hello(1, 9).getHelloResponse().getStatus())
                    .isEqualTo(StatusCode.OK);

            // 持有者先持有（条目经复制建立），等待者折叠挂起（登记在 Leader
            // 本地集；等待者非持有归属，释放半程零操作）。
            Envelope hold = holder.request(acquire(2, "ak",
                    LockType.LOCK_TYPE_REENTRANT, 1L, 0L, null));
            assertThat(hold.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            long token = hold.getAcquireResponse().getLeaseToken();
            Envelope await = awaiter.request(acquire(7, "ak",
                    LockType.LOCK_TYPE_REENTRANT, 11L, -1L, "x"));
            assertThat(await.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            assertThat(leader.runtime.conditionRegistry().count("ak")).isEqualTo(1);

            // Leader 视角：LOCK 行复制态照常（持有 1）+ condition_waiters 读
            // 本地集；KEY_DETAIL 五字段明细齐备；SUMMARY 等待者合计含条件。
            io.github.lamspace.openlatch.server.admin.AdminRequestHandler lHandler =
                    adminHandler(leader);
            ClusterHarness.TestConn lAdmin = h.connect(leader);
            lAdmin.hello(1, 9);
            io.github.lamspace.openlatch.protocol.AdminListKeysResponse lKeys = admin(leader,
                    lHandler, lAdmin, adminListKeysMsg(30L, "")).getAdminListKeysResponse();
            var lRow = lKeys.getItemsList().stream().filter(i -> i.getKey().equals("ak"))
                    .findFirst().orElseThrow();
            assertThat(lRow.getFamily()).isEqualTo("lock");
            assertThat(lRow.getHolders()).isEqualTo(1);
            assertThat(lRow.getConditionWaiters()).isEqualTo(1);
            io.github.lamspace.openlatch.protocol.AdminKeyDetailResponse lDetail = admin(leader,
                    lHandler, lAdmin, adminKeyDetailMsg(31L, "ak")).getAdminKeyDetailResponse();
            assertThat(lDetail.getStatus()).isEqualTo(StatusCode.OK);
            assertThat(lDetail.getWaitQueueLeaderOnly()).isFalse();
            assertThat(lDetail.getConditionWaiters()).isEqualTo(1);
            assertThat(lDetail.getConditionWaitersInfoList()).hasSize(1);
            var view = lDetail.getConditionWaitersInfoList().get(0);
            assertThat(view.getCondition()).isEqualTo("x");
            assertThat(view.getSessionId()).isEqualTo(awaiter.session.sessionId());
            assertThat(view.getRequestId()).isEqualTo(7L);
            assertThat(view.getThreadId()).isEqualTo(11L);
            assertThat(view.getRegisteredAtMs()).isPositive();
            // 搬运入队项与条件集互斥计数：无搬运时等待队列区段为空、不重复。
            assertThat(lDetail.getWaitersList()).isEmpty();
            io.github.lamspace.openlatch.protocol.AdminSummaryResponse lSum = admin(leader,
                    lHandler, lAdmin, adminSummaryMsg(32L)).getAdminSummaryResponse();
            assertThat(lSum.getTotalWaiters()).isGreaterThanOrEqualTo(1);

            // Follower 视角：复制态照常呈现（持有 1），条件读数如实零、
            // 随 wait_queue_leader_only 同源标注（等待集不入复制态）。
            io.github.lamspace.openlatch.server.admin.AdminRequestHandler fHandler =
                    adminHandler(follower);
            ClusterHarness.TestConn fAdmin = h.connect(follower);
            fAdmin.hello(1, 9);
            io.github.lamspace.openlatch.protocol.AdminListKeysResponse fKeys = admin(follower,
                    fHandler, fAdmin, adminListKeysMsg(40L, "")).getAdminListKeysResponse();
            var fRow = fKeys.getItemsList().stream().filter(i -> i.getKey().equals("ak"))
                    .findFirst().orElseThrow();
            assertThat(fRow.getHolders()).isEqualTo(1);
            assertThat(fRow.getConditionWaiters()).as("Follower 如实零读").isZero();
            io.github.lamspace.openlatch.protocol.AdminKeyDetailResponse fDetail = admin(follower,
                    fHandler, fAdmin, adminKeyDetailMsg(41L, "ak")).getAdminKeyDetailResponse();
            assertThat(fDetail.getStatus()).isEqualTo(StatusCode.OK);
            assertThat(fDetail.getWaitQueueLeaderOnly()).isTrue();
            assertThat(fDetail.getConditionWaiters()).isZero();
            assertThat(fDetail.getConditionWaitersInfoList()).isEmpty();
            assertThat(fDetail.getHoldersList()).hasSize(1);
            // 观察零扰动：反复查询后搬运/登记时序不变（Leader 计数同值）。
            admin(follower, fHandler, fAdmin, adminKeyDetailMsg(42L, "ak"));
            admin(leader, lHandler, lAdmin, adminKeyDetailMsg(43L, "ak"));
            assertThat(leader.runtime.conditionRegistry().count("ak")).isEqualTo(1);

            // 换主前释放持有（复制态清账；后续以新持有者重放窗口场景）。
            Envelope rel = holder.request(Envelope.newBuilder()
                    .setProtocolVersion(9).setType(MessageType.LOCK_RELEASE)
                    .setRequestId(3)
                    .setReleaseRequest(ReleaseRequest.newBuilder().setKey("ak")
                            .setThreadId(1L).setLeaseToken(token))
                    .build());
            assertThat(rel.getReleaseResponse().getStatus()).isEqualTo(StatusCode.OK);

            // 杀主：新 Leader 登记集为零（换主清零、无旧 term 残留可重放）。
            h.stopNode(leader.id);
            h.awaitTrue(h::hasLeader, 30_000, "新主选出");
            ClusterHarness.Node newLeader = h.leader();
            assertThat(newLeader).isNotSameAs(leader);
            assertThat(newLeader.runtime.conditionRegistry().totalCount()).isZero();

            // 换主窗 signal 不补偿：新持有者 SIGNAL 恒 OK 但零搬运（"signal
            // 是事件"——旧登记不复活，等待者以 timed await 自救的契约面）。
            ClusterHarness.TestConn holder2 = h.connect(newLeader);
            assertThat(holder2.hello(1, 9).getHelloResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            Envelope hold2 = holder2.request(acquire(2, "ak",
                    LockType.LOCK_TYPE_REENTRANT, 21L, 0L, null));
            assertThat(hold2.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            long token2 = hold2.getAcquireResponse().getLeaseToken();
            assertThat(holder2.request(condOp(3, 9, "ak", ConditionOp.CONDITION_OP_SIGNAL,
                    "x", 21L, 0L)).getConditionOpResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            assertThat(newLeader.runtime.conditionRegistry().count("ak")).isZero();
            assertThat(newLeader.runtime.waitQueue().waitCount("ak")).isZero();

            // 过渡窗管理面：无重挂登记时明细如实空（复制态行随新持有者
            // 重建、条件两字段缺省零值/空列表）。
            io.github.lamspace.openlatch.server.admin.AdminRequestHandler nHandler =
                    adminHandler(newLeader);
            ClusterHarness.TestConn nAdmin = h.connect(newLeader);
            nAdmin.hello(1, 9);
            io.github.lamspace.openlatch.protocol.AdminKeyDetailResponse gapDetail = admin(
                    newLeader, nHandler, nAdmin, adminKeyDetailMsg(50L, "ak"))
                    .getAdminKeyDetailResponse();
            assertThat(gapDetail.getStatus()).isEqualTo(StatusCode.OK);
            assertThat(gapDetail.getConditionWaiters()).isZero();
            assertThat(gapDetail.getConditionWaitersInfoList()).isEmpty();

            // 重挂（新会话折叠 ACQUIRE，判例客户端 ACQUIRE 车道迁移形态）：
            // 幂等接纳、同身份重发不双登记；窗口后明细恢复可见。
            ClusterHarness.TestConn awaiter2 = h.connect(newLeader);
            assertThat(awaiter2.hello(1, 9).getHelloResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            Envelope reRegister = awaiter2.request(acquire(9, "ak",
                    LockType.LOCK_TYPE_REENTRANT, 22L, -1L, "x"));
            assertThat(reRegister.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            assertThat(newLeader.runtime.conditionRegistry().count("ak")).isEqualTo(1);
            Envelope reRetry = awaiter2.request(acquire(9, "ak",
                    LockType.LOCK_TYPE_REENTRANT, 22L, -1L, "x"));
            assertThat(reRetry.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            assertThat(newLeader.runtime.conditionRegistry().count("ak"))
                    .as("幂等重挂不双登记").isEqualTo(1);
            io.github.lamspace.openlatch.protocol.AdminKeyDetailResponse afterDetail = admin(
                    newLeader, nHandler, nAdmin, adminKeyDetailMsg(51L, "ak"))
                    .getAdminKeyDetailResponse();
            assertThat(afterDetail.getConditionWaiters()).isEqualTo(1);
            assertThat(afterDetail.getConditionWaitersInfoList()).hasSize(1);
            assertThat(afterDetail.getConditionWaitersInfoList().get(0).getRequestId())
                    .isEqualTo(9L);

            // 新 term 的 SIGNAL→RELEASE 唤醒链完整续行：搬运入队（集零、队列
            // 接管）→ 释放经 freed_keys 队首桥推 AWAIT_NOTIFY（ref=折叠 rid）
            // → 队首重发（condition 已清除）授予新凭证。
            assertThat(holder2.request(condOp(4, 9, "ak", ConditionOp.CONDITION_OP_SIGNAL,
                    "x", 21L, 0L)).getConditionOpResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            assertThat(newLeader.runtime.conditionRegistry().count("ak")).isZero();
            assertThat(newLeader.runtime.waitQueue().waitCount("ak")).isEqualTo(1);
            Envelope rel2 = holder2.request(Envelope.newBuilder()
                    .setProtocolVersion(9).setType(MessageType.LOCK_RELEASE)
                    .setRequestId(5)
                    .setReleaseRequest(ReleaseRequest.newBuilder().setKey("ak")
                            .setThreadId(21L).setLeaseToken(token2))
                    .build());
            assertThat(rel2.getReleaseResponse().getStatus()).isEqualTo(StatusCode.OK);
            Envelope notify = awaiter2.awaitOutbound(10_000);
            assertThat(notify.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
            assertThat(notify.getAwaitNotify().getRequestIdRef()).isEqualTo(9L);
            Envelope reacquire = awaiter2.request(acquire(9, "ak",
                    LockType.LOCK_TYPE_REENTRANT, 22L, -1L, null));
            assertThat(reacquire.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(reacquire.getAcquireResponse().getLeaseToken()).isPositive();
            // 授予侧收口后该归属集内无残留（purgeOwner 臂经普通授予路径生效）。
            assertThat(newLeader.runtime.conditionRegistry()
                    .isRegistered(awaiter2.session.sessionId(), 9, "ak")).isFalse();

            holder2.disconnect();
            awaiter2.disconnect();
            lAdmin.disconnect();
            fAdmin.disconnect();
            nAdmin.disconnect();
        }
    }

    @Test
    void followerRejectsConditionOpAndFoldWithSameShapeNotLeader() throws IOException {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node follower = h.nodes().stream()
                    .filter(ClusterHarness.Node::alive).filter(x -> !x.isLeader())
                    .findFirst().orElseThrow();
            ClusterHarness.TestConn c = h.connect(follower);
            assertThat(c.hello(1, 9).getHelloResponse().getStatus()).isEqualTo(StatusCode.OK);

            // CONDITION_OP 直发车道：同型 NOT_LEADER、op 回显、零副作用
            //（无 leader 提示字段，判例 QUEUE/TOPIC）。
            Envelope sig = c.request(condOp(2, 9, "fk", ConditionOp.CONDITION_OP_SIGNAL,
                    "x", 1L, 0L));
            assertThat(sig.hasConditionOpResponse())
                    .as("拒绝须携同型 condition_op_response").isTrue();
            assertThat(sig.getConditionOpResponse().getStatus())
                    .isEqualTo(StatusCode.NOT_LEADER);
            assertThat(sig.getConditionOpResponse().getOp())
                    .isEqualTo(ConditionOp.CONDITION_OP_SIGNAL);
            Envelope leave = c.request(condOp(3, 9, "fk", ConditionOp.CONDITION_OP_LEAVE,
                    "x", 0L, 7L));
            assertThat(leave.getConditionOpResponse().getStatus())
                    .isEqualTo(StatusCode.NOT_LEADER);

            // 折叠 ACQUIRE 走 ACQUIRE 车道：NOT_LEADER 随附 v2 提示字段。
            Envelope fold = c.request(acquire(4, "fk",
                    LockType.LOCK_TYPE_REENTRANT, 1L, -1L, "x"));
            assertThat(fold.getAcquireResponse().getStatus()).isEqualTo(StatusCode.NOT_LEADER);
            assertThat(fold.getAcquireResponse().getLeaderNodeId())
                    .isEqualTo(h.leader().id);

            // 角色门先于预检登记/搬运：本节点条件登记与等待队列恒空。
            assertThat(follower.runtime.conditionRegistry().totalCount()).isZero();
            assertThat(follower.runtime.waitQueue().maxQueueDepth()).isZero();
        }
    }

    /**
     * 行为断言一（持有者死亡不代醒，契约"signal 仍是唯一通道"）：等待者登记后
     * 新持有者经断连（`SESSION_CLOSE` 双路之一）死亡——锁随会话收账释放（他者
     * 可获取侧证"死亡不吞锁"），等待者 MUST NOT 收到任何代为唤醒的通知、登记
     * 原样存续；继任持有者 SIGNAL 后唤醒链完整续行（搬运→释放接力通知→队首
     * 重发授予）。行为断言二（SIGNAL_ALL 串醒按到达序）：两名等待者按登记先后
     * 入集，SIGNAL_ALL 全员搬运后释放——队首（先到者）自推进授予、后到者
     * 重发得 QUEUED（位次接管），先到者释放后后到者经通知链完成授予。
     */
    @Test
    void holderDeathKeepsWaiterAsleepAndSignalAllSerializesByArrival() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn a = h.connect(leader);
            a.hello(1, 9);
            ClusterHarness.TestConn b1 = h.connect(leader);
            b1.hello(1, 9);
            ClusterHarness.TestConn c = h.connect(leader);
            c.hello(1, 9);

            // —— 持有者死亡不代醒 ——
            Envelope a1 = a.request(acquire(1, "kd",
                    LockType.LOCK_TYPE_REENTRANT, 1L, 0L, null));
            assertThat(a1.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(a.request(rel(2, "kd", a1.getAcquireResponse().getLeaseToken(), 1L))
                    .getReleaseResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(b1.request(acquire(3, "kd",
                    LockType.LOCK_TYPE_REENTRANT, 11L, 0L, null))
                    .getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(b1.request(acquire(4, "kd",
                    LockType.LOCK_TYPE_REENTRANT, 11L, -1L, "x"))
                    .getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            Envelope a2 = a.request(acquire(5, "kd",
                    LockType.LOCK_TYPE_REENTRANT, 1L, 0L, null)); // A 重新持有
            assertThat(a2.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            a.disconnect(); // 断连传播 SESSION_CLOSE：收账 A 的持有
            Thread.sleep(300);
            assertThat(b1.pollOutbound()).isNull(); // 不代醒：无任何推送
            assertThat(leader.runtime.conditionRegistry().count("kd")).isEqualTo(1); // 登记存续
            Envelope takeover = c.request(acquire(6, "kd",
                    LockType.LOCK_TYPE_REENTRANT, 21L, 0L, null));
            assertThat(takeover.getAcquireResponse().getStatus())
                    .isEqualTo(StatusCode.OK); // 死亡不吞锁：他者可入
            assertThat(c.request(condOp(7, 9, "kd", ConditionOp.CONDITION_OP_SIGNAL,
                    "x", 21L, 0L)).getConditionOpResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            assertThat(leader.runtime.conditionRegistry().count("kd")).isZero();
            assertThat(leader.runtime.waitQueue().waitCount("kd")).isEqualTo(1);
            c.request(rel(8, "kd", takeover.getAcquireResponse().getLeaseToken(), 21L)); // 释放接力
            Envelope bNotify = b1.awaitOutbound(10_000); // 接力通知先到（ref=折叠 rid）
            assertThat(bNotify.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
            assertThat(bNotify.getAwaitNotify().getRequestIdRef()).isEqualTo(4);
            Envelope woken = b1.request(acquire(4, "kd",
                    LockType.LOCK_TYPE_REENTRANT, 11L, -1L, null));
            assertThat(woken.getAcquireResponse().getStatus())
                    .isEqualTo(StatusCode.OK); // 唤醒链完整续行（同 rid 无 condition 重发）
            assertThat(woken.getAcquireResponse().getLeaseToken()).isPositive();
            b1.disconnect();

            // —— SIGNAL_ALL 按到达序串醒 ——
            ClusterHarness.TestConn d = h.connect(leader);
            d.hello(1, 9);
            ClusterHarness.TestConn e = h.connect(leader);
            e.hello(1, 9);
            Envelope c10 = c.request(acquire(10, "sk",
                    LockType.LOCK_TYPE_REENTRANT, 21L, 0L, null));
            assertThat(c10.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(c.request(acquire(11, "sk",
                    LockType.LOCK_TYPE_REENTRANT, 21L, -1L, "x"))
                    .getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            Envelope d12 = d.request(acquire(12, "sk",
                    LockType.LOCK_TYPE_REENTRANT, 31L, 0L, null));
            assertThat(d12.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(d.request(acquire(13, "sk",
                    LockType.LOCK_TYPE_REENTRANT, 31L, -1L, "x"))
                    .getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            Envelope eHold = e.request(acquire(14, "sk",
                    LockType.LOCK_TYPE_REENTRANT, 41L, 0L, null));
            assertThat(eHold.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(e.request(condOp(15, 9, "sk", ConditionOp.CONDITION_OP_SIGNAL_ALL,
                    "x", 41L, 0L)).getConditionOpResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            assertThat(leader.runtime.conditionRegistry().count("sk")).isZero();
            assertThat(leader.runtime.waitQueue().waitCount("sk")).isEqualTo(2);
            e.request(rel(16, "sk", eHold.getAcquireResponse().getLeaseToken(), 41L));
            // 队首=先登记 c(rid11)：接力通知先到 c、c 自推进授予、d 重发得
            // QUEUED（位次不越前辈）。
            Envelope cNotify = c.awaitOutbound(10_000);
            assertThat(cNotify.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
            assertThat(cNotify.getAwaitNotify().getRequestIdRef()).isEqualTo(11);
            Envelope cWake = c.request(acquire(11, "sk",
                    LockType.LOCK_TYPE_REENTRANT, 21L, -1L, null));
            assertThat(cWake.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            Envelope dTry = d.request(acquire(13, "sk",
                    LockType.LOCK_TYPE_REENTRANT, 31L, -1L, null));
            assertThat(dTry.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            c.request(rel(17, "sk", cWake.getAcquireResponse().getLeaseToken(), 21L));
            Envelope dNotify = d.awaitOutbound(10_000); // 队首推进通知 d（rid13）
            assertThat(dNotify.getType()).isEqualTo(MessageType.AWAIT_NOTIFY);
            assertThat(dNotify.getAwaitNotify().getRequestIdRef()).isEqualTo(13);
            Envelope dWake = d.request(acquire(13, "sk",
                    LockType.LOCK_TYPE_REENTRANT, 31L, -1L, null));
            assertThat(dWake.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            assertThat(leader.runtime.waitQueue().waitCount("sk")).isZero();
            d.request(rel(18, "sk", dWake.getAcquireResponse().getLeaseToken(), 31L));
            c.disconnect();
            d.disconnect();
            e.disconnect();
        }
    }

    /** 构造释放信封（归属线程随持有请求一致）。 */
    private static Envelope rel(long rid, String key, long token, long threadId) {
        return Envelope.newBuilder().setProtocolVersion(9).setType(MessageType.LOCK_RELEASE)
                .setRequestId(rid)
                .setReleaseRequest(ReleaseRequest.newBuilder().setKey(key)
                        .setLeaseToken(token).setThreadId(threadId))
                .build();
    }
}
