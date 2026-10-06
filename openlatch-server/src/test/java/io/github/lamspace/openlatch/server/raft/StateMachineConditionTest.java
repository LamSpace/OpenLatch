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
import io.github.lamspace.openlatch.protocol.ConditionOp;
import io.github.lamspace.openlatch.protocol.ConditionOpRequest;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.protocol.raft.RaftEntryType;
import io.github.lamspace.openlatch.protocol.raft.SnapshotLock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 条件 v9 复制边界守卫（replicated-state-machine 规格常驻条款）：signal 家族
 * （SIGNAL/SIGNAL_ALL/LEAVE）在纯本地裁决流量下应用位点不前进、副本摘要与
 * 基线逐字节相等、重启节点不重放任何陈旧唤醒（等待集为 Leader 易失态、搬运
 * 时序不入日志——"signal 是事件不是状态"）；await 折叠的释放半程恰以一条既有
 * {@code LOCK_ACQUIRE_ENTRY} 进日志（其生效性由后续持有者可授予侧证）；并以
 * 描述符级断言钉死复制面（条件维条目类型零占用——{@code RaftEntryType} 无
 * CONDITION 取值，v10 起值域上界 14 为 phaser 专用不构成例外、
 * {@code SnapshotLock} 无 condition 字段）——未来把 signal 家族塞进日志或把
 * 等待集写进快照的改动，在本守卫与编号证据上即刻转红。
 */
@Timeout(value = 180, unit = TimeUnit.SECONDS)
class StateMachineConditionTest {

    /** 构造 v9 获取信封（{@code condition=null} 为普通形态）。 */
    private static Envelope acquire(long rid, String key, long threadId,
            long waitMs, String condition) {
        AcquireRequest.Builder b = AcquireRequest.newBuilder()
                .setKey(key).setLockType(LockType.LOCK_TYPE_REENTRANT)
                .setThreadId(threadId).setWaitMs(waitMs);
        if (condition != null) {
            b.setCondition(condition);
        }
        return Envelope.newBuilder().setProtocolVersion(9).setType(MessageType.LOCK_ACQUIRE)
                .setRequestId(rid).setAcquireRequest(b).build();
    }

    /** 构造 CONDITION_OP 信封。 */
    private static Envelope condOp(long rid, String key, ConditionOp op, String condition,
            long threadId, long awaitRequestId) {
        return Envelope.newBuilder().setProtocolVersion(9)
                .setType(MessageType.CONDITION_OP).setRequestId(rid)
                .setConditionOpRequest(ConditionOpRequest.newBuilder()
                        .setKey(key).setOp(op).setCondition(condition)
                        .setThreadId(threadId).setAwaitRequestId(awaitRequestId))
                .build();
    }

    @Test
    void signalFamilyLeavesLogAndDigestUntouched() throws Exception {
        try (ClusterHarness h = ClusterHarness.start(3)) {
            // 探针关闭：排除 NOOP/失联探针条目，"signal 家族零条目"的判据纯净。
            h.setProbesEnabled(false);
            ClusterHarness.Node leader = h.leader();
            ClusterHarness.TestConn holder = h.connect(leader);
            assertThat(holder.hello(1, 9).getHelloResponse().getStatus())
                    .isEqualTo(StatusCode.OK);
            ClusterHarness.TestConn signaler = h.connect(leader);
            assertThat(signaler.hello(1, 9).getHelloResponse().getStatus())
                    .isEqualTo(StatusCode.OK);

            // 基线前置流量（入日志的部分）：持有 → 折叠 await（恰一条既有类型
            // 条目的释放半程）→ 他者获取被授予（侧证释放半程经复制生效——
            // 折叠零日志时此处必然排队而非授予）。
            Envelope hold = holder.request(acquire(2, "ck", 1L, 0L, null));
            assertThat(hold.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);
            Envelope await = holder.request(acquire(7, "ck", 1L, -1L, "x"));
            assertThat(await.getAcquireResponse().getStatus()).isEqualTo(StatusCode.QUEUED);
            Envelope second = signaler.request(acquire(3, "ck", 2L, -1L, null));
            assertThat(second.getAcquireResponse().getStatus()).isEqualTo(StatusCode.OK);

            long baselineIndex = leader.lastApplied();
            String baselineDigest = leader.digest();
            assertThat(h.aliveAgreeWithLeader()).isTrue();

            // 守卫流量：SIGNAL 首次搬运（登记转入等待队列管辖，零条目）、
            // 其后空集无操作；SIGNAL_ALL 空集；LEAVE 幂等——全链零日志贡献。
            for (int i = 0; i < 4; i++) {
                Envelope sig = signaler.request(
                        condOp(40L + i, "ck", ConditionOp.CONDITION_OP_SIGNAL, "x", 2L, 0L));
                assertThat(sig.getConditionOpResponse().getStatus()).isEqualTo(StatusCode.OK);
            }
            for (int i = 0; i < 2; i++) {
                Envelope all = signaler.request(condOp(50L + i, "ck",
                        ConditionOp.CONDITION_OP_SIGNAL_ALL, "x", 2L, 0L));
                assertThat(all.getConditionOpResponse().getStatus()).isEqualTo(StatusCode.OK);
            }
            for (int i = 0; i < 2; i++) {
                Envelope leave = signaler.request(condOp(60L + i, "ck",
                        ConditionOp.CONDITION_OP_LEAVE, "x", 0L, 999L));
                assertThat(leave.getConditionOpResponse().getStatus()).isEqualTo(StatusCode.OK);
            }

            // 断言核心：8 次 signal 家族操作应用位点与摘要零前进。
            assertThat(leader.lastApplied()).isEqualTo(baselineIndex);
            assertThat(leader.digest()).isEqualTo(baselineDigest);
            assertThat(h.aliveAgreeWithLeader()).isTrue();

            // 重启 follower：追赶后不存在任何陈旧唤醒重放（搬运与通知时序均为
            // Leader 易失态，无日志来源可重演）。
            int fid = h.nodes().stream().filter(x -> x != leader)
                    .findFirst().orElseThrow().id;
            h.stopNode(fid);
            h.restartNode(fid);
            ClusterHarness.TestConn rconn = h.connect(h.node(fid));
            rconn.hello(1, 9);
            Thread.sleep(500);
            assertThat(rconn.pollOutbound()).isNull(); // 无陈旧 AWAIT_NOTIFY
            rconn.disconnect();
            holder.disconnect();
            signaler.disconnect();
        }
    }

    @Test
    void replicationSurfaceCarriesNoConditionTypes() {
        // 编号证据：条件维自 v9 起条目类型零占用——await 复用既有
        // LOCK_ACQUIRE_ENTRY、signal 家族零条目，值域中无 CONDITION 取值
        // （v10 上界升至 14 且 14 为 PHASER_OP_ENTRY 专用值，与条件边界无涉——
        // 证据线为版本相对口径而非绝对上界；UNRECOGNIZED 为 protobuf 哨兵值，
        // 取号即抛，不入值域断言）。
        for (RaftEntryType t : RaftEntryType.values()) {
            if (t == RaftEntryType.UNRECOGNIZED) {
                continue;
            }
            assertThat(t.getNumber()).isLessThanOrEqualTo(14);
            assertThat(t.name()).doesNotContain("CONDITION");
        }
        // 快照面：SnapshotLock 字段零 condition（等待集不入快照，await 的复制
        // 足迹仅为既有持有字段的清零，v9 对序列化面零扩展）。
        assertThat(SnapshotLock.getDescriptor().getFields().stream()
                .noneMatch(f -> f.getName().contains("condition"))).isTrue();
    }
}
