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

package io.github.lamspace.openlatch.server.metrics;

import io.github.lamspace.openlatch.core.CoreConfig;
import io.github.lamspace.openlatch.core.CoreEngine;
import io.github.lamspace.openlatch.core.SystemClock;
import io.github.lamspace.openlatch.protocol.AtomicOp;
import io.github.lamspace.openlatch.protocol.LockType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.github.lamspace.openlatch.server.session.ServerSessionRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 指标清单 ↔ Prometheus 线路名的映射词表（服务端指标
 * 清单与线路命名的唯一权威断言点）：逐项记录后 scrape，断言点分逻辑名翻译
 * 出的线名（counter 带 {@code _total} 尾不重复追加、Timer 输出
 * {@code _seconds_bucket/_count/_sum}、gauge 原名下划线化）。
 */
class ServerMetricsVocabularyTest {

    /** 被测词表。 */
    private ServerMetrics metrics;
    private String scrape;

    @BeforeEach
    void recordAllShapes() {
        metrics = new ServerMetrics();
        CoreEngine core = new CoreEngine(new CoreConfig(), new SystemClock(), (s, r, k) -> { });
        io.github.lamspace.openlatch.server.topic.TopicRegistry topics =
                new io.github.lamspace.openlatch.server.topic.TopicRegistry(
                        new ServerSessionRegistry(), 8, 16);
        topics.setDropListener(metrics::recordTopicDropped);
        metrics.bindStandaloneGauges(core, new ServerSessionRegistry(), topics);
        metrics.recordAcquire(StatusCode.OK, 2_000_000L);
        metrics.recordAcquire(StatusCode.QUEUED, 500_000L);
        metrics.recordAcquire(StatusCode.DENIED, 300_000L);
        metrics.recordRelease(StatusCode.OK);
        metrics.recordRenew(StatusCode.NOT_HELD);
        metrics.recordLeaseExpired(2);
        metrics.recordAtomic(LockType.LOCK_TYPE_ATOMIC_LONG, AtomicOp.ATOMIC_CAS, StatusCode.OK);
        metrics.recordAtomic(LockType.LOCK_TYPE_ATOMIC_INTEGER, AtomicOp.ATOMIC_GET, StatusCode.OK);
        metrics.recordAtomic(LockType.LOCK_TYPE_ATOMIC_BOOLEAN, AtomicOp.ATOMIC_SET, StatusCode.INVALID_REQUEST);
        // v6：有值引用形态各一线（成功与超限拒绝）。
        metrics.recordAtomic(LockType.LOCK_TYPE_ATOMIC_REFERENCE, AtomicOp.ATOMIC_CAS, StatusCode.OK);
        metrics.recordAtomic(LockType.LOCK_TYPE_ATOMIC_REFERENCE, AtomicOp.ATOMIC_SET, StatusCode.INVALID_REQUEST);
        metrics.recordBarrier("await", StatusCode.QUEUED);
        metrics.recordBarrier("action_done", StatusCode.BARRIER_BROKEN);
        // v7：队列操作计数线（op 词表五值各一形态 + 双"满"分轨）。
        metrics.recordQueue(io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_PUT,
                StatusCode.OK);
        metrics.recordQueue(io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_TAKE,
                StatusCode.DENIED);
        metrics.recordQueue(io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_DRAIN,
                StatusCode.QUEUED);
        metrics.recordQueue(io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_PEEK,
                StatusCode.OK);
        metrics.recordQueue(io.github.lamspace.openlatch.protocol.QueueOp.QUEUE_OP_SIZE,
                StatusCode.OVERLOADED);
        // v8：topic 操作计数线（op 词表三值 + 在带裁决 REJECT_SUBSCRIBERS）。
        metrics.recordTopic(io.github.lamspace.openlatch.protocol.TopicOp.TOPIC_OP_SUBSCRIBE,
                StatusCode.OK);
        metrics.recordTopic(io.github.lamspace.openlatch.protocol.TopicOp.TOPIC_OP_SUBSCRIBE,
                StatusCode.REJECT_SUBSCRIBERS);
        metrics.recordTopic(io.github.lamspace.openlatch.protocol.TopicOp.TOPIC_OP_UNSUBSCRIBE,
                StatusCode.OK);
        metrics.recordTopic(io.github.lamspace.openlatch.protocol.TopicOp.TOPIC_OP_PUBLISH,
                StatusCode.OK);
        metrics.recordTopic(io.github.lamspace.openlatch.protocol.TopicOp.TOPIC_OP_PUBLISH,
                StatusCode.INVALID_REQUEST);
        metrics.recordTopic(io.github.lamspace.openlatch.protocol.TopicOp.TOPIC_OP_PUBLISH,
                StatusCode.NOT_LEADER);
        // 丢弃计数经登记表监听器回发（drop-newest 不入拒绝面）。
        metrics.recordTopicDropped(1);
        // v9：signal 家族计数线（op 词表三值 + NOT_HELD 权限线/NOT_LEADER/
        // INVALID_REQUEST/SESSION_EXPIRED 在带可达码形）。await 折叠不记本线——
        // 其计数落 acquire 既有线（折叠口径钉死，运行时归数由 gating 系用例以
        // 真流量断言，本词表不重复记 acquire 臂以免扰动计时器样本基数）。
        io.github.lamspace.openlatch.protocol.ConditionOp sig =
                io.github.lamspace.openlatch.protocol.ConditionOp.CONDITION_OP_SIGNAL;
        io.github.lamspace.openlatch.protocol.ConditionOp sigAll =
                io.github.lamspace.openlatch.protocol.ConditionOp.CONDITION_OP_SIGNAL_ALL;
        io.github.lamspace.openlatch.protocol.ConditionOp leave =
                io.github.lamspace.openlatch.protocol.ConditionOp.CONDITION_OP_LEAVE;
        metrics.recordCondition(sig, StatusCode.OK);
        metrics.recordCondition(sig, StatusCode.NOT_HELD);
        metrics.recordCondition(sigAll, StatusCode.OK);
        metrics.recordCondition(leave, StatusCode.OK);
        metrics.recordCondition(sig, StatusCode.NOT_LEADER);
        metrics.recordCondition(sig, StatusCode.INVALID_REQUEST);
        metrics.recordCondition(leave, StatusCode.SESSION_EXPIRED);
        // v10：phaser 计数线（op 词表七值各一形态；两超限共码 OVERLOADED 以
        // op 分轨——register 线=配额、await_advance 线=深度；QUEUED 可达面
        // 与 condition 恒不可达恰成分轨对照）。
        metrics.recordPhaser(io.github.lamspace.openlatch.protocol.PhaserOp.PHASER_OP_REGISTER,
                StatusCode.OK);
        metrics.recordPhaser(io.github.lamspace.openlatch.protocol.PhaserOp.PHASER_OP_REGISTER,
                StatusCode.OVERLOADED);
        metrics.recordPhaser(io.github.lamspace.openlatch.protocol.PhaserOp.PHASER_OP_ARRIVE,
                StatusCode.OK);
        metrics.recordPhaser(io.github.lamspace.openlatch.protocol.PhaserOp
                .PHASER_OP_ARRIVE_AND_AWAIT, StatusCode.QUEUED);
        metrics.recordPhaser(io.github.lamspace.openlatch.protocol.PhaserOp
                .PHASER_OP_ARRIVE_AND_DEREGISTER, StatusCode.OK);
        metrics.recordPhaser(io.github.lamspace.openlatch.protocol.PhaserOp
                .PHASER_OP_AWAIT_ADVANCE, StatusCode.OVERLOADED);
        metrics.recordPhaser(io.github.lamspace.openlatch.protocol.PhaserOp.PHASER_OP_CANCEL,
                StatusCode.OK);
        metrics.recordPhaser(io.github.lamspace.openlatch.protocol.PhaserOp.PHASER_OP_QUERY,
                StatusCode.INVALID_REQUEST);
        metrics.recordPhaser(io.github.lamspace.openlatch.protocol.PhaserOp.PHASER_OP_ARRIVE,
                StatusCode.NOT_LEADER);
        // v11：timer 计数线（op 词表五值各一形态；QUEUED/OVERLOADED 仅 await
        // op 可达；DENIED 经 timer AWAIT 终态首次回到可达面——既有码值新可达线
        // 的词表证据；fired 为唤醒集合事件计数）。
        metrics.recordTimer(io.github.lamspace.openlatch.protocol.TimerOp.TIMER_OP_SCHEDULE,
                StatusCode.OK);
        metrics.recordTimer(io.github.lamspace.openlatch.protocol.TimerOp
                .TIMER_OP_SCHEDULE, StatusCode.INVALID_REQUEST);
        metrics.recordTimer(io.github.lamspace.openlatch.protocol.TimerOp.TIMER_OP_DISARM,
                StatusCode.OK);
        metrics.recordTimer(io.github.lamspace.openlatch.protocol.TimerOp.TIMER_OP_AWAIT,
                StatusCode.QUEUED);
        metrics.recordTimer(io.github.lamspace.openlatch.protocol.TimerOp.TIMER_OP_AWAIT,
                StatusCode.OVERLOADED);
        metrics.recordTimer(io.github.lamspace.openlatch.protocol.TimerOp.TIMER_OP_AWAIT,
                StatusCode.DENIED);
        metrics.recordTimer(io.github.lamspace.openlatch.protocol.TimerOp.TIMER_OP_CANCEL,
                StatusCode.OK);
        metrics.recordTimer(io.github.lamspace.openlatch.protocol.TimerOp.TIMER_OP_QUERY,
                StatusCode.OK);
        metrics.recordTimer(io.github.lamspace.openlatch.protocol.TimerOp.TIMER_OP_AWAIT,
                StatusCode.NOT_LEADER);
        metrics.recordTimerFired(2);
        // is_leader 由集群装配注册（08c），本测试经角色绑定入口补齐词表覆盖。
        metrics.bindClusterIsLeader(7, () -> true);
        scrape = metrics.registry().scrape();
    }

    @Test
    void gaugeLineNames() {
        assertThat(scrape).contains("openlatch_server_locks_held{type=\"lock\"}");
        assertThat(scrape).contains("openlatch_server_locks_held{type=\"semaphore\"}");
        assertThat(scrape).contains("openlatch_server_waiters");
        assertThat(scrape).contains("openlatch_server_sessions");
        assertThat(scrape).contains("openlatch_server_queue_depth_max");
        // v7：元素深度线与等待队深线两口径并存且不同名。
        assertThat(scrape).contains("openlatch_server_elements_depth_max");
        assertThat(scrape).contains("openlatch_cluster_is_leader{node_id=\"7\"} 1");
    }

    @Test
    void counterLineNamesCarryTotalSuffixOnce() {
        assertThat(scrape).contains("openlatch_server_acquire_total{status=\"OK\"}");
        assertThat(scrape).contains("openlatch_server_release_total{status=\"OK\"}");
        assertThat(scrape).contains("openlatch_server_renew_total{status=\"NOT_HELD\"}");
        assertThat(scrape).contains("openlatch_server_lease_expired_total 2");
        assertThat(scrape).contains("openlatch_server_atomic_total{kind=\"long\",op=\"cas\",status=\"OK\"} 1");
        assertThat(scrape).contains("openlatch_server_atomic_total{kind=\"integer\",op=\"get\",status=\"OK\"} 1");
        assertThat(scrape).contains("openlatch_server_atomic_total{kind=\"boolean\",op=\"set\",status=\"INVALID_REQUEST\"} 1");
        assertThat(scrape).contains("openlatch_server_atomic_total{kind=\"reference\",op=\"cas\",status=\"OK\"} 1");
        assertThat(scrape).contains("openlatch_server_atomic_total{kind=\"reference\",op=\"set\",status=\"INVALID_REQUEST\"} 1");
        assertThat(scrape).contains("openlatch_server_barrier_total{op=\"await\",status=\"QUEUED\"} 1");
        assertThat(scrape).contains("openlatch_server_barrier_total{op=\"action_done\",status=\"BARRIER_BROKEN\"} 1");
        assertThat(scrape).contains("openlatch_server_queue_total{op=\"put\",status=\"OK\"} 1");
        assertThat(scrape).contains("openlatch_server_queue_total{op=\"take\",status=\"DENIED\"} 1");
        assertThat(scrape).contains("openlatch_server_queue_total{op=\"drain\",status=\"QUEUED\"} 1");
        assertThat(scrape).contains("openlatch_server_queue_total{op=\"peek\",status=\"OK\"} 1");
        assertThat(scrape).contains("openlatch_server_queue_total{op=\"size\",status=\"OVERLOADED\"} 1");
        // 不得出现 *_total_total 双后缀。
        assertThat(scrape).doesNotContain("_total_total");
    }

    /** v8：topic 三命名点线路名（counter 线、丢弃线不入拒绝码形面、订阅 gauge）。 */
    @Test
    void topicLineNames() {
        assertThat(scrape).contains("openlatch_server_topic_total{op=\"subscribe\",status=\"OK\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_topic_total{op=\"subscribe\",status=\"REJECT_SUBSCRIBERS\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_topic_total{op=\"unsubscribe\",status=\"OK\"} 1");
        assertThat(scrape).contains("openlatch_server_topic_total{op=\"publish\",status=\"OK\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_topic_total{op=\"publish\",status=\"INVALID_REQUEST\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_topic_total{op=\"publish\",status=\"NOT_LEADER\"} 1");
        assertThat(scrape).contains("openlatch_server_topic_dropped_total 1");
        assertThat(scrape).contains("openlatch_server_topic_subscribers_max 0");
        // waiters 口径不含订阅者（三口径分离的负向守门）。
        assertThat(scrape).doesNotContain("topic_total_total");
        assertThat(scrape).doesNotContain("topic.dropped");
    }

    /**
     * v11：timer 两命名点线路名——{@code timer.total{op,status}} 按五操作词表
     * 归数（{@code QUEUED}/{@code OVERLOADED} 仅 await 线；{@code DENIED} 经
     * timer AWAIT 终态首次可达——既有码值新可达线的词表证据）；
     * {@code timer.fired.total} 为唤醒集合事件计数（唤醒面非到期面）。timer
     * 不设单键峰值 Gauge（装载态二值——"无 timer_*_max 线"为口径缺位避让的
     * 线路面证据）。恒不可达面负向守门：{@code BARRIER_BROKEN}/{@code
     * REJECT_SUBSCRIBERS}/{@code NOT_HELD} 对 timer 无线。
     */
    @Test
    void timerLineNames() {
        assertThat(scrape)
                .contains("openlatch_server_timer_total{op=\"schedule\",status=\"OK\"} 1");
        assertThat(scrape).contains(
                "openlatch_server_timer_total{op=\"schedule\",status=\"INVALID_REQUEST\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_timer_total{op=\"disarm\",status=\"OK\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_timer_total{op=\"await\",status=\"QUEUED\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_timer_total{op=\"await\",status=\"OVERLOADED\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_timer_total{op=\"await\",status=\"DENIED\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_timer_total{op=\"cancel\",status=\"OK\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_timer_total{op=\"query\",status=\"OK\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_timer_total{op=\"await\",status=\"NOT_LEADER\"} 1");
        assertThat(scrape).contains("openlatch_server_timer_fired_total 2");
        // 避让注记的线路面证据：无单键峰值线；恒不可达码形无线。
        assertThat(scrape).doesNotContain("openlatch_server_timer_armed_max");
        assertThat(scrape).doesNotContain("timer_total{op=\"await\",status=\"BARRIER_BROKEN\"}");
        assertThat(scrape).doesNotContain("timer_total{op=\"schedule\",status=\"NOT_HELD\"}");
    }

    /**
     * v10：phaser 两命名点线路名——{@code phaser.total{op,status}} 按七操作词表
     * 归数、两超限 {@code OVERLOADED} 以 op 分轨（register 配额线 vs
     * await_advance 深度线）；{@code phaser.parties.registered.max} gauge 为
     * 注册 party 单键峰值（应到集合口径，非等待口径——五口径互引）。
     * 恒不可达面负向守门：{@code DENIED}/{@code BARRIER_BROKEN}/
     * {@code REJECT_SUBSCRIBERS}/{@code NOT_HELD} 对 phaser 无线（无立即式、
     * 无破相、无持有概念）。
     */
    @Test
    void phaserLineNames() {
        assertThat(scrape)
                .contains("openlatch_server_phaser_total{op=\"register\",status=\"OK\"} 1");
        assertThat(scrape).contains(
                "openlatch_server_phaser_total{op=\"register\",status=\"OVERLOADED\"} 1");
        assertThat(scrape).contains(
                "openlatch_server_phaser_total{op=\"await_advance\",status=\"OVERLOADED\"} 1");
        assertThat(scrape).contains(
                "openlatch_server_phaser_total{op=\"arrive_and_await\",status=\"QUEUED\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_phaser_total{op=\"arrive\",status=\"OK\"} 1");
        assertThat(scrape).contains(
                "openlatch_server_phaser_total{op=\"arrive_and_deregister\",status=\"OK\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_phaser_total{op=\"cancel\",status=\"OK\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_phaser_total{op=\"query\",status=\"INVALID_REQUEST\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_phaser_total{op=\"arrive\",status=\"NOT_LEADER\"} 1");
        assertThat(scrape).contains("openlatch_server_phaser_parties_registered_max 0");
        // 恒不可达面负向守门。
        assertThat(scrape).doesNotContain("phaser_total{op=\"register\",status=\"DENIED\"");
        assertThat(scrape).doesNotContain("phaser_total{op=\"register\",status=\"BARRIER_BROKEN\"");
        assertThat(scrape).doesNotContain("phaser_total{op=\"register\",status=\"NOT_HELD\"");
        assertThat(scrape).doesNotContain("phaser_total{op=\"register\",status=\"REJECT_SUBSCRIBERS\"");
        assertThat(scrape).doesNotContain("phaser_total_total");
    }

    /**
     * v9：condition 两命名点线路名——{@code condition.total{op,status}} 计数线
     * 按三操作词表与在带可达码形归数（{@code NOT_HELD} 权限线单列）；
     * {@code condition.waiters.max} gauge 为抓取时刻单键条件等待峰值（不含
     * 搬运入队项）。恒不可达面负向守门：{@code QUEUED}/{@code DENIED}/
     * {@code OVERLOADED} 对 condition 无线（等待满拒绝产生于 await 折叠侧，
     * 计数落 {@code acquire_total{status="OVERLOADED"}} 既有线——折叠口径的
     * 线路证据）。四口径（等待队深/元素/订阅/条件等待）命名互引防混读。
     */
    @Test
    void conditionLineNames() {
        assertThat(scrape)
                .contains("openlatch_server_condition_total{op=\"signal\",status=\"OK\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_condition_total{op=\"signal\",status=\"NOT_HELD\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_condition_total{op=\"signal_all\",status=\"OK\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_condition_total{op=\"leave\",status=\"OK\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_condition_total{op=\"signal\",status=\"NOT_LEADER\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_condition_total{op=\"signal\",status=\"INVALID_REQUEST\"} 1");
        assertThat(scrape)
                .contains("openlatch_server_condition_total{op=\"leave\",status=\"SESSION_EXPIRED\"} 1");
        assertThat(scrape).contains("openlatch_server_condition_waiters_max 0");
        // await 折叠归线证据（负向面）：condition 线上永无等待满/挂起码形——
        // 超限臂计数落 acquire 既有线（运行时归数由 gating 系用例真流量断言）。
        assertThat(scrape)
                .doesNotContain("openlatch_server_condition_total{op=\"signal\",status=\"QUEUED\"");
        assertThat(scrape)
                .doesNotContain("openlatch_server_condition_total{op=\"signal\",status=\"OVERLOADED\"");
        assertThat(scrape)
                .doesNotContain("openlatch_server_condition_total{op=\"signal\",status=\"DENIED\"");
        assertThat(scrape).doesNotContain("condition_total_total");
        // await op 值在 condition 线上不存在（折叠边界的计数面证据，防"条件自成
        // 一线"口径漂移；barrier 线的 op="await" 为既有词表，不属本负向面）。
        assertThat(scrape).doesNotContain("condition_total{op=\"await\"");
    }

    @Test
    void timerLineNamesFoldSecondsAndResultLabel() {
        assertThat(scrape).contains("openlatch_server_acquire_duration_seconds_count{result=\"granted\"} 1");
        assertThat(scrape).contains("openlatch_server_acquire_duration_seconds_count{result=\"queued\"} 1");
        assertThat(scrape).contains("openlatch_server_acquire_duration_seconds_count{result=\"denied\"} 1");
        assertThat(scrape).contains("openlatch_server_acquire_duration_seconds_sum{result=\"granted\"} 0.002");
        // 新 Prometheus 客户端的标签序为插入序（result 先于 le）。
        assertThat(scrape).contains("openlatch_server_acquire_duration_seconds_bucket{result=\"granted\",le=");
    }
}
