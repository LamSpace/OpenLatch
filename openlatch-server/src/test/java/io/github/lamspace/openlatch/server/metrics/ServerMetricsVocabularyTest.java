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
