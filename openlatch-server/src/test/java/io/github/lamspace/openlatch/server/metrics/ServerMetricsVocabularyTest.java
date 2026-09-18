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
        metrics.bindStandaloneGauges(core, new ServerSessionRegistry());
        metrics.recordAcquire(StatusCode.OK, 2_000_000L);
        metrics.recordAcquire(StatusCode.QUEUED, 500_000L);
        metrics.recordAcquire(StatusCode.DENIED, 300_000L);
        metrics.recordRelease(StatusCode.OK);
        metrics.recordRenew(StatusCode.NOT_HELD);
        metrics.recordLeaseExpired(2);
        metrics.recordAtomic(LockType.LOCK_TYPE_ATOMIC_LONG, AtomicOp.ATOMIC_CAS, StatusCode.OK);
        metrics.recordAtomic(LockType.LOCK_TYPE_ATOMIC_INTEGER, AtomicOp.ATOMIC_GET, StatusCode.OK);
        metrics.recordAtomic(LockType.LOCK_TYPE_ATOMIC_BOOLEAN, AtomicOp.ATOMIC_SET, StatusCode.INVALID_REQUEST);
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
        // 不得出现 *_total_total 双后缀。
        assertThat(scrape).doesNotContain("_total_total");
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
