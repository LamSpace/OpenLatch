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

package io.github.lamspace.openlatch.client.internal;

import io.github.lamspace.openlatch.client.LockLostException;
import io.github.lamspace.openlatch.client.LockType;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.LeaseRenewResponse;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.HashedWheelTimer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * 看门狗单测（tasks 6.1–6.5）：周期续租、明确失效错误即时失锁、
 * 连续两次超时失锁、断连跳过不计数（D5）、解锁注销。
 */
class WatchdogTest {

    /** 测试租约（毫秒）：续租周期为其三分之一（100ms）。 */
    private static final long LEASE_MS = 300;
    /** 测试锁键。 */
    private static final String KEY = "watched";
    /** 测试线程标识。 */
    private static final long THREAD_ID = 3L;
    /** 测试租约凭据。 */
    private static final long TOKEN = 42L;

    /** 细刻度定时器。 */
    private HashedWheelTimer timer;
    /** 内存通道。 */
    private EmbeddedChannel channel;
    /** 会话上下文。 */
    private SessionContext session;
    /** 多路复用器。 */
    private RequestMultiplexer multiplexer;
    /** 持锁簿记。 */
    private HeldLockRegistry registry;
    /** 连接状态开关（模拟）。 */
    private AtomicBoolean connectionActive;
    /** 失锁事件队列。 */
    private BlockingQueue<HeldLockRegistry.HeldEntry> lostEntries;
    /** 被测对象。 */
    private Watchdog watchdog;

    /**
     * 装配：真实多路复用器 + 内存通道，每请求超时取 2000ms（超时用例单独调低）。
     */
    @BeforeEach
    void setUp() {
        setUpWatchdog(2000);
    }

    /**
     * 释放资源。
     */
    @AfterEach
    void tearDown() {
        timer.stop();
        channel.close();
    }

    /** 周期续租：成功后持续发送续租请求并刷新本地时间戳。 */
    @Test
    void renewsPeriodicallyOnSuccess() throws Exception {
        HeldLockRegistry.HeldEntry entry = registerEntry();
        watchdog.start(entry);

        Envelope first = awaitOutboundRenew();
        assertThat(first.getLeaseRenewRequest().getKey()).isEqualTo(KEY);
        assertThat(first.getLeaseRenewRequest().getLeaseToken()).isEqualTo(TOKEN);
        assertThat(first.getLeaseRenewRequest().getLeaseMs()).isEqualTo(LEASE_MS);
        respondRenew(first.getRequestId(), StatusCode.OK);
        long renewedAt = entry.lastRenewAtMs();

        // 下一周期继续续租
        Envelope second = awaitOutboundRenew();
        respondRenew(second.getRequestId(), StatusCode.OK);
        assertThat(entry.lastRenewAtMs()).isGreaterThanOrEqualTo(renewedAt);
    }

    /** 明确失效错误（凭证不匹配）即时失锁。 */
    @Test
    void invalidTokenCausesImmediateLoss() throws Exception {
        HeldLockRegistry.HeldEntry entry = registerEntry();
        watchdog.start(entry);

        Envelope renew = awaitOutboundRenew();
        respondRenew(renew.getRequestId(), StatusCode.INVALID_TOKEN);

        assertThat(lostEntries.poll(2, TimeUnit.SECONDS)).isSameAs(entry);
        assertThat(registry.get(KEY, THREAD_ID)).isNull();
        // 失锁后不再续租
        Thread.sleep(250);
        assertThat(channel.outboundMessages()).isEmpty();
    }

    /** 明确失效错误（会话失效）即时失锁。 */
    @Test
    void sessionExpiredCausesImmediateLoss() throws Exception {
        HeldLockRegistry.HeldEntry entry = registerEntry();
        watchdog.start(entry);

        Envelope renew = awaitOutboundRenew();
        respondRenew(renew.getRequestId(), StatusCode.SESSION_EXPIRED);

        assertThat(lostEntries.poll(2, TimeUnit.SECONDS)).isSameAs(entry);
    }

    /** OVERLOADED 计为瞬时失败：单次重试，连续两次判定失锁（spec「过载错误计入连续失败」）。 */
    @Test
    void overloadedCountsTowardConsecutiveFailures() throws Exception {
        HeldLockRegistry.HeldEntry entry = registerEntry();
        watchdog.start(entry);

        Envelope first = awaitOutboundRenew();
        respondRenew(first.getRequestId(), StatusCode.OVERLOADED);
        assertThat(lostEntries).isEmpty();

        Envelope second = awaitOutboundRenew();
        respondRenew(second.getRequestId(), StatusCode.OVERLOADED);
        assertThat(lostEntries.poll(2, TimeUnit.SECONDS)).isSameAs(entry);
    }

    /** 瞬时失败被成功续租隔断：计数重置，非连续过载不失锁。 */
    @Test
    void transientFailureThenSuccessResetsCount() throws Exception {
        HeldLockRegistry.HeldEntry entry = registerEntry();
        watchdog.start(entry);

        respondRenew(awaitOutboundRenew().getRequestId(), StatusCode.OVERLOADED);
        respondRenew(awaitOutboundRenew().getRequestId(), StatusCode.OK);
        respondRenew(awaitOutboundRenew().getRequestId(), StatusCode.OVERLOADED);
        assertThat(lostEntries).isEmpty();

        respondRenew(awaitOutboundRenew().getRequestId(), StatusCode.OK);
        assertThat(lostEntries).isEmpty();
    }

    /** 单次续租超时：下一周期重试，不失锁；恢复成功后计数重置。 */
    @Test
    void singleTimeoutRetriesNextCycle() throws Exception {
        tearDown();
        setUpWatchdog(50);
        HeldLockRegistry.HeldEntry entry = registerEntry();
        watchdog.start(entry);

        awaitOutboundRenew(); // 第一次续租：不响应 → 超时
        Envelope second = awaitOutboundRenew();
        assertThat(lostEntries).isEmpty();

        respondRenew(second.getRequestId(), StatusCode.OK);
        assertThat(lostEntries).isEmpty();
    }

    /** 连续两次续租超时：判定失锁。 */
    @Test
    void consecutiveTwoTimeoutsCauseLoss() throws Exception {
        tearDown();
        setUpWatchdog(50);
        HeldLockRegistry.HeldEntry entry = registerEntry();
        watchdog.start(entry);

        awaitOutboundRenew(); // 第一次超时
        awaitOutboundRenew(); // 第二次超时

        assertThat(lostEntries.poll(2, TimeUnit.SECONDS)).isSameAs(entry);
    }

    /** 断连期间跳过续租且不计数（D5）；恢复后继续。 */
    @Test
    void disconnectedSkipsWithoutCounting() throws Exception {
        HeldLockRegistry.HeldEntry entry = registerEntry();
        watchdog.start(entry);

        connectionActive.set(false);
        Thread.sleep(250); // 跨越两个周期
        assertThat(channel.outboundMessages()).isEmpty();
        assertThat(lostEntries).isEmpty();

        connectionActive.set(true);
        Envelope renew = awaitOutboundRenew();
        respondRenew(renew.getRequestId(), StatusCode.OK);
        assertThat(lostEntries).isEmpty();
    }

    /** stop 后不再续租（完全释放注销路径）。 */
    @Test
    void stopCancelsRenewals() throws Exception {
        HeldLockRegistry.HeldEntry entry = registerEntry();
        watchdog.start(entry);
        Envelope renew = awaitOutboundRenew();
        respondRenew(renew.getRequestId(), StatusCode.OK);

        watchdog.stop(entry);
        Thread.sleep(300);
        assertThat(channel.outboundMessages()).isEmpty();
    }

    /**
     * 以给定每请求超时重建被测对象。
     *
     * @param requestTimeoutMs 每请求超时（毫秒）
     */
    private void setUpWatchdog(long requestTimeoutMs) {
        timer = new HashedWheelTimer(r -> {
            Thread t = new Thread(r, "test-watchdog-timer");
            t.setDaemon(true);
            return t;
        }, 10, TimeUnit.MILLISECONDS);
        channel = new EmbeddedChannel();
        session = new SessionContext(1L);
        multiplexer = new RequestMultiplexer(timer, () -> channel, () -> session);
        registry = new HeldLockRegistry();
        connectionActive = new AtomicBoolean(true);
        lostEntries = new LinkedBlockingQueue<>();
        watchdog = new Watchdog(timer, multiplexer, registry, requestTimeoutMs,
                connectionActive::get, (entry, cause) -> lostEntries.offer(entry));
    }

    /**
     * 登记测试持锁条目。
     *
     * @return 持锁条目
     */
    private HeldLockRegistry.HeldEntry registerEntry() {
        return registry.register(KEY, THREAD_ID, LockType.REENTRANT, TOKEN, LEASE_MS, 1L,
                System.currentTimeMillis());
    }

    /**
     * 等待一条出站续租请求。
     *
     * @return 续租信封
     * @throws InterruptedException 等待被中断
     */
    private Envelope awaitOutboundRenew() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 2000;
        while (System.currentTimeMillis() < deadline) {
            Envelope env = channel.readOutbound();
            if (env != null && env.getType() == MessageType.LEASE_RENEW) {
                return env;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("no renew request within 2s");
    }

    /**
     * 注入续租响应。
     *
     * @param requestId 请求 id
     * @param status    状态码
     */
    private void respondRenew(long requestId, StatusCode status) {
        multiplexer.onResponse(Envelope.newBuilder()
                .setProtocolVersion(1)
                .setType(MessageType.LEASE_RENEW)
                .setRequestId(requestId)
                .setLeaseRenewResponse(LeaseRenewResponse.newBuilder().setStatus(status))
                .build());
    }
}
