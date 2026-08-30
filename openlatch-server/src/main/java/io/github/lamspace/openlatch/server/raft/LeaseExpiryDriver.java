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

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.github.lamspace.openlatch.protocol.raft.ApplyResult;
import io.github.lamspace.openlatch.protocol.raft.ExpirePayload;
import io.github.lamspace.openlatch.protocol.raft.RaftEntryType;
import io.github.lamspace.openlatch.protocol.raft.RaftLogEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * 租约到期驱动（详设 §4.3.1/P2-09）：到期判断只发生在 Leader——周期扫描
 * 影子表投影中已到期（且未被在途条目覆盖）的持锁，逐条提交
 * {@code LEASE_EXPIRE_ENTRY(key, leaseToken)}；实际释放由全副本在条目
 * 应用时完成（回放守卫见 {@link LockStateMachineCore}）。
 *
 * <p><b>为什么以影子表为输入</b>：集群模式下 {@code CoreEngine.expireDue()}
 * 只允许在应用线程、条目时刻下被调用（design D12"引擎唯一漏斗"）——
 * 扫描若直驱引擎会引入"Leader 本地提前释放"的非复制迁移。影子表投影
 * 与引擎堆由同一应用序列双写（到期集恒等），是合法的非侵入观察面。
 *
 * <p><b>在途抑制</b>：同一 key 在到期条目完成应用（或被判定）之前不重复
 * 提交（{@link #onEntryApplied} 解除）；提交失败同样解除，下个扫描周期
 * 自然重试。切换 Leader 时抑制集整体作废（design D5 的 NOOP 探针同此
 * 生命周期，由 SessionCoordinator 另行管理）。
 *
 * <p><b>线程模型</b>：单守护调度线程执行扫描（周期
 * {@code leaseTickIntervalMs}，非 Leader 时为空转短路）；{@link #onEntryApplied}
 * 在状态机应用线程回调——两侧仅经并发容器交接。
 *
 * <p><b>failover 语义</b>：新 Leader 当选即触发首扫（{@link #onLeadershipGained()}），
 * 已复制的到期时刻按其物理时钟续驱——到期误差 ≤ 一个扫描周期 + 切换耗时
 * （详设 §12 风险 2 的可接受声明）。
 */
public final class LeaseExpiryDriver implements AutoCloseable {

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(LeaseExpiryDriver.class);

    /** 装配子系统（角色查询）。 */
    private final RaftSubsystem subsystem;
    /** 语义内核（影子表扫描输入）。 */
    private final LockStateMachineCore kernel;
    /** 提交通道。 */
    private final ReplicationGateway gateway;
    /** 扫描周期（毫秒）。 */
    private final long tickMs;
    /** 在途抑制集：key → 已提交未落地的条目 token。 */
    private final Map<String, Long> inflight = new ConcurrentHashMap<>();
    /** 调度器，start 后非空。 */
    private volatile ScheduledExecutorService scheduler;

    /**
     * 构造到期驱动（不启动）。
     *
     * @param subsystem Raft 子系统
     * @param kernel    状态机内核
     * @param gateway   复制网关
     * @param tickMs    扫描周期（毫秒，&gt;0；生产取 {@code leaseTickIntervalMs}）
     */
    public LeaseExpiryDriver(RaftSubsystem subsystem, LockStateMachineCore kernel,
                             ReplicationGateway gateway, long tickMs) {
        this.subsystem = subsystem;
        this.kernel = kernel;
        this.gateway = gateway;
        this.tickMs = tickMs;
    }

    /**
     * 启动周期扫描（幂等；仅注册调度，Leader 角色检查在每次 tick 内）。
     */
    public void start() {
        if (scheduler != null) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "openlatch-lease-expiry-driver");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleAtFixedRate(this::scanSafely, tickMs, tickMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 单次扫描：非 Leader 短路；对投影中到期且不在抑制集的 key 提交到期条目。
     */
    private void scanSafely() {
        try {
            if (!subsystem.isLeader()) {
                return;
            }
            long now = System.currentTimeMillis();
            for (Map.Entry<String, ShadowTable.HeldRef> en : kernel.shadow().heldEntries().entrySet()) {
                ShadowTable.HeldRef ref = en.getValue();
                if (ref.expiresAtMs() > now) {
                    continue;
                }
                if (inflight.putIfAbsent(en.getKey(), ref.leaseToken()) != null) {
                    continue; // 在途抑制
                }
                ByteString payload;
                try {
                    payload = ExpirePayload.newBuilder()
                            .setKey(en.getKey()).setLeaseToken(ref.leaseToken())
                            .build().toByteString();
                } catch (RuntimeException e) {
                    inflight.remove(en.getKey());
                    continue;
                }
                final String key = en.getKey();
                final long token = ref.leaseToken();
                RaftLogEntry entry = RaftLogEntry.newBuilder()
                        .setType(RaftEntryType.LEASE_EXPIRE_ENTRY)
                        .setSeq(nextDriverSeq())
                        .setWallClockMs(now)
                        .setCommandPayload(payload)
                        .build();
                gateway.submit(entry).whenComplete((r, err) -> {
                    if (err != null) {
                        inflight.remove(key, token); // 提交失败：下周期重试
                    }
                    // 成功路径的解除在 onEntryApplied（落地即解除）。
                });
            }
        } catch (RuntimeException e) {
            log.error("expiry scan failed", e);
        }
    }

    /** 驱动条目序号发生器（与 gateway 客户端提交序号空间隔离：高位标记）。 */
    private final java.util.concurrent.atomic.AtomicLong driverSeq =
            new java.util.concurrent.atomic.AtomicLong(1L << 62);

    /**
     * 取用驱动侧专用序号。
     *
     * @return 驱动侧专用序号（{@code 1L<<62} 起，与 gateway 客户端 seq 不冲突）
     */
    private long nextDriverSeq() {
        return driverSeq.getAndIncrement();
    }

    /**
     * 条目应用通知：到期条目落地（无论守卫是否释放）即解除该 key 的在途抑制。
     *
     * @param entry  已应用条目
     * @param result 回执
     */
    public void onEntryApplied(RaftLogEntry entry, ApplyResult result) {
        if (entry.getType() != RaftEntryType.LEASE_EXPIRE_ENTRY) {
            return;
        }
        try {
            ExpirePayload p = ExpirePayload.parseFrom(entry.getCommandPayload());
            inflight.remove(p.getKey(), p.getLeaseToken());
        } catch (InvalidProtocolBufferException e) {
            log.warn("expire entry unparsable on applied hook (seq={})", entry.getSeq());
        }
    }

    /**
     * 新任期开始：在途抑制集整体作废（旧任目的条目可能永远不会落地），
     * 并立即触发一次首扫补偿切换窗口的漏扫。
     */
    public void onLeadershipGained() {
        inflight.clear();
        scanSafely();
    }

    /**
     * 关停扫描线程（子系统关停前调用）。幂等。
     */
    @Override
    public void close() {
        ScheduledExecutorService s = scheduler;
        scheduler = null;
        if (s != null) {
            s.shutdownNow();
        }
    }
}
