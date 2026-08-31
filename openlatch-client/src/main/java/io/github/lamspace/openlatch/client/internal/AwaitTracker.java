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

import io.github.lamspace.openlatch.client.AcquireSpec;
import io.github.lamspace.openlatch.client.LockAcquisitionTimeoutException;
import io.github.lamspace.openlatch.client.LockGrant;
import io.github.lamspace.openlatch.client.OpenLatchException;
import io.github.lamspace.openlatch.protocol.AcquireResponse;
import io.github.lamspace.openlatch.protocol.AwaitNotify;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.ReleaseRequest;
import io.github.lamspace.openlatch.protocol.StatusCode;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * 等待跟踪器（详设 §6.5）：管理排队中的获取请求全生命周期。
 *
 * <p><b>状态机</b>：
 * <pre>
 * startAcquire → 发送 ACQUIRE
 *   ├─ OK       → 完成 future，登记授予，等待结束
 *   ├─ QUEUED   → 登记挂起，等待通知
 *   ├─ DENIED   → future 失败（LockDeniedException），等待结束
 *   └─ 错误码   → future 失败（携带状态码），等待结束
 * 挂起中：
 *   ├─ AWAIT_NOTIFY → 以同一 requestId 重发（服务端幂等，§4.8）
 *   ├─ 重发响应 OK/QUEUED/错误 → 同上各分支
 *   ├─ 重发请求超时 → 仅结束该次重发，保持挂起等待下一次通知（design.md D1）
 *   └─ 用户总超时 → future 失败（LockAcquisitionTimeoutException），等待结束
 * </pre>
 *
 * <p><b>补偿归还（design.md D3）</b>：等待以任何方式结束后，其 {@code requestId}
 * 在保留窗口内维持 {@code requestId → (key, threadId)} 映射；无挂起项匹配的
 * 授予响应（孤儿 OK）到达时发送补偿 {@code RELEASE} 归还，防止锁泄漏。
 * 覆盖三种孤儿时序：重复通知双授予、总超时后在途重发被授予、断连外的一切
 * 授予-放弃竞争。
 *
 * <p><b>并发</b>：所有终止性迁移（完成/失败用户 future）在对应等待条目的
 * 内置锁内完成并以其 future 是否已完成作最终仲裁，保证每个请求恰有一次
 * 终态、重复授予必被归还。
 */
public final class AwaitTracker {

    /** 日志器。 */
    private static final Logger log = LoggerFactory.getLogger(AwaitTracker.class);
    /** 已结束等待映射的额外保留缓冲（毫秒）：覆盖在途重发的请求超时窗口。 */
    private static final long RETENTION_BUFFER_MS = 1000;

    /** 挂起等待表：requestId → 等待条目。 */
    private final ConcurrentMap<Long, WaitEntry> waits = new ConcurrentHashMap<>();
    /** 已结束等待的短暂保留表：requestId → (key, threadId)，孤儿归还用。 */
    private final ConcurrentMap<Long, AbandonedEntry> recentlyAbandoned = new ConcurrentHashMap<>();
    /** 共享定时器：等待总超时与保留表清理挂于此。 */
    private final HashedWheelTimer timer;
    /** 多路复用器：重发与补偿释放经此出站。 */
    private final RequestMultiplexer multiplexer;
    /** 每请求超时（毫秒）。 */
    private final long requestTimeoutMs;
    /** 授予回调：成功授予时登记持锁与看门狗（由客户端装配）。 */
    private final BiConsumer<AcquireSpec, LockGrant> onGranted;
    /** 已结束等待映射的保留时长（毫秒）。 */
    private final long abandonedRetentionMs;
    /**
     * v2 {@code NOT_LEADER} 接管钩子（S3 客户端重定向，详设 §6.3）：
     * 默认空实现（返回 false，按错误码失败等待——v1/单机语义回归不变）。
     */
    private volatile NotLeaderHandler notLeaderHandler = request -> false;

    /**
     * {@code NOT_LEADER} 接管请求：交付客户端重定向编排所需的完整上下文。
     *
     * @param requestId     本次等待的请求 id（同会话重发复用）
     * @param envelope      原获取信封（同会话原地重发原样复用）
     * @param spec          获取参数（新会话重放时重建信封用）
     * @param userFuture    用户 future（接管后由其延续到重放终态）
     * @param leaderNodeId  服务端提示的 Leader nodeId（-1=服务端暂不知晓）
     * @param leaderAddress 服务端提示的 Leader 接入地址（空串=未提供）
     * @param remainingMs   等待总预算剩余（毫秒；非正表示预算耗尽）
     */
    public record NotLeaderRequest(long requestId, Envelope envelope, AcquireSpec spec,
            CompletableFuture<LockGrant> userFuture, long leaderNodeId, String leaderAddress,
            long remainingMs) {
    }

    /**
     * {@code NOT_LEADER} 接管处理器。
     */
    @FunctionalInterface
    public interface NotLeaderHandler {

        /**
         * 等待收到 NOT_LEADER 时被调用。
         *
         * @param request 接管上下文
         * @return {@code true} 表示接管——跟踪器仅摘除登记、不终止用户
         *         future（终态由处理器的重放链交付）；{@code false} 表示
         *         不接管，按错误码使等待失败（v1 兼容路径）
         */
        boolean onNotLeader(NotLeaderRequest request);
    }

    /**
     * 装配 {@code NOT_LEADER} 接管钩子（客户端 v2 编排调用）。
     *
     * @param handler 处理器；{@code null} 回落为不接管（v1 语义）
     */
    public void setNotLeaderHandler(NotLeaderHandler handler) {
        this.notLeaderHandler = handler == null ? request -> false : handler;
    }

    /**
     * 挂起等待条目。全部终止性迁移在条目内置锁（{@code synchronized(this)}）内完成。
     */
    private static final class WaitEntry {
        /** 请求 id，重发复用。 */
        private final long requestId;
        /** 获取请求信封，重发原样复用（幂等前提：同 requestId）。 */
        private final Envelope envelope;
        /** 获取参数，补偿释放与授予回调使用。 */
        private final AcquireSpec spec;
        /** 用户 future，恰有一次终态。 */
        private final CompletableFuture<LockGrant> userFuture;
        /** 等待总超时任务句柄；不限时等待为 {@code null}。 */
        private Timeout totalTimeoutTask;
        /** 是否曾收到 QUEUED：重发超时保持挂起（D1）的判定依据。 */
        private volatile boolean everQueued;
        /** 等待总截止时刻（epoch 毫秒；{@code Long.MAX_VALUE}=不限时/立即式）。 */
        private final long deadlineMs;

        /**
         * 创建等待条目。
         *
         * @param requestId 请求 id
         * @param envelope  获取请求信封
         * @param spec      获取参数
         * @param userFuture 用户 future
         * @param deadlineMs 等待总截止时刻（epoch 毫秒）
         */
        WaitEntry(long requestId, Envelope envelope, AcquireSpec spec,
                CompletableFuture<LockGrant> userFuture, long deadlineMs) {
            this.requestId = requestId;
            this.envelope = envelope;
            this.spec = spec;
            this.userFuture = userFuture;
            this.deadlineMs = deadlineMs;
        }
    }

    /**
     * 已结束等待的保留记录。
     *
     * @param key      锁键
     * @param threadId 申请线程标识
     */
    private record AbandonedEntry(String key, long threadId) {
    }

    /**
     * 创建等待跟踪器。
     *
     * @param timer            共享定时器
     * @param multiplexer      多路复用器
     * @param requestTimeoutMs 每请求超时（毫秒）
     * @param onGranted        授予回调
     */
    public AwaitTracker(HashedWheelTimer timer, RequestMultiplexer multiplexer,
            long requestTimeoutMs, BiConsumer<AcquireSpec, LockGrant> onGranted) {
        this.timer = timer;
        this.multiplexer = multiplexer;
        this.requestTimeoutMs = requestTimeoutMs;
        this.onGranted = onGranted;
        this.abandonedRetentionMs = requestTimeoutMs + RETENTION_BUFFER_MS;
    }

    /**
     * 发起获取并接管其生命周期：登记等待条目、按需挂总超时任务、发出首次请求。
     *
     * @param requestId      已分配的请求 id（重发复用）
     * @param envelope       获取请求信封（已含请求 id）
     * @param spec           获取参数
     * @param userFuture     用户 future
     * @param totalTimeoutMs 等待总超时（毫秒）；非正表示不限时（立即式）
     */
    public void startAcquire(long requestId, Envelope envelope, AcquireSpec spec,
            CompletableFuture<LockGrant> userFuture, long totalTimeoutMs) {
        WaitEntry entry = new WaitEntry(requestId, envelope, spec, userFuture,
                totalTimeoutMs > 0 ? System.currentTimeMillis() + totalTimeoutMs : Long.MAX_VALUE);
        waits.put(requestId, entry);
        if (totalTimeoutMs > 0) {
            entry.totalTimeoutTask = timer.newTimeout(t -> onTotalTimeout(requestId),
                    totalTimeoutMs, TimeUnit.MILLISECONDS);
        }
        sendOrResend(entry);
    }

    /**
     * 处理服务端队首通知：命中挂起项则以同一 {@code requestId} 重发；
     * 未命中（等待已超时/失败/完成）则忽略（详设 §6.5 边界场景）。
     *
     * @param notify 通知消息
     */
    public void onNotify(AwaitNotify notify) {
        WaitEntry entry = waits.get(notify.getRequestIdRef());
        if (entry == null) {
            return;
        }
        synchronized (entry) {
            if (entry.userFuture.isDone()) {
                return;
            }
            entry.everQueued = true;
        }
        sendOrResend(entry);
    }

    /**
     * 处理孤儿入站信封（多路复用器无匹配挂起项的响应）。
     * 仅处理获取响应：
     * <ul>
     *   <li>等待仍挂起（重发请求超时后响应才到）→ 按正常授予/失败处理；</li>
     *   <li>等待已结束且在保留窗口内 → 对授予发送补偿释放（D3）。</li>
     * </ul>
     *
     * @param envelope 孤儿信封
     */
    public void onOrphanResponse(Envelope envelope) {
        if (envelope.getType() != MessageType.LOCK_ACQUIRE || !envelope.hasAcquireResponse()) {
            return;
        }
        AcquireResponse response = envelope.getAcquireResponse();
        long requestId = envelope.getRequestId();
        WaitEntry entry = waits.get(requestId);
        if (entry != null) {
            handleResponse(entry, response);
            return;
        }
        if (response.getStatus() != StatusCode.OK) {
            return;
        }
        AbandonedEntry abandoned = recentlyAbandoned.get(requestId);
        if (abandoned != null) {
            compensateRelease(abandoned.key(), response.getLeaseToken(), abandoned.threadId());
        } else {
            log.debug("orphan grant without abandoned record, requestId={}", requestId);
        }
    }

    /**
     * 断连清空：全部挂起等待以给定原因快速失败（详设 §6.2）。
     *
     * @param cause 失败原因
     */
    public void failAll(Throwable cause) {
        for (Long requestId : waits.keySet()) {
            WaitEntry entry = waits.remove(requestId);
            if (entry == null) {
                continue;
            }
            synchronized (entry) {
                if (entry.totalTimeoutTask != null) {
                    entry.totalTimeoutTask.cancel();
                }
                entry.userFuture.completeExceptionally(cause);
            }
        }
    }

    /**
     * 当前挂起等待数，供测试与诊断。
     *
     * @return 挂起等待数
     */
    public int waitingCount() {
        return waits.size();
    }

    /**
     * 可迁移等待快照（S3 Leader 改道：本车道降级时挂起项向新主车道的
     * 重新排队移交上下文）。
     *
     * @param requestId   原请求 id（新车道重放将换新 id）
     * @param envelope    原信封（spec 字段重建来源）
     * @param spec        获取参数
     * @param userFuture  用户 future（迁移不改其终态责任，由重放链交付）
     * @param remainingMs 等待总预算剩余（毫秒；不限时为负值）
     */
    public record PendingWait(long requestId, Envelope envelope, AcquireSpec spec,
            CompletableFuture<LockGrant> userFuture, long remainingMs) {
    }

    /**
     * 摘取全部挂起等待并交还调用方重放（Leader 改道迁移专用）：条目从
     * 挂起表移除、总超时任务取消、future 保持未完成；同时登记保留映射，
     * 使旧队列迟到的授予经补偿归还路径消化，不产生孤儿锁。
     *
     * @return 迁移上下文列表（已完成 future 的条目仅摘除、不返回）
     */
    public List<PendingWait> drainPending() {
        List<PendingWait> out = new ArrayList<>();
        for (Long requestId : waits.keySet()) {
            WaitEntry entry = waits.remove(requestId);
            if (entry == null) {
                continue;
            }
            synchronized (entry) {
                if (entry.userFuture.isDone()) {
                    continue;
                }
                long remaining = entry.deadlineMs == Long.MAX_VALUE
                        ? -1L : entry.deadlineMs - System.currentTimeMillis();
                endWait(entry);
                out.add(new PendingWait(entry.requestId, entry.envelope, entry.spec,
                        entry.userFuture, remaining));
            }
        }
        return out;
    }

    /**
     * 发送（或重发）获取请求，响应回到 {@link #handleResponse}。
     *
     * @param entry 等待条目
     */
    private void sendOrResend(WaitEntry entry) {
        multiplexer.sendWithId(entry.envelope, requestTimeoutMs)
                .whenComplete((resp, err) -> {
                    if (err != null) {
                        onSendFailure(entry, err);
                        return;
                    }
                    // 类型校验必须先行：默认 AcquireResponse 实例的 status 恰为
                    // 枚举零值 OK、token 为 0，错型响应一旦漏检将被误判为
                    // "零凭据授予"。
                    if (resp.getType() != MessageType.LOCK_ACQUIRE || !resp.hasAcquireResponse()) {
                        onUnexpectedResponse(entry, resp);
                        return;
                    }
                    handleResponse(entry, resp.getAcquireResponse());
                });
    }

    /**
     * 错型响应处理（协议违例防御）：首次请求收到错型响应 → 等待失败；
     * 重发阶段收到错型响应 → 保持挂起等待下一次通知（与 D1 同调：
     * 等待项在服务端队列中的资格不因一次异常响应而丧失）。
     *
     * @param entry 等待条目
     * @param resp  错型信封
     */
    private void onUnexpectedResponse(WaitEntry entry, Envelope resp) {
        log.warn("unexpected response type {} for acquire requestId={}",
                resp.getType(), entry.requestId);
        synchronized (entry) {
            if (entry.userFuture.isDone()) {
                return;
            }
            if (entry.everQueued) {
                return;
            }
            endWait(entry);
            entry.userFuture.completeExceptionally(new OpenLatchException(
                    "unexpected response type " + resp.getType()
                            + " for acquire of '" + entry.spec.key() + "'"));
        }
    }

    /**
     * 请求收发失败处理：首次请求失败 → 等待失败；<b>重发</b>失败仅结束该次
     * 重发、保持挂起等待下一次通知（design.md D1），由等待总超时兜底。
     *
     * @param entry 等待条目
     * @param err   失败原因
     */
    private void onSendFailure(WaitEntry entry, Throwable err) {
        synchronized (entry) {
            if (entry.userFuture.isDone()) {
                return;
            }
            if (entry.everQueued) {
                // 重发失败：等待项仍在服务端队列（或已被队首超时清扫），保持挂起
                return;
            }
            endWait(entry);
            entry.userFuture.completeExceptionally(err);
        }
    }

    /**
     * 获取响应分发：授予 / 排队 / 拒绝 / 错误码四分支。
     *
     * @param entry    等待条目
     * @param response 获取响应
     */
    private void handleResponse(WaitEntry entry, AcquireResponse response) {
        switch (response.getStatus()) {
            case OK -> grant(entry, response);
            case QUEUED -> {
                synchronized (entry) {
                    if (!entry.userFuture.isDone()) {
                        entry.everQueued = true;
                    }
                }
            }
            case DENIED -> failWait(entry, new LockDeniedException(entry.spec.key()));
            case NOT_LEADER -> onNotLeader(entry, response);
            default -> failWait(entry, new OpenLatchException(response.getStatus(),
                    "acquire of '" + entry.spec.key() + "' failed: " + response.getStatus()));
        }
    }

    /**
     * {@code NOT_LEADER}（v2）分发：装配了接管钩子且返回 {@code true} 时，
     * 摘除登记但不终止用户 future（客户端改连新 Leader 或以同 requestId 原地
     * 重发后重新驱动）；否则按错误码使等待失败（v1 兼容/立即式路径）。
     *
     * @param entry    等待条目
     * @param response 携带 leader 提示的拒绝响应
     */
    private void onNotLeader(WaitEntry entry, AcquireResponse response) {
        NotLeaderHandler handler = notLeaderHandler;
        long remaining = entry.deadlineMs == Long.MAX_VALUE
                ? -1L : entry.deadlineMs - System.currentTimeMillis();
        boolean taken = handler.onNotLeader(new NotLeaderRequest(
                entry.requestId, entry.envelope, entry.spec, entry.userFuture,
                response.getLeaderNodeId(), response.getLeaderAddress(), remaining));
        if (taken) {
            synchronized (entry) {
                if (!entry.userFuture.isDone()) {
                    endWait(entry);
                }
            }
            return;
        }
        failWait(entry, new OpenLatchException(response.getStatus(),
                "acquire of '" + entry.spec.key() + "' failed: " + response.getStatus()));
    }

    /**
     * 授予处理：完成用户 future 并回调授予登记；若 future 已被终止竞争
     * 分出（总超时先行），则对到手的授予发送补偿释放。
     *
     * @param entry    等待条目
     * @param response 授予响应
     */
    private void grant(WaitEntry entry, AcquireResponse response) {
        LockGrant grant = new LockGrant(response.getLeaseToken(), response.getGrantedLeaseMs());
        synchronized (entry) {
            if (entry.userFuture.isDone()) {
                // 终止竞争：已放弃的等待收到了授予 → 归还
                compensateRelease(entry.spec.key(), grant.leaseToken(), entry.spec.threadId());
                return;
            }
            endWait(entry);
            // 顺序不变量：必须先登记持锁簿记、再完成 future。否则调用方在
            // future 完成后立即 unlock() 时，持锁条目尚未登记，会误抛
            // IllegalMonitorStateException（授予-解锁竞态）。
            try {
                onGranted.accept(entry.spec, grant);
            } finally {
                entry.userFuture.complete(grant);
            }
        }
    }

    /**
     * 以异常终止等待（拒绝/错误码路径）。
     *
     * @param entry 等待条目
     * @param cause 失败原因
     */
    private void failWait(WaitEntry entry, Throwable cause) {
        synchronized (entry) {
            if (entry.userFuture.isDone()) {
                return;
            }
            endWait(entry);
            entry.userFuture.completeExceptionally(cause);
        }
    }

    /**
     * 等待总超时回调：终止等待并以 {@link LockAcquisitionTimeoutException}
     * 失败用户 future。服务端队列条目按 §6.3 惰性回收。
     *
     * @param requestId 超时的请求 id
     */
    private void onTotalTimeout(long requestId) {
        WaitEntry entry = waits.get(requestId);
        if (entry == null) {
            return;
        }
        synchronized (entry) {
            if (entry.userFuture.isDone()) {
                return;
            }
            endWait(entry);
            entry.userFuture.completeExceptionally(new LockAcquisitionTimeoutException(
                    "acquire of '" + entry.spec.key() + "' timed out"));
        }
    }

    /**
     * 结束等待：摘除挂起项、取消总超时任务、登记保留映射并挂清理任务。
     * 仅在条目内置锁内调用。
     *
     * @param entry 等待条目
     */
    private void endWait(WaitEntry entry) {
        waits.remove(entry.requestId, entry);
        if (entry.totalTimeoutTask != null) {
            entry.totalTimeoutTask.cancel();
        }
        recentlyAbandoned.put(entry.requestId,
                new AbandonedEntry(entry.spec.key(), entry.spec.threadId()));
        timer.newTimeout(t -> recentlyAbandoned.remove(entry.requestId),
                abandonedRetentionMs, TimeUnit.MILLISECONDS);
    }

    /**
     * 补偿归还：对已放弃等待收到的授予发送释放。释放结果仅记日志——
     * 最坏情况下锁由服务端租约到期兜底释放。
     *
     * @param key      锁键
     * @param token    租约凭证
     * @param threadId 申请线程标识
     */
    private void compensateRelease(String key, long token, long threadId) {
        Envelope release = Envelope.newBuilder()
                .setType(MessageType.LOCK_RELEASE)
                .setReleaseRequest(ReleaseRequest.newBuilder()
                        .setKey(key)
                        .setLeaseToken(token)
                        .setThreadId(threadId))
                .build();
        multiplexer.send(release.toBuilder(), requestTimeoutMs).whenComplete((resp, err) -> {
            if (err != null) {
                log.debug("compensation release of '{}' failed: {}", key, err.toString());
            }
        });
    }
}
