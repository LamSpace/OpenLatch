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

package io.github.lamspace.openlatch.client;

import io.github.lamspace.openlatch.protocol.AtomicOp;
import io.github.lamspace.openlatch.protocol.AtomicOpRequest;
import io.github.lamspace.openlatch.protocol.AtomicOpResponse;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.StatusCode;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 原子变量句柄的共享执行核（三形态实现的公共引擎）：请求信封构造、
 * {@code op_seq} 分配、在途写互斥与可重试失败的同序号自动重发循环。
 *
 * <p><b>重发裁决（效果确定性的来源）</b>：写操作在总界限
 * （{@code requestTimeout}，含重发）内对下列失败以同一 {@code op_seq}
 * 重发——{@code NOT_LEADER}/{@code OVERLOADED} 应答码、传输失败
 * （{@link ServerUnavailableException}）、单次请求超时；服务端每 key 去重槽
 * 保证同 {@code (会话, op_seq)} 的重发不重复生效。可重试失败途中会话发生
 * 切换（重连换了逻辑 sid）时写路径放弃重发并抛
 * {@link OpenLatchException}——新会话下旧序号不再命中去重槽，盲目重发会
 * 二次生效；读路径（GET 幂等）不受此限。总界限耗尽抛
 * {@link OpenLatchTimeoutException}，其效果不确定语义见各公开接口注释。
 *
 * <p><b>在途写互斥</b>：同 key 写操作经客户端级监视器串行——保证任意迟到
 * 重发的 {@code op_seq} 始终是该 key 的最近序号，去重单槽即充分。
 * 跨客户端的并发写由服务端定序承载，互斥不覆盖该域（也无需覆盖）。
 *
 * <p><b>线程模型</b>：句柄无内部可变状态（除经基原子的序号分配在客户端侧）；
 * 阻塞等待发生在调用线程，中断即恢复标志并抛出，不重试。
 */
abstract class RemoteAtomicBase {

    /** 应答读界在请求超时之外的余量（毫秒），与 {@code RemoteSemaphore} 口径一致。 */
    private static final long SLACK_MS = 1000L;
    /** 断线重连窗口的会话轮询间隔（毫秒）。 */
    private static final long SESSION_POLL_MS = 50L;

    /** 所属客户端。 */
    final OpenLatchClient client;
    /** 原子变量 key。 */
    final String key;
    /** 线路形态判别（三原子 LockType 之一）。 */
    final io.github.lamspace.openlatch.protocol.LockType wireKind;
    /** 初值主张（0=不主张；随每条请求携带，条目存在时服务端作一致性断言）。 */
    final long initialClaim;

    /**
     * 构造共享执行核。
     *
     * @param client       所属客户端
     * @param key          原子变量 key
     * @param wireKind     线路形态判别
     * @param initialClaim 初值主张（{@code >= 0}，0 不主张）
     */
    RemoteAtomicBase(OpenLatchClient client, String key,
                     io.github.lamspace.openlatch.protocol.LockType wireKind, long initialClaim) {
        this.client = client;
        this.key = key;
        this.wireKind = wireKind;
        this.initialClaim = initialClaim;
    }

    /**
     * 本句柄的 key。
     *
     * @return key
     */
    public String key() {
        return key;
    }

    /**
     * 服务端应答四元组（不可变读数）。
     *
     * @param applied 是否落值（CAS 家族成败；GET 恒 false）
     * @param oldValue 操作前值
     * @param value    操作后（或当前）值
     * @param version  版本戳
     */
    record Quad(boolean applied, long oldValue, long value, long version) { }

    /**
     * 执行一次读操作（GET）：无锁、无序号、幂等重试。
     *
     * @return 应答四元组（applied=false，old==value==当前值）
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        不可重试失败
     * @throws OpenLatchTimeoutException 总界限耗尽
     */
    final Quad read() throws InterruptedException {
        return execute(AtomicOp.ATOMIC_GET, 0, 0, 0, false);
    }

    /**
     * 执行一次写操作（在途互斥 + 同序号重发）。
     *
     * @param op             操作类型
     * @param operand        操作数（新值/增量/CAS 更新值）
     * @param expected       CAS 期望值
     * @param expectedVersion 版本断言（0 不主张）
     * @return 应答四元组
     * @throws InterruptedException      本地等待被中断
     * @throws OpenLatchException        不可重试失败（含会话切换放弃）
     * @throws OpenLatchTimeoutException 总界限耗尽（效果不确定）
     */
    final Quad write(AtomicOp op, long operand, long expected, long expectedVersion)
            throws InterruptedException {
        Object monitor = client.atomicWriteMonitor(key);
        synchronized (monitor) {
            return execute(op, operand, expected, expectedVersion, true);
        }
    }

    /**
     * 请求执行循环：构造一次信封（固定 request_id 由 multiplexer 分配、
     * 重发复用同一信封与 {@code op_seq}），在总界限内按重发裁决推进。
     *
     * @param op      操作
     * @param operand 操作数
     * @param expected 期望值
     * @param expectedVersion 版本断言
     * @param writeOp 是否写路径（决定序号与放弃语义）
     * @return 应答四元组
     * @throws InterruptedException 中断
     */
    private Quad execute(AtomicOp op, long operand, long expected, long expectedVersion,
            boolean writeOp) throws InterruptedException {
        long opSeq = writeOp ? client.nextAtomicOpSeq() : 0L;
        long budgetMs = client.config().requestTimeout().toMillis();
        long deadline = System.currentTimeMillis() + budgetMs;
        Long startSession = null;
        Envelope env = null;
        long envSid = -1L;
        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new OpenLatchTimeoutException("atomic " + (writeOp ? "write" : "read")
                        + " on '" + key + "' exceeded request timeout; the write may have been"
                        + " applied — verify with getStamped()");
            }
            OpenLatchClient.LatchRoute route = client.latchRoute();
            if (route == null) {
                Thread.sleep(Math.min(remaining, SESSION_POLL_MS));
                continue;
            }
            if (startSession == null) {
                startSession = route.session().sessionId();
            }
            if (env == null || envSid != route.session().sessionId()) {
                long rid = route.session().nextRequestId();
                env = Envelope.newBuilder()
                        .setType(MessageType.ATOMIC_OP)
                        .setRequestId(rid)
                        .setAtomicOpRequest(AtomicOpRequest.newBuilder()
                                .setKey(key).setOp(op).setLockType(wireKind)
                                .setOperand(operand).setExpected(expected)
                                .setExpectedVersion(expectedVersion)
                                .setInitialValue(initialClaim).setOpSeq(opSeq))
                        .build();
                envSid = route.session().sessionId();
            }
            if (writeOp && route.session().sessionId() != startSession) {
                throw new OpenLatchException(StatusCode.SESSION_EXPIRED,
                        "session switched mid-flight for atomic write on '" + key
                                + "' — dedup slot no longer applies; verify effect with getStamped()");
            }
            AtomicOpResponse resp;
            try {
                Envelope answer = route.mux().sendWithId(env,
                        Math.min(remaining, budgetMs + SLACK_MS))
                        .get(Math.min(remaining, budgetMs + SLACK_MS), TimeUnit.MILLISECONDS);
                resp = answer.getAtomicOpResponse();
            } catch (ExecutionException e) {
                Throwable cause = unwrap(e);
                if (isTransientRetryable(cause)) {
                    continue; // 同信封（同 op_seq）重发
                }
                if (cause instanceof InterruptedException ie) {
                    throw ie;
                }
                if (cause instanceof RuntimeException re) {
                    throw re;
                }
                throw new OpenLatchException("atomic op on '" + key + "' failed", cause);
            } catch (TimeoutException e) {
                continue; // 读界兜底：同序号重发，服务端去重槽裁决
            }
            StatusCode st = resp.getStatus();
            if (st == StatusCode.OK) {
                return new Quad(resp.getApplied(), resp.getOldValue(), resp.getValue(),
                        resp.getVersion());
            }
            if (st == StatusCode.NOT_LEADER || st == StatusCode.OVERLOADED) {
                continue;
            }
            throw new OpenLatchException(st, "atomic op on '" + key + "' rejected: " + st
                    + (writeOp ? "; effect indeterminate — verify with getStamped()" : ""));
        }
    }

    /**
     * 瞬态可重发判定：传输不可用（含 Leader 切换窗口的改道失败）与
     * 单次请求超时——两者均不改变服务端裁决结果，同序号重发安全。
     *
     * @param cause 解包原因
     * @return 可重发为 {@code true}
     */
    private static boolean isTransientRetryable(Throwable cause) {
        return cause instanceof ServerUnavailableException
                || cause instanceof OpenLatchTimeoutException
                || (cause instanceof OpenLatchException ole
                        && ole.status() == StatusCode.NOT_LEADER);
    }

    /**
     * 解包 {@link ExecutionException}。
     *
     * @param t 待解包异常
     * @return 真实原因
     */
    private static Throwable unwrap(Throwable t) {
        return t instanceof ExecutionException && t.getCause() != null ? t.getCause() : t;
    }
}
