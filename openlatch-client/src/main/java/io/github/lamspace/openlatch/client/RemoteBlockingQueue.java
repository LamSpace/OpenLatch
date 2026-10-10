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

import io.github.lamspace.openlatch.client.internal.LatchNotifyRegistry;
import io.github.lamspace.openlatch.protocol.Envelope;
import io.github.lamspace.openlatch.protocol.MessageType;
import io.github.lamspace.openlatch.protocol.QueueOp;
import io.github.lamspace.openlatch.protocol.QueueOpRequest;
import io.github.lamspace.openlatch.protocol.QueueOpResponse;
import io.github.lamspace.openlatch.protocol.StatusCode;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link OBlockingQueue}/{@link ODelayQueue} 的服务端实现（包私有，经
 * {@code OpenLatchClient.newBlockingQueue/newDelayQueue} 获得）。
 *
 * <p><b>车道</b>：写侧沿用 {@link RemoteAtomicBase} 的同 key 在途写互斥 +
 * 同 {@code op_seq} 重发纪律（共用客户端监视器与序号分配——服务端每会话
 * 单槽的充分性前提），应答丢失/瞬态失败以同一信封重发，由服务端去重槽
 * 裁决（PUT 不双插、TAKE/DRAIN 重放交付同一份字节）；会话中途切换——
 * 上一尝试被<b>明确拒入</b>（非权威拒绝/缺码形/等待深度满，条目未入任何
 * 日志、零生效）时在新会话重建信封续发（HELLO 提示建道后的换窗即此形态），
 * 其余（应答不确定窗）仍放弃（槽随旧会话失效，判例原子写）。阻塞挂起（put/take/带超时 offer/poll）
 * 走"等待-通知-重发"闭环（判例 {@link RemoteCountDownLatch#await()}）：
 * {@code QUEUED} 后保持 {@link LatchNotifyRegistry} 登记，推送到达以同
 * {@code requestId} 同信封重发；服务端应用点竞态回弹（改写 QUEUED）对本类
 * 透明——表现为继续等待。
 *
 * <p><b>状态码分派</b>：{@code NOT_LEADER}/{@code OVERLOADED}（转发竞态窗口）
 * 与传输瞬态按同信封重试；{@code DENIED} 在立即式是正常终态（队满/空队），
 * 在阻塞式是回弹缺口的防御兜底（继续重发）；{@code INVALID_REQUEST}/
 * {@code SESSION_EXPIRED} 显式抛错不重试（超限/形状/低版本/会话换代）。
 * 应答形态裁决：缺 {@code queue_op_response} 载荷的异型/空码形按瞬态重发
 * （MUST NOT 读 protobuf 默认实例的 {@code status}=零值 {@code OK} 伪成功，
 * 否则 {@code take} 交付空串、{@code size} 读 0——换主窗拒绝的成型路径）；
 * TAKE 的 {@code OK}-缺 {@code element_bytes} 属交付契约违例，显式抛错，
 * 而 PEEK/SIZE 的 {@code OK}-缺省是合法空读数（护栏仅约束 TAKE）。
 *
 * <p><b>无租约</b>：队列操作不登记持锁簿记、不启动看门狗——与
 * {@link OAtomicReference} 同一"值/元素不绑定归属"的纪律。
 */
final class RemoteBlockingQueue implements ODelayQueue {

    /** 应答读界在请求超时之外的余量（毫秒），与原子/Latch 车道口径一致。 */
    private static final long SLACK_MS = 1000L;
    /** 断线重连窗口的会话轮询间隔（毫秒）。 */
    private static final long SESSION_POLL_MS = 50L;

    /** 所属客户端。 */
    private final OpenLatchClient client;
    /** 队列 key。 */
    private final String key;
    /** 定型容量主张（随每条请求携带作一致性断言）。 */
    private final long capacity;
    /** 线路形态判别（QUEUE/DELAY_QUEUE）。 */
    private final io.github.lamspace.openlatch.protocol.LockType wireKind;

    /**
     * 构造队列句柄。
     *
     * @param client   所属客户端
     * @param key      队列键（非空）
     * @param capacity 定型容量主张（{@code >= 1}，调用方已校验）
     * @param wireKind 线路形态（QUEUE/DELAY_QUEUE）
     */
    RemoteBlockingQueue(OpenLatchClient client, String key, long capacity,
            io.github.lamspace.openlatch.protocol.LockType wireKind) {
        this.client = client;
        this.key = key;
        this.capacity = capacity;
        this.wireKind = wireKind;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public long capacity() {
        return capacity;
    }

    // ===== 阻塞投递/消费 =====

    @Override
    public void put(byte[] element) throws InterruptedException {
        requireElement(element);
        if (!parkExchange(QueueOp.QUEUE_OP_PUT, element,
                client.config().defaultWaitTimeout().toMillis())) {
            throw new OpenLatchTimeoutException("put on queue '" + key
                    + "' exceeded default wait budget; the element may have been enqueued"
                    + " — verify with size(), the slot dedup keeps resend safe");
        }
    }

    @Override
    public void put(String element) throws InterruptedException {
        put(toUtf8(element));
    }

    @Override
    public boolean offer(byte[] element) {
        requireElement(element);
        return immediate(QueueOp.QUEUE_OP_PUT, false, element, 0, 0).getStatus()
                == StatusCode.OK;
    }

    @Override
    public boolean offer(String element) {
        return offer(toUtf8(element));
    }

    @Override
    public boolean offer(byte[] element, long timeout, TimeUnit unit)
            throws InterruptedException {
        requireElement(element);
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must be >= 0: " + timeout);
        }
        if (timeout == 0) {
            return offer(element);
        }
        return parkExchange(QueueOp.QUEUE_OP_PUT, element, unit.toMillis(timeout));
    }

    @Override
    public boolean offer(String element, long timeout, TimeUnit unit)
            throws InterruptedException {
        return offer(toUtf8(element), timeout, unit);
    }

    @Override
    public byte[] take() throws InterruptedException {
        byte[] element = parkExchangeTake(client.config().defaultWaitTimeout().toMillis());
        if (element == null) {
            throw new OpenLatchTimeoutException("take on queue '" + key
                    + "' exceeded default wait budget; no element was delivered");
        }
        return element;
    }

    @Override
    public String takeAsString() throws InterruptedException {
        return fromUtf8(take());
    }

    @Override
    public byte[] poll() {
        QueueOpResponse resp = immediate(QueueOp.QUEUE_OP_TAKE, false, null, 0, 0);
        return resp.hasElementBytes() ? resp.getElementBytes().toByteArray() : null;
    }

    @Override
    public String pollAsString() {
        byte[] element = poll();
        return element == null ? null : fromUtf8(element);
    }

    @Override
    public byte[] poll(long timeout, TimeUnit unit) throws InterruptedException {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must be >= 0: " + timeout);
        }
        if (timeout == 0) {
            return poll();
        }
        return parkExchangeTake(unit.toMillis(timeout));
    }

    // ===== 批量与读数 =====

    @Override
    public int drainTo(Collection<? super byte[]> sink) {
        return drainTo(sink, 0);
    }

    @Override
    public int drainTo(Collection<? super byte[]> sink, int maxElements) {
        if (sink == null) {
            throw new IllegalArgumentException("sink must not be null");
        }
        if (maxElements < 0) {
            throw new IllegalArgumentException("maxElements must be >= 0");
        }
        QueueOpResponse resp = immediate(QueueOp.QUEUE_OP_DRAIN, false, null, 0, maxElements);
        List<com.google.protobuf.ByteString> drained = resp.getDrainedBytesList();
        for (com.google.protobuf.ByteString element : drained) {
            sink.add(element.toByteArray());
        }
        return drained.size();
    }

    @Override
    public byte[] peek() {
        QueueOpResponse resp = read(QueueOp.QUEUE_OP_PEEK);
        return resp.hasElementBytes() ? resp.getElementBytes().toByteArray() : null;
    }

    @Override
    public String peekAsString() {
        byte[] element = peek();
        return element == null ? null : fromUtf8(element);
    }

    @Override
    public int size() {
        return (int) read(QueueOp.QUEUE_OP_SIZE).getSize();
    }

    @Override
    public int remainingCapacity() {
        long left = capacity - size();
        return left < 0 ? 0 : (int) Math.min(left, Integer.MAX_VALUE);
    }

    // ===== 延时注入 =====

    @Override
    public boolean offerDelayed(byte[] element, long delay, TimeUnit unit) {
        requireElement(element);
        if (delay < 0) {
            throw new IllegalArgumentException("delay must be >= 0");
        }
        if (wireKind != io.github.lamspace.openlatch.protocol.LockType.LOCK_TYPE_DELAY_QUEUE) {
            throw new IllegalStateException(
                    "offerDelayed requires a delay-queue handle for key '" + key + "'");
        }
        QueueOpResponse resp = exchange(QueueOp.QUEUE_OP_PUT, false, element,
                unit.toNanos(delay) / 1_000_000L, 0,
                client.config().requestTimeout().toMillis());
        return resp.getStatus() == StatusCode.OK;
    }

    @Override
    public boolean offerDelayed(String element, long delay, TimeUnit unit) {
        return offerDelayed(toUtf8(element), delay, unit);
    }

    // ===== 车道内部 =====

    /**
     * 阻塞 take 公共路径：交付元素字节（预算耗尽回 {@code null}）。
     *
     * @param budgetMs 等待预算（毫秒）
     * @return 交付元素；预算耗尽为 {@code null}
     * @throws InterruptedException 等待被中断
     */
    private byte[] parkExchangeTake(long budgetMs) throws InterruptedException {
        ParkOutcome outcome = parkLoop(QueueOp.QUEUE_OP_TAKE, null, budgetMs);
        return outcome.delivered();
    }

    /**
     * 阻塞 put 公共路径。
     *
     * @param op       操作（PUT——挂起环仅 PUT/TAKE 两类）
     * @param element  元素字节
     * @param budgetMs 等待预算（毫秒）
     * @return 入队成功 {@code true}；预算耗尽 {@code false}
     * @throws InterruptedException 等待被中断
     */
    private boolean parkExchange(QueueOp op, byte[] element, long budgetMs)
            throws InterruptedException {
        return parkLoop(op, element, budgetMs).granted();
    }

    /**
     * 挂起环终态。
     *
     * @param granted   写类入队/授予成功为 {@code true}（TAKE 成功交付同真）
     * @param delivered TAKE 交付字节（PUT 或预算耗尽为 {@code null}）
     */
    private record ParkOutcome(boolean granted, byte[] delivered) {
    }

    /**
     * 等待-通知-重发闭环：QUEUED 保持登记等推送，推送/兜底读界到点后以同
     * 信封（同 requestId、同 op_seq）重发；DENIED 视作回弹缺口继续重发；
     * OVERLOADED（等待深度满）显式抛错。应答形态裁决：缺
     * {@code queue_op_response} 载荷的异型/空码形按瞬态退避改道重发
     * （MUST NOT 读 protobuf 默认实例伪成功）；TAKE 的 OK-缺
     * {@code element_bytes} 属交付契约违例，显式 {@code OpenLatchException}
     * 抛出，MUST NOT 交付零长度数组伪装元素。
     *
     * @param op       挂起型操作（PUT/TAKE）
     * @param element  PUT 元素（TAKE 为 null）
     * @param budgetMs 总预算（毫秒）
     * @return 终态
     * @throws InterruptedException 等待被中断
     */
    private ParkOutcome parkLoop(QueueOp op, byte[] element,
            long budgetMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + budgetMs;
        long opSeq = nextSeq();
        LatchNotifyRegistry registry = client.latchNotifies();
        OpenLatchClient.LatchRoute route = client.latchRoute();
        Envelope env = null;
        long envSession = -1;
        long rid = -1;
        Long startSession = null;
        // 上一尝试被明确拒入（非权威拒绝/缺码形——条目未入任何日志、零生效）
        // 的标志：此后的会话切换可在新会话重建信封续发（HELLO 提示建道后的
        // 换窗即此形态）；不确定窗（超时/传输失败）后仍严格放弃。
        boolean noEffectReject = false;
        try {
            while (true) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return new ParkOutcome(false, null);
                }
                if (route == null) {
                    Thread.sleep(Math.min(remaining, SESSION_POLL_MS));
                    route = client.latchRoute();
                    continue;
                }
                if (env == null || envSession != route.session().sessionId()) {
                    envSession = route.session().sessionId();
                    rid = route.session().nextRequestId();
                    env = queueEnvelope(rid, op, true, element, 0, 0, opSeq);
                    if (startSession == null) {
                        startSession = envSession;
                    } else if (startSession != envSession) {
                        if (noEffectReject) {
                            startSession = envSession;
                            noEffectReject = false;
                        } else {
                            throw new OpenLatchException(StatusCode.SESSION_EXPIRED,
                                    "session switched mid-flight for queue op on '" + key
                                            + "' — dedup slot no longer applies; verify with size()");
                        }
                    }
                }
                CompletableFuture<Void> arrived = registry.register(envSession, rid);
                Envelope answer;
                try {
                    answer = route.mux().sendWithId(env,
                                    Math.min(remaining, client.config().requestTimeout().toMillis()))
                            .get(Math.min(remaining,
                                    client.config().requestTimeout().toMillis() + SLACK_MS),
                                    TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    Throwable cause = unwrap(e);
                    if (isTransient(cause)) {
                        // 不确定窗（可能已受理）：恢复严格换代守卫。
                        noEffectReject = false;
                        registry.remove(envSession, rid);
                        if (cause instanceof OpenLatchException ole
                                && ole.status() == StatusCode.SESSION_EXPIRED) {
                            env = null;
                            startSession = null;
                        }
                        route = client.latchRoute();
                        continue;
                    }
                    if (cause instanceof RuntimeException re) {
                        throw re;
                    }
                    throw new OpenLatchException("queue op on '" + key + "' failed", cause);
                } catch (TimeoutException e) {
                    // 应答丢失属不确定窗：恢复严格换代守卫。
                    noEffectReject = false;
                    registry.remove(envSession, rid);
                    continue; // 应答读界兜底：同信封同序号重发，服务端裁决
                }
                if (!answer.hasQueueOpResponse()) {
                    // 缺码形应答（type 回显 QUEUE_OP 而载荷异型/空缺）：拒绝未
                    // 受理或组装违例——MUST NOT 读 protobuf 默认实例（status 零值
                    // OK）伪成功，按瞬态车道退避改道同信封重发。明确零生效：
                    // 允许随后的会话切换重建续发。
                    noEffectReject = true;
                    registry.remove(envSession, rid);
                    route = client.latchRoute();
                    continue;
                }
                QueueOpResponse resp = answer.getQueueOpResponse();
                StatusCode status = resp.getStatus();
                if (status == StatusCode.OK) {
                    if (op == QueueOp.QUEUE_OP_TAKE && !resp.hasElementBytes()) {
                        // OK-TAKE 必携交付是状态机契约（显式 presence 使零长度
                        // 空串元素与缺省可辨）——缺元素即交付契约被破坏，显式
                        // 抛出留现场，MUST NOT 交付 byte[0] 伪装元素。
                        throw new OpenLatchException(StatusCode.INTERNAL_ERROR,
                                "protocol violation: OK TAKE without element payload on queue '"
                                        + key + "' — delivery contract broken server-side; "
                                        + "verify with size()");
                    }
                    return op == QueueOp.QUEUE_OP_TAKE
                            ? new ParkOutcome(true, resp.getElementBytes().toByteArray())
                            : new ParkOutcome(true, null);
                }
                if (status == StatusCode.QUEUED || status == StatusCode.DENIED) {
                    // QUEUED：正常挂起等推送；DENIED：回弹改写缺位的防御兜底——
                    // 同信封继续重发等价于再排队（去重槽/幂等位保证不重复生效）。
                    // 二者均已受理/入队，非零生效窗：恢复严格换代守卫。
                    noEffectReject = false;
                    long left = deadline - System.currentTimeMillis();
                    if (left <= 0) {
                        return new ParkOutcome(false, null);
                    }
                    try {
                        arrived.get(Math.min(left,
                                client.config().requestTimeout().toMillis()),
                                TimeUnit.MILLISECONDS);
                    } catch (TimeoutException pushLost) {
                        // 推送丢失兜底：自发幂等重发一轮。
                    } catch (ExecutionException ee) {
                        // arrived 仅被 complete(null)，理论不可达，防御续环。
                    }
                    continue;
                }
                if (status == StatusCode.NOT_LEADER) {
                    // 非权威拒绝=明确零生效：允许随后的会话切换重建续发。
                    // 必须重取路由：驻留期核对或提示建道可能已就位新的当值
                    // Leader 车道，不重取则本环钉死在旧路由上耗尽整个预算
                    // （判例同本环的瞬态/缺码形分支，二者均已重取）。
                    noEffectReject = true;
                    route = client.latchRoute();
                    continue;
                }
                throw new OpenLatchException(status, "queue op on '" + key
                        + "' rejected: " + status + " (OVERLOADED = waiter queue full)");
            }
        } finally {
            if (rid >= 0) {
                registry.remove(envSession, rid);
            }
        }
    }

    /**
     * 立即式一次交换（offer/poll/drain 及其超时为 0 的退化）：OK/DENIED
     * 为正常终态，其余非瞬态拒绝抛错。
     *
     * @param op          操作
     * @param blocking    阻塞位（立即式恒 false）
     * @param element     元素（可 null）
     * @param delayMs     延迟
     * @param maxElements 批量上限
     * @return 应答
     */
    private QueueOpResponse immediate(QueueOp op, boolean blocking, byte[] element,
            long delayMs, int maxElements) {
        return exchange(op, blocking, element, delayMs, maxElements,
                client.config().requestTimeout().toMillis());
    }

    /**
     * 零迁移读交换（PEEK/SIZE）。
     *
     * @param op 读操作
     * @return 应答
     */
    private QueueOpResponse read(QueueOp op) {
        return exchange(op, false, null, 0, 0, client.config().requestTimeout().toMillis());
    }

    /**
     * 同步交换环：同信封同 op_seq 直至终态或预算耗尽（瞬态/NOT_LEADER/
     * 缺码形应答重试，DENIED 由立即式调用方按语义消化）。缺码形应答
     * （type 回显 QUEUE_OP 而载荷异型/空缺）MUST NOT 读 protobuf 默认实例
     * 伪成功，按瞬态退避改道重发；{@code TAKE} 的 {@code OK}-缺
     * {@code element_bytes} 属交付契约违例，显式抛出不以 null 混淆空队。
     *
     * @param op          操作
     * @param blocking    阻塞位
     * @param element     元素（可 null）
     * @param delayMs     延迟
     * @param maxElements 批量上限
     * @param budgetMs    总预算（毫秒）
     * @return 终态应答（OK/DENIED）
     */
    private QueueOpResponse exchange(QueueOp op, boolean blocking, byte[] element,
            long delayMs, int maxElements, long budgetMs) {
        long deadline = System.currentTimeMillis() + budgetMs;
        boolean writeOp = op == QueueOp.QUEUE_OP_PUT || op == QueueOp.QUEUE_OP_TAKE
                || op == QueueOp.QUEUE_OP_DRAIN;
        Object monitor = writeOp ? client.atomicWriteMonitor(key) : null;
        long opSeq = writeOp ? nextSeq() : 0L;
        Envelope env = null;
        long envSession = -1;
        Long startSession = null;
        // 上一尝试被明确拒入（非权威拒绝/缺码形/等待深度满——条目未入任何
        // 日志、零生效）的标志：此后的会话切换可在新会话重建信封续发（HELLO
        // 提示建道后的换窗即此形态）；不确定窗（超时/传输失败）后仍严格放弃。
        boolean noEffectReject = false;
        while (true) {
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                throw new OpenLatchTimeoutException("queue " + op + " on '" + key
                        + "' exceeded request timeout; the op may have been applied"
                        + " — verify with size()");
            }
            OpenLatchClient.LatchRoute route = client.latchRoute();
            if (route == null) {
                try {
                    sleepPoll(remaining);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new OpenLatchException("queue op on '" + key
                            + "' interrupted while awaiting session", ie);
                }
                continue;
            }
            synchronized (monitor == null ? this : monitor) {
                if (env == null || envSession != route.session().sessionId()) {
                    envSession = route.session().sessionId();
                    env = queueEnvelope(route.session().nextRequestId(), op, blocking,
                            element, delayMs, maxElements, opSeq);
                    if (startSession == null) {
                        startSession = envSession;
                    } else if (writeOp && startSession != envSession) {
                        if (noEffectReject) {
                            startSession = envSession;
                            noEffectReject = false;
                        } else {
                            throw new OpenLatchException(StatusCode.SESSION_EXPIRED,
                                    "session switched mid-flight for queue write on '" + key + "'");
                        }
                    }
                }
                Envelope answer;
                try {
                    answer = route.mux().sendWithId(env,
                                    Math.min(remaining,
                                            client.config().requestTimeout().toMillis()))
                            .get(Math.min(remaining,
                                    client.config().requestTimeout().toMillis() + SLACK_MS),
                                    TimeUnit.MILLISECONDS);
                } catch (ExecutionException e) {
                    Throwable cause = unwrap(e);
                    if (isTransient(cause)) {
                        // 不确定窗（可能已受理）：恢复严格换代守卫。
                        noEffectReject = false;
                        if (cause instanceof OpenLatchException ole
                                && ole.status() == StatusCode.SESSION_EXPIRED) {
                            env = null;
                            startSession = null;
                        }
                        continue;
                    }
                    if (cause instanceof RuntimeException re) {
                        throw re;
                    }
                    throw new OpenLatchException("queue op on '" + key + "' failed", cause);
                } catch (TimeoutException e) {
                    // 应答丢失属不确定窗：恢复严格换代守卫。
                    noEffectReject = false;
                    continue; // 同序号重发，服务端去重槽裁决
                } catch (InterruptedException ie) {
                    // 立即式不声明受检中断：还原中断位并包装显式抛出。
                    Thread.currentThread().interrupt();
                    throw new OpenLatchException("queue op on '" + key
                            + "' interrupted while awaiting response", ie);
                }
                if (!answer.hasQueueOpResponse()) {
                    // 缺码形应答（type 回显 QUEUE_OP 而载荷异型/空缺）：拒绝未
                    // 受理或组装违例——MUST NOT 读 protobuf 默认实例（status 零值
                    // OK）伪成功，按瞬态车道退避改道同信封重发。明确零生效：
                    // 允许随后的会话切换重建续发。
                    noEffectReject = true;
                    continue;
                }
                QueueOpResponse resp = answer.getQueueOpResponse();
                StatusCode status = resp.getStatus();
                if (status == StatusCode.OK || status == StatusCode.DENIED) {
                    if (status == StatusCode.OK && op == QueueOp.QUEUE_OP_TAKE
                            && !resp.hasElementBytes()) {
                        // OK-TAKE 缺元素=交付契约违例（与 parkLoop 同口径），
                        // 显式抛出，MUST NOT 以 null 混淆"空队"合法形态。
                        throw new OpenLatchException(StatusCode.INTERNAL_ERROR,
                                "protocol violation: OK TAKE without element payload on queue '"
                                        + key + "' — delivery contract broken server-side; "
                                        + "verify with size()");
                    }
                    return resp;
                }
                if (status == StatusCode.NOT_LEADER || status == StatusCode.OVERLOADED) {
                    // 二者均在提交/入队前拒入（非权威 / 等待深度满）：零生效，
                    // 允许随后的会话切换重建续发。
                    noEffectReject = true;
                    continue;
                }
                throw new OpenLatchException(status, "queue op on '" + key
                        + "' rejected: " + status);
            }
        }
    }

    /**
     * 构造队列请求信封。
     *
     * @param rid         请求 id
     * @param op          操作
     * @param blocking    阻塞位
     * @param element     元素（可 null）
     * @param delayMs     延迟
     * @param maxElements 批量上限
     * @param opSeq       写序号
     * @return 信封
     */
    private Envelope queueEnvelope(long rid, QueueOp op, boolean blocking, byte[] element,
            long delayMs, int maxElements, long opSeq) {
        QueueOpRequest.Builder rb = QueueOpRequest.newBuilder()
                .setKey(key).setOp(op).setLockType(wireKind)
                .setBlocking(blocking).setCapacity(capacity)
                .setDelayMs(delayMs).setMaxElements(maxElements).setOpSeq(opSeq);
        if (element != null) {
            rb.setElementBytes(com.google.protobuf.ByteString.copyFrom(element));
        }
        return Envelope.newBuilder().setType(MessageType.QUEUE_OP)
                .setRequestId(rid).setQueueOpRequest(rb).build();
    }

    /**
     * 写序号分配（与原子族共用客户端单调源——同 key 在途互斥由调用侧
     * 监视器保证）。
     *
     * @return 新写序号
     */
    private long nextSeq() {
        return client.nextAtomicOpSeq();
    }

    /**
     * 会话未就绪窗口的短睡。
     *
     * @param remaining 剩余预算（毫秒）
     * @throws InterruptedException 睡眠被中断
     */
    private static void sleepPoll(long remaining) throws InterruptedException {
        Thread.sleep(Math.min(remaining, SESSION_POLL_MS));
    }

    /**
     * 元素非空契约校验（与 {@link OAtomicReference} 的 null 语义刻意不同）。
     *
     * @param element 待检元素
     */
    private static void requireElement(byte[] element) {
        if (element == null) {
            throw new IllegalArgumentException("queue element must not be null");
        }
    }

    /**
     * String→UTF-8 字节（null 拒）。
     *
     * @param element 元素字符串
     * @return UTF-8 字节
     */
    private static byte[] toUtf8(String element) {
        if (element == null) {
            throw new IllegalArgumentException("queue element must not be null");
        }
        return element.getBytes(StandardCharsets.UTF_8);
    }

    /**
     * UTF-8 字节→String。
     *
     * @param element 元素字节
     * @return 字符串
     */
    private static String fromUtf8(byte[] element) {
        return new String(element, StandardCharsets.UTF_8);
    }

    /**
     * 瞬态失败判定（传输失败/请求超时/NOT_LEADER/会话未就绪），判例
     * {@link RemoteCountDownLatch} 与原子车道。
     *
     * @param cause 解包后的原因
     * @return 可同信封重试为 {@code true}
     */
    private static boolean isTransient(Throwable cause) {
        return cause instanceof ServerUnavailableException
                || cause instanceof OpenLatchTimeoutException
                || cause instanceof OpenLatchException ole
                && (ole.status() == StatusCode.NOT_LEADER
                        || ole.status() == StatusCode.OVERLOADED);
    }

    /**
     * 解包 ExecutionException。
     *
     * @param e 执行异常
     * @return 根因
     */
    private static Throwable unwrap(Throwable e) {
        Throwable t = e;
        while (t instanceof ExecutionException && t.getCause() != null) {
            t = t.getCause();
        }
        return t;
    }
}
