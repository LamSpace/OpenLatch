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
import io.github.lamspace.openlatch.protocol.StatusCode;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * {@link OCountDownLatch} 的远程实现。
 *
 * <p><b>等待编排</b>：{@code LATCH_AWAIT} 应答 OK 即归零放行；QUEUED 后
 * 挂起等待 {@code AWAIT_NOTIFY}——通知到达以同一 {@code requestId} 重发
 * （服务端按 {@code (会话, 请求)} 幂等去重）。断线时进行中的请求以传输
 * 失败终结，循环在剩余预算内等新会话就绪后以新 {@code requestId} 重放
 * （改连换 id 的幂等口径与 {@code OLock} 等待一致）。通知
 * 等待设请求超时上限作推送丢失的兜底：到点自发重发一次（幂等无害）。
 *
 * <p><b>无租约</b>：等待者不登记持锁簿记、不启动看门狗——await 全程零
 * {@code LEASE_RENEW} 流量（服务端 awaiter 断连即摘除）。
 *
 * <p><b>countDown 至多一次</b>：请求超时/传输失败不自动重发（重复扣减
 * 风险大于收益），异常交由调用方裁决。
 */
final class RemoteCountDownLatch implements OCountDownLatch {

    /** 断线重连窗口的会话轮询间隔（毫秒）。 */
    private static final long SESSION_POLL_MS = 50;
    /** 应答读界在请求超时之外的余量（毫秒），与 RemoteLock 口径一致。 */
    private static final long SLACK_MS = 200;

    /** 所属客户端。 */
    private final OpenLatchClient client;
    /** 屏障键。 */
    private final String key;
    /** 定型断言（创建者句柄 > 0，纯加入句柄为 0）。 */
    private final long total;

    /**
     * 创建远程屏障句柄。仅由 {@link OpenLatchClient} 工厂调用。
     *
     * @param client 所属客户端
     * @param key    屏障键
     * @param total  定型断言（{@code >= 0}）
     */
    RemoteCountDownLatch(OpenLatchClient client, String key, long total) {
        this.client = client;
        this.key = key;
        this.total = total;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public void await() throws InterruptedException {
        if (!doAwait(client.config().defaultWaitTimeout().toMillis())) {
            throw new LockAcquisitionTimeoutException(
                    "await of latch '" + key + "' timed out");
        }
    }

    @Override
    public boolean await(long timeout, TimeUnit unit) throws InterruptedException {
        if (timeout < 0) {
            throw new IllegalArgumentException("timeout must be >= 0");
        }
        return doAwait(unit.toMillis(timeout));
    }

    @Override
    public long init() {
        return countDownOp(0);
    }

    @Override
    public long countDown() {
        return countDownOp(1);
    }

    @Override
    public long countDown(long n) {
        if (n < 1) {
            throw new IllegalArgumentException("count must be >= 1");
        }
        return countDownOp(n);
    }

    /**
     * 等待公共路径。
     *
     * @param budgetMs 等待总预算（毫秒）
     * @return 归零放行 {@code true}；预算耗尽 {@code false}
     * @throws InterruptedException 等待被中断
     * @throws OpenLatchException   服务端显式拒绝（定型不符 / 屏障不存在 / 队列满）
     */
    private boolean doAwait(long budgetMs) throws InterruptedException {
        long deadline = System.currentTimeMillis() + budgetMs;
        LatchNotifyRegistry registry = client.latchNotifies();
        OpenLatchClient.LatchRoute route = client.latchRoute();
        Envelope env = null;
        long envSession = -1;
        long rid = -1;
        try {
            while (true) {
                long remaining = deadline - System.currentTimeMillis();
                if (remaining <= 0) {
                    return false;
                }
                if (route == null) {
                    Thread.sleep(Math.min(remaining, SESSION_POLL_MS));
                    route = client.latchRoute();
                    continue;
                }
                if (env == null || envSession != route.session().sessionId()) {
                    envSession = route.session().sessionId();
                    rid = route.session().nextRequestId();
                    env = OpenLatchClient.latchAwaitEnvelope(rid, key, total);
                }
                CompletableFuture<Void> arrived = registry.register(envSession, rid);
                CompletableFuture<Envelope> response = route.mux().sendWithId(env,
                        Math.min(remaining, client.config().requestTimeout().toMillis()));
                StatusCode status;
                long waitMs;
                try {
                    waitMs = Math.min(remaining,
                            client.config().requestTimeout().toMillis() + SLACK_MS);
                    Envelope resp = response.get(waitMs, TimeUnit.MILLISECONDS);
                    status = resp.getLatchAwaitResponse().getStatus();
                } catch (ExecutionException e) {
                    Throwable cause = unwrap(e);
                    if (isTransient(cause)) {
                        // 传输失败/请求超时/会话已换：清注册，按新会话或同会话重发。
                        registry.remove(envSession, rid);
                        if (cause instanceof OpenLatchException ole
                                && ole.status() == StatusCode.SESSION_EXPIRED) {
                            env = null;
                        }
                        route = client.latchRoute();
                        continue;
                    }
                    if (cause instanceof RuntimeException re) {
                        throw re;
                    }
                    throw new OpenLatchException("await of latch '" + key + "' failed", cause);
                } catch (TimeoutException e) {
                    // 应答读界兜底（响应 future 自带请求超时，理论先到）。
                    registry.remove(envSession, rid);
                    continue;
                }
                registry.remove(envSession, rid);
                if (status == StatusCode.OK) {
                    return true;
                }
                if (status == StatusCode.QUEUED) {
                    long left = deadline - System.currentTimeMillis();
                    if (left <= 0) {
                        return false;
                    }
                    try {
                        arrived.get(Math.min(left,
                                client.config().requestTimeout().toMillis()), TimeUnit.MILLISECONDS);
                    } catch (TimeoutException pushLost) {
                        // 推送丢失兜底：自发幂等重发一次。
                    } catch (ExecutionException | InterruptedException e) {
                        if (e instanceof InterruptedException ie) {
                            throw ie;
                        }
                        // arrived 仅被 complete(null)，此分支不可达，防御清场。
                    }
                    continue;
                }
                throw new OpenLatchException(status, "await of latch '" + key + "' rejected: " + status);
            }
        } finally {
            if (rid >= 0) {
                registry.remove(envSession, rid);
            }
        }
    }

    /**
     * countDown/init 公共发送路径（至多一次，见类契约）。
     *
     * @param n 扣减量（0 为纯初始化）
     * @return 剩余计数
     */
    private long countDownOp(long n) {
        OpenLatchClient.LatchRoute route = client.latchRoute();
        if (route == null) {
            throw new ServerUnavailableException("connection is not active");
        }
        long rid = route.session().nextRequestId();
        Envelope env = OpenLatchClient.latchCountDownEnvelope(rid, key, n, total);
        try {
            Envelope resp = route.mux().sendWithId(env, client.config().requestTimeout().toMillis())
                    .get(client.config().requestTimeout().toMillis() + SLACK_MS,
                            TimeUnit.MILLISECONDS);
            StatusCode status = resp.getLatchCountDownResponse().getStatus();
            if (status != StatusCode.OK) {
                throw new OpenLatchException(status,
                        "countDown of latch '" + key + "' failed: " + status);
            }
            return resp.getLatchCountDownResponse().getRemaining();
        } catch (ExecutionException e) {
            Throwable cause = unwrap(e);
            if (cause instanceof OpenLatchException ole) {
                throw ole;
            }
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new OpenLatchException("countDown of latch '" + key + "' failed", cause);
        } catch (TimeoutException e) {
            throw new OpenLatchTimeoutException(
                    "countDown of latch '" + key + "' timed out (result unknown, not retried)");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new OpenLatchException("countDown of latch '" + key + "' interrupted", e);
        }
    }

    /**
     * 瞬态失败判定：等待循环可安全重试的失败（服务端裁决依赖幂等去重）。
     *
     * @param cause 解包后的失败原因
     * @return 瞬态返回 {@code true}
     */
    private static boolean isTransient(Throwable cause) {
        if (cause instanceof ServerUnavailableException || cause instanceof OpenLatchTimeoutException) {
            return true;
        }
        return cause instanceof OpenLatchException ole
                && ole.status() == StatusCode.SESSION_EXPIRED;
    }

    /**
     * 解包 {@link ExecutionException}/{@link CompletionException}。
     *
     * @param t 待解包异常
     * @return 真实原因
     */
    private static Throwable unwrap(Throwable t) {
        Throwable cause = t;
        while ((cause instanceof ExecutionException || cause instanceof CompletionException)
                && cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }
}
