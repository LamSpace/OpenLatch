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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 屏障等待的通知路由登记表（Phase 3 P3-06）：{@code (会话, 请求 id) →
 * 通知到达信号}。{@code AWAIT_NOTIFY} 推送不携带通道判别信息，按到达连接
 * 所属会话与 {@code request_id_ref} 命中登记项；未命中返回 {@code false}，
 * 由调用方回落锁等待队列路由（锁 tracker 对未命中本就静默）。
 *
 * <p><b>线程模型</b>：登记/摘除在等待者线程，{@link #signal} 在网络 EventLoop
 * 线程；{@link ConcurrentHashMap} 承载，信号 future 一次性消费（signal 即摘除）。
 */
public final class LatchNotifyRegistry {

    /** 构造空登记表。 */
    public LatchNotifyRegistry() {
        // 登记项随等待者生命周期惰性填充
    }

    /**
     * 登记键：会话与请求 id 的复合（不同车道会话的 id 空间相互独立）。
     *
     * @param sessionId 会话 id
     * @param requestId 请求 id
     */
    private record PendingKey(long sessionId, long requestId) {
        /**
         * 构造登记键。
         *
         * @param sessionId 会话 id
         * @param requestId 请求 id
         */
        PendingKey {
            // 复合键无附加校验：不可变性与等值语义由 record 承载。
        }
    }

    /** 登记表中在等的通知信号。 */
    private final ConcurrentMap<PendingKey, CompletableFuture<Void>> pending = new ConcurrentHashMap<>();

    /**
     * 登记等待：返回通知到达时完成的信号 future（同键重复登记替换旧项，
     * 旧项由调用方保证不再消费——等待者同一时刻至多一个活动请求）。
     *
     * @param sessionId 等待请求所属会话
     * @param requestId 等待请求 id
     * @return 通知到达信号
     */
    public CompletableFuture<Void> register(long sessionId, long requestId) {
        CompletableFuture<Void> f = new CompletableFuture<>();
        pending.put(new PendingKey(sessionId, requestId), f);
        return f;
    }

    /**
     * 通知到达路由：命中登记项则完成并摘除。
     *
     * @param sessionId 推送到达连接的会话 id
     * @param requestId 推送携带的 {@code request_id_ref}
     * @return 命中屏障等待返回 {@code true}；非本注册表所辖返回 {@code false}
     */
    public boolean signal(long sessionId, long requestId) {
        CompletableFuture<Void> f = pending.remove(new PendingKey(sessionId, requestId));
        if (f == null) {
            return false;
        }
        f.complete(null);
        return true;
    }

    /**
     * 摘除登记（等待结束时清理，不完成 future——future 属主已退出）。
     *
     * @param sessionId 会话 id
     * @param requestId 请求 id
     */
    public void remove(long sessionId, long requestId) {
        pending.remove(new PendingKey(sessionId, requestId));
    }
}
